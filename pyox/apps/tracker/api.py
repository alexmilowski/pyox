from flask import Blueprint, g, current_app, request, Response, jsonify, copy_current_request_context
import json
import functools
import sys
import traceback
import logging
from redis import Redis
from time import sleep
from uuid import uuid4
from io import StringIO
from datetime import datetime

from pyox.apps.tracker.tasks import task_authenticate, task_create

from pyox import ServiceError, ClusterInformation, Oozie, Workflow

job_update_expiry = 60

# expire in 24 hours
REDIS_EXPIRES = 24*60*60

def get_redis():
   r = getattr(g, '_redis', None)
   if r is None:
      port = 6379
      password = None
      host = current_app.config.get('REDIS_HOST')
      if host is None:
         host = 'localhost'
      else:
         parts = host.split(':')
         host = parts[0]
         if len(parts)>1:
            port = int(parts[1])
         if len(parts)>2:
            password = parts[2]
      r = g._redis = Redis(host=host,port=port,password=password,decode_responses=True)
   return r

def json_seq(f):
   @functools.wraps(f)
   def wrapped(*args,**kwargs):
      data = f(*args,**kwargs)
      if type(data)=='function':
         def iter():
            for item in data():
               yield '\x1e'+json.dumps(item)
         return Response(status=200,response=iter,mimetype='application/json-seq; charset=utf-8')
      else:
         return Response(status=200,response=json.dumps(data),mimetype='application/json; charset=utf-8')

   return wrapped

def get_cluster_client():
   conf = current_app.config.get('KNOX')
   if conf is None:
      raise ValueError('Missing gateway configuration')
   client = ClusterInformation(
      base=conf.get('base'),
      secure=conf.get('secure',False),
      host=conf.get('host','localhost'),
      port=conf.get('port',50070),
      gateway=conf.get('gateway'),
      username=request.authorization.username if request.authorization is not None else None,
      password=request.authorization.password if request.authorization is not None else None)
   client.proxies = conf.get('proxies')
   client.verify = conf.get('verify',True)
   return client

def get_oozie_client(app,username=None,password=None,cookies=None,bearer_token=None,bearer_token_encode=True):
   conf = app.config.get('KNOX')
   if conf is None:
      raise ValueError('Missing gateway configuration')
   client = Oozie(
      base=conf.get('base'),
      secure=conf.get('secure',False),
      host=conf.get('host','localhost'),
      port=conf.get('port',50070),
      gateway=conf.get('gateway'),
      namenode=conf.get('namenode'),
      tracker=conf.get('tracker'),
      username=username,
      password=password,
      cookies=cookies,
      bearer_token=bearer_token,
      bearer_token_encode=bearer_token_encode)
   client.proxies = conf.get('proxies')
   client.verify = conf.get('verify',True)
   return client

def application_ids(info):
   actions = info.get('actions')
   if actions is not None:
      return list(map(lambda x : x[4:],filter(lambda x : x[0:4]=='job_' if x is not None else False,map(lambda action : action.get('externalId'),actions)))) + \
             list(map(lambda x : x[4:],filter(lambda x : x[0:4]=='job_' if x is not None else False,map(lambda action : action.get('externalChildIDs'),actions))))
   else:
      return []

def set_property(redis,objid,propname,value):
   redis.hset(objid,propname,value)
   redis.expire(objid,REDIS_EXPIRES)


def get_property(redis,objid,propname):
   return redis.hget(objid,propname)

def get_object(redis,objid):
   obj = {}
   for propname in redis.hkeys(objid):
      obj[propname] = redis.hget(objid,propname)
   return obj if len(obj.keys())>0 else None

def action_copy_job_id(app_id):
   return 'action-copy-job-'+app_id

def invoke_application_log_copy(oozie,redis,parent_id,action_id,username,verbose=False):

   logger = logging.getLogger(__name__)
   logger.info('Invoking copy from job {} for application {}'.format(parent_id,action_id))

   confid = str(uuid4())
   logdir = '/user/'+username+'/WORK/logs'
   path = logdir + '/' + confid
   workflow = Workflow.start(
      'shell-'+action_id,'shell',
      job_tracker='sandbox-RMS:8032',
      name_node='hdfs://sandbox'
   ).action(
      'shell',
      Workflow.shell(
         'copy.sh',
         configuration={
            'mapred.job.queue.name' : 'HQ_IST'
         },
         argument=[logdir,parent_id,'application_'+action_id],
         file=path+'/copy.sh'
      )
   ).kill('error','Cannot run copy workflow')
   if verbose:
      print(str(workflow))

   script = StringIO('''#!/bin/bash
hdfs dfs -mkdir -p $1/$2
hdfs dfs -rm $1/$2/$3.log
yarn logs -applicationId $3 | hdfs dfs -put - $1/$2/$3.log
''')
   jobid = oozie.submit(
      path,
      properties={
         'oozie.use.system.libpath' : True,
         'user.name' : username
      },
      workflow=workflow,
      copy=[(script,'copy.sh')],
      verbose=verbose
   )

   set_property(redis,parent_id,action_copy_job_id(action_id),jobid)
   set_property(redis,jobid,'status','RUNNING')
   set_property(redis,jobid,'path',path)
   set_property(redis,jobid,'cleanup',True)

   return jobid


service_api = Blueprint('service_api',__name__)

def nocache_headers():
   return {
      'Last-Modified' : datetime.now(),
      'Cache-Control' : 'no-store, no-cache, must-revalidate, post-check=0, pre-check=0, max-age=0',
      'Pragma' : 'no-cache',
      'Expires' : '-1'
   }

def error_response(status_code,message,**kwargs):
   obj = {'message':message,'status_code':status_code}
   for name in kwargs:
      obj[name] = kwargs[name]
   headers = nocache_headers()
   if status_code==401:
      headers['WWW-Authenticate'] = 'Basic realm="KNOX Credentials"'
   return Response(status=status_code,response=json.dumps(obj)+'\n',mimetype='application/json; charset=utf-8',headers=headers)

def api_response(status_code,obj,**kwargs):
   for name in kwargs:
      obj[name] = kwargs[name]
   headers = nocache_headers()
   return Response(status=status_code,response=json.dumps(obj)+'\n',mimetype='application/json; charset=utf-8',headers=headers)

def request_job_ids():
   content_type = request.headers['content-type']
   if content_type.startswith('text/plain'):
      ids = list(map(lambda x : x.strip(),request.data.decode('UTF-8').split('\n')))
   elif content_type.startswith('application/json'):
      data = json.loads(request.data)
      ids = data if type(data)==list else data.get('id')
      if type(ids)==str:
         ids = [ids]
   else:
      ids = None
   return ids

def get_job_summary(redis,job_id):

   logger = logging.getLogger(__name__)

   job_summary = get_object(redis,job_id)
   if job_summary is None:
      return None

   last_checked = job_summary.get('last-checked')
   if last_checked is None or (datetime.now()-datetime.strptime(last_checked,'%Y-%m-%dT%H:%M:%S.%f')).seconds>job_update_expiry:
      logger.info('{} is out of date, updating from {}'.format(job_id,last_checked))
      update_job_summary(redis,job_id)
   raw_app_ids = job_summary.get('application-ids')
   if raw_app_ids is not None:
      job_summary['application-ids'] = json.loads(raw_app_ids)
   removal = []
   for name in job_summary:
      if name[0:10]=='action-job':
         removal.append(name)
   for name in removal:
      job_summary.pop(name)
   return job_summary

TRACKING_KEY = 'dataplatform.service.tracking'

def tracking(redis,oozie_id):
   redis.hset(TRACKING_KEY,oozie_id,datetime.now().isoformat())
   redis.expire(TRACKING_KEY,REDIS_EXPIRES)

def stop_tracking(redis,oozie_id):
   redis.hdel(TRACKING_KEY,oozie_id)
   redis.expire(TRACKING_KEY,REDIS_EXPIRES)

def update_job_summary(redis,job_id):
   client = get_oozie_client(current_app,username=request.authorization.username if request.authorization is not None else None,password=request.authorization.password if request.authorization is not None else None)
   info = client.status(job_id)
   status = info.get('status')
   app_ids = application_ids(info)
   set_property(redis,job_id,'status',status)
   set_property(redis,job_id,'last-checked',datetime.now().isoformat())
   set_property(redis,job_id,'application-ids',json.dumps(app_ids))
   return {
      'id' : job_id,
      'status' : status,
      'applications-ids' : app_ids
   }

@service_api.route('/task/track/',methods=['POST'])
def service_track_job():
   redis = get_redis()
   if request.authorization is None:
      return error_response(401,'Authorization required')

   ids = request_job_ids()
   if ids is None:
      return error_response(400,'Unrecognized content type: '+request.headers['content-type'])

   logger = logging.getLogger(__name__)

   summaries = []
   auth = task_authenticate(redis,current_app.config.get('KEY'),request.authorization.username,request.authorization.password)
   for oozie_id in ids:
      task_id = task_create(redis,access=auth,type='track',oozie=oozie_id)
      tracking(redis,oozie_id)
      logger.info('Tracking {}, task {}'.format(oozie_id,task_id))
      try:
         summary = update_job_summary(redis,oozie_id)
         summaries.append(summary)
      except ServiceError as err:
         if err.status_code==404:
            logger.info('Job {job} does not exist, ignoring.'.format(job=oozie_id))
         else:
            raise err
   return api_response(200,summaries)

def create_log_dir(client):
   client.createHDFSClient().make_directory('/user/'+request.authorization.username+'/WORK/logs')

def log_file(client,username,oozie_id,app_id):
   path = '/user/{}/WORK/logs/{}/application_{}.log'.format(username,oozie_id,app_id)
   return client.open(path)

def copy_job_status(redis,client,oozie_id,app_ids,unknown=True):
   job_status = {}
   finished = True
   succeeded = True
   for app_id in app_ids:
      log_job_id = get_property(redis,oozie_id,action_copy_job_id(app_id))
      log_job_status = 'UNKNOWN'
      if log_job_id is not None:
         log_job_status = get_property(redis,log_job_id,'status')

         if log_job_status=='RUNNING' or log_job_status=='PROCESSING':
            log_job_info = client.status(log_job_id)
            log_job_status = log_job_info.get('status')
            if log_job_status is not None:
               set_property(redis,log_job_id,'status',log_job_status)

         if log_job_status is None:
            log_job_status = 'UNKNOWN'

      finished = finished and (log_job_status=='SUCCEEDED' or log_job_status=='KILLED')
      succeeded = succeeded and log_job_status=='SUCCEEDED'
      if finished and get_property(redis,log_job_id,'cleanup') not in ['RUNNING','SUCCEEDED']:
         auth = task_authenticate(redis,current_app.config.get('KEY'),request.authorization.username,request.authorization.password)
         task_id = task_create(redis,access=auth,type='job-cleanup',oozie=log_job_id)
         set_property(redis,log_job_id,'cleanup','RUNNING')

      if log_job_id is not None:
         job_status[app_id] = {'id':oozie_id,'application':app_id, 'job': log_job_id,'status' : log_job_status }
      elif unknown:
         job_status[app_id] = {'id':oozie_id,'application':app_id,'status':'UNKNOWN'}

   return (finished,succeeded,job_status)



@service_api.route('/task/copy-logs/',methods=['POST'])
def service_job_copy_logs():
   logger = logging.getLogger(__name__)
   ids = request_job_ids()
   if ids is None:
      return error_response(400,'Unrecognized content type: '+request.headers['content-type'])
   refresh = request.args.get('refresh')=='true'
   client = get_oozie_client(current_app,username=request.authorization.username if request.authorization is not None else None,password=request.authorization.password if request.authorization is not None else None)
   try:
      job_status = []
      for oozie_id in ids:
         logger.info('Copying logs for {} ...'.format(oozie_id))
         redis = get_redis()
         app_ids_json = get_property(redis,oozie_id,'application-ids') if not refresh else None
         status = get_property(redis,oozie_id,'status') if not refresh else None
         app_ids = json.loads(app_ids_json) if app_ids_json is not None else None

         #print(app_ids)

         finished = True
         succeeded = True
         if app_ids is None or refresh:
            tracking(redis,oozie_id)
            logger.info('Refreshing application information for {} ...'.format(oozie_id))
            info = client.status(oozie_id)
            app_ids = application_ids(info)
            status = info.get('status')

            set_property(redis,oozie_id,'application-ids',json.dumps(app_ids))
            set_property(redis,oozie_id,'status',status)

            create_log_dir(client)

            logger.info('Applications for {} are {}'.format(oozie_id,app_ids))

            job_ids = list(map(lambda app_id : invoke_application_log_copy(client,redis,oozie_id,app_id,request.authorization.username),app_ids))
            for i,log_job_id in enumerate(job_ids):
               app_id = app_ids[i]
               job_status.append({'id':oozie_id,'application':app_id,'job':log_job_id,'status':get_property(redis,log_job_id,'status')})
            finished = False
            succeeded = False
         else:
            tracking(redis,oozie_id)
            _finished,_succeeded,_job_status = copy_job_status(redis,client,oozie_id,app_ids)
            created_dir = False
            for app_id, log_job_info in _job_status.items():
               finished = finished and (log_job_info['status'] in ['SUCCEEDED','KILLED','FAILED'])
               succeeded = succeeded and log_job_info['status']=='SUCCEEDED'
               # Resubmit missing jobs
               if log_job_info['status'] in ['UNKNOWN','KILLED','FAILED']:
                  if not created_dir:
                     create_log_dir(client)
                     created_dir = True
                  logger.info('Copy needed for {} ...'.format(oozie_id))
                  log_job_id = invoke_application_log_copy(client,redis,oozie_id,log_job_info['application'],request.authorization.username)
                  log_job_info['job'] = log_job_id
                  log_job_info['status'] = get_property(redis,log_job_id,'status')
                  logger.info('Job {} invoked for {} ...'.format(log_job_id,oozie_id))
               job_status.append(log_job_info)

      return api_response(200,{'finished':finished,'succeeded':succeeded,'jobs':job_status})
   except ServiceError as err:
      return error_response(err.status_code,err.message)

@service_api.route('/job/<oozie_id>/logs/status')
def service_job_copy_logs_status(oozie_id):
   client = get_oozie_client(current_app,username=request.authorization.username if request.authorization is not None else None,password=request.authorization.password if request.authorization is not None else None)
   try:
      redis = get_redis()
      app_ids_json = get_property(redis,oozie_id,'application-ids')
      status = get_property(redis,oozie_id,'status')
      app_ids = json.loads(app_ids_json) if app_ids_json is not None else None

      if app_ids is None:
         return error_response(400,'There are no copy jobs.')

      finished,succeeded,job_status = copy_job_status(redis,client,oozie_id,app_ids)

      return api_response(200,{'finished':finished,'succeeded':succeeded,'jobs':job_status})
   except ServiceError as err:
      return error_response(err.status_code,err.message)

@service_api.route('/job/<job_id>')
def service_job_summary(job_id):
   refresh = request.args.get('refresh')=='true'
   client = get_oozie_client(current_app,username=request.authorization.username if request.authorization is not None else None,password=request.authorization.password if request.authorization is not None else None)
   try:
      redis = get_redis()
      job_summary = get_job_summary(redis,job_id)
      app_ids = job_summary.get('application-ids') if job_summary is not None else None
      status = job_summary.get('status') if job_summary is not None else None
      if status is None or status=='RUNNING' or refresh:
         info = client.status(job_id)
         status = info.get('status')
         app_ids = application_ids(info)
         set_property(redis,job_id,'status',status)
         set_property(redis,job_id,'application-ids',json.dumps(app_ids))
         job_summary = {
            'status' : status,
            'applications-ids' : app_ids
         }

      if app_ids is not None:
         copy_status = copy_job_status(redis,client,job_id,app_ids,unknown=False)
         job_summary['log-jobs'] = copy_status[2]

      job_summary['id'] = job_id
      return api_response(200,job_summary)
   except ServiceError as err:
      if err.status_code==404:
         return error_response(err.status_code,'Job {} was not found.'.format(job_id))
      else:
         return error_response(err.status_code,err.message)



def best_effort_logs(job_id,app_id=None):
   refresh = request.args.get('refresh')=='true'
   redis = get_redis()
   client = get_oozie_client(current_app,username=request.authorization.username if request.authorization is not None else None,password=request.authorization.password if request.authorization is not None else None)

   info = client.status(job_id)
   app_ids = application_ids(info) if app_id is None else [app_id]
   status = info.get('status')

   set_property(redis,job_id,'status',status)
   set_property(redis,job_id,'application-ids',json.dumps(application_ids(info)))

   if status!='RUNNING':
      username = request.authorization.username
      @copy_current_request_context
      def cat_logs():
         hdfs = client.createHDFSClient()
         for app_id in app_ids:
            try:
               log_data = log_file(hdfs,username,job_id,app_id)
               for chunk in log_data:
                  yield chunk
            except ServiceError as err:
               pass

      return Response(status=200,response=cat_logs(),mimetype='text/plain; charset=utf-8')
   else:
      return error_response(400,'No logs copied.')


@service_api.route('/job/<job_id>/logs')
def service_job_logs(job_id):
   client = get_oozie_client(current_app,username=request.authorization.username if request.authorization is not None else None,password=request.authorization.password if request.authorization is not None else None)
   id_list = request.args.getlist('id')
   try:
      redis = get_redis()
      job_summary = get_job_summary(redis,job_id)

      app_ids = job_summary.get('application-ids') if job_summary is not None else None

      if app_ids is None:
         return best_effort_logs(job_id)

      succeeded = []
      for app_id in app_ids:
         if len(id_list)>0 and app_id not in id_list:
            continue
         log_job_id = get_property(redis,job_id,action_copy_job_id(app_id))
         log_job_status = get_property(redis,log_job_id,'status')

         if log_job_status!='SUCCEEDED':
            log_job_info = client.status(log_job_id)
            log_job_status = log_job_info.get('status')
            if log_job_status is not None:
               set_property(redis,log_job_id,'status',log_job_status)
            if log_job_status!='SUCCEEDED':
               continue
         if log_job_status=='SUCCEEDED':
            succeeded.append(app_id)

      username = request.authorization.username
      @copy_current_request_context
      def cat_logs():
         hdfs = client.createHDFSClient()
         for app_id in succeeded:
            log_data = log_file(hdfs,username,job_id,app_id)
            for chunk in log_data:
               yield chunk


      return Response(status=200,response=cat_logs(),mimetype='text/plain; charset=utf-8')
   except ServiceError as err:
      return error_response(err.status_code,err.message)

@service_api.route('/job/<job_id>/logs/<app_id>')
def service_job_app_logs(job_id,app_id):
   try:
      return best_effort_logs(job_id,app_id=app_id)
   except ServiceError as err:
      return error_response(err.status_code,err.message)

@service_api.route('/job/<job_id>/status')
def service_job_status(job_id):
   refresh = request.args.get('refresh')=='true'
   client = get_oozie_client(current_app,username=request.authorization.username if request.authorization is not None else None,password=request.authorization.password if request.authorization is not None else None)
   try:
      redis = get_redis()
      status = get_property(redis,job_id,'status')
      app_ids_json = get_property(redis,job_id,'application-ids')
      app_ids = json.loads(app_ids_json) if app_ids_json is not None else None
      if status is None or status=='RUNNING' or refresh:
         info = client.status(id)
         status = info.get('status')
         app_ids = application_ids(info)
         set_property(redis,job_id,'status',status)
         set_property(redis,job_id,'application-ids',json.dumps(app_ids))
      return api_response(200,{'id':job_id,'status':status,'status_code':200})
   except ServiceError as err:
      if err.status_code==404:
         return error_response(err.status_code,'Job {} was not found.'.format(id))
      else:
         return error_response(err.status_code,err.message)

@service_api.route('/jobs/tracking')
def service_tracking_jobs():
   redis = get_redis()
   job_ids = redis.hkeys(TRACKING_KEY)
   job_list = []
   client = get_oozie_client(current_app,username=request.authorization.username if request.authorization is not None else None,password=request.authorization.password if request.authorization is not None else None)
   for job_id in job_ids:
      if len(job_id)==0:
         continue
      try:
         job_summary = get_job_summary(redis,job_id)
         if job_summary is None:
            stop_tracking(redis,job_id)
            continue
      except ServiceError as err:
         if err.status_code==404:
            logger.info('Job {job} no longer exists, removing from tracking.'.format(job=job_id))
            stop_tracking(redis,job_id)
            continue
         else:
            raise err
      job_summary['id'] = job_id
      app_ids = job_summary.get('application-ids') if job_summary is not None else None
      status = job_summary.get('status') if job_summary is not None else None
      if app_ids is not None:
         copy_status = copy_job_status(redis,client,job_id,app_ids,unknown=False)
         job_summary['applications'] = copy_status[2]
      job_list.append(job_summary)
   return api_response(200,job_list)


@service_api.route('/jobs')
def service_jobs():
   client = get_oozie_client(current_app,username=request.authorization.username if request.authorization is not None else None,password=request.authorization.password if request.authorization is not None else None)
   try:
      return api_response(200,client.list_jobs())
   except ServiceError as err:
      return error_response(err.status_code,err.message)
