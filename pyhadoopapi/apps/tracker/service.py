from flask import Flask, current_app, request
from redis import Redis
from pyhadoopapi.apps.tracker.views import service_ui
from pyhadoopapi.apps.tracker.views import assets
from pyhadoopapi.apps.tracker.api import service_api, get_oozie_client, invoke_application_log_copy, application_ids, set_property, get_property, error_response

from pyhadoopapi.apps.tracker.tasks import task_list, task_get, task_lock, task_unlock, task_authenticate, task_authentication, task_delete, task_set_property, task_create
from pyhadoopapi.apps.monitor.api import cluster_api
from pyhadoopapi import ServiceError



import logging
import threading
import time
import base64
import json
import sys
import signal

from atexit import register

thread_counter = 0
def _task_thread_name():
   global thread_counter
   thread_counter += 1
   return __name__+'.tasks-'+str(thread_counter)

# TODO: this should be consolidated!
def _get_redis(app):
   port = 6379
   password = None
   host = app.config.get('REDIS_HOST')
   if host is None:
      host = 'localhost'
   else:
      parts = host.split(':')
      host = parts[0]
      if len(parts)>1:
         port = int(parts[1])
      if len(parts)>2:
         password = parts[2]
   return Redis(host=host,port=port,password=password,decode_responses=True)

def _track_job(app,task,verbose=False):
   logger = logging.getLogger(__name__)
   task_id = task.get('id')
   oozie = task.get('oozie')
   logger.info('Tracking job {} for {}'.format(task_id,oozie))

   redis = _get_redis(app)

   auth = task_authentication(redis,app.config.get('KEY'),task['access'])
   username = auth[0]
   password = auth[1]
   client = get_oozie_client(app,username=username,password=password)
   client.verbose = verbose
   try:
      info = client.status(oozie)
      status = info.get('status')
      logger.info('Job {} status {}'.format(oozie,status))
      set_property(redis,oozie,'status',status)
      app_ids = application_ids(info)
      logger.info('Job {}, applications: {}'.format(oozie,app_ids))
      set_property(redis,oozie,'application-ids',json.dumps(app_ids))
      if status=='SUCCEEDED':
         task_delete(redis,task_id)
      elif status in ['KILLED','FAILED']:
         copied = task.get('copied')
         if copied=='SUCCEEDED':
            task_delete(redis,task_id)
         elif copied=='RUNNING':
            done = True
            for job_id in json.loads(task['copy_jobs']):
               copy_job_info = client.status(job_id)
               current_done = copy_job_info.get('status') in ['SUCCEEDED','KILLED','FAILED']
               done = current_done and done
               logger.info('Copy job {} status {}'.format(job_id,copy_job_info.get('status')))
               if current_done:
                  cleanup_auth = task_authenticate(redis,app.config.get('KEY'),username,password)
                  cleanup_task_id = task_create(redis,access=cleanup_auth,type='job-cleanup',oozie=job_id)
            if done:
               logger.info('Deleting task {}'.format(task_id))
               task_delete(redis,task_id)
         else:
            job_ids = []
            errors = False
            if len(app_ids)==0:
               logger.info('No application logs to copy for {}'.format(oozie))
               task_delete(redis,task_id)
            else:
               for app_id in app_ids:
                  try:
                     job_id = invoke_application_log_copy(client,redis,oozie,app_id,username,verbose=verbose)
                     job_ids.append(job_id)
                     logger.info('Copy job {} started.'.format(job_id))
                  except:
                     errors = True
                     logger.error('Error invoking application log copy for {}/{}'.format(oozie,app_id), exc_info=True)
               if not errors:
                  task_set_property(redis,task_id,'copied','RUNNING')
                  task_set_property(redis,task_id,'copy_jobs',json.dumps(job_ids))
   except ServiceError as err:
      if err.status_code==404:
         logger.info('Job {} does not exist, deleting task {}'.format(oozie,task_id))
         task_delete(redis,task_id)
      else:
         logger.error('Cannot get information on job {}, status {} : {}'.format(oozie,err.status_code,err.message))

def _job_cleanup(app,task,verbose=False):
   logger = logging.getLogger(__name__)
   task_id = task.get('id')
   job_id = task.get('oozie')
   logger.info('Cleanup job {} for {}'.format(task_id,job_id))

   redis = _get_redis(app)

   auth = None
   try:
      auth = task_authentication(redis,app.config.get('KEY'),task['access'])
   except Exception:
      logger.exception('Cannot authenticate task, deleting {}'.format(task_id))
      task_delete(redis,task_id)
      return

   username = auth[0]
   password = auth[1]
   client = get_oozie_client(app,username=username,password=password)
   hdfs = client.createHDFSClient()

   path = get_property(redis,job_id,'path')
   cleanup = get_property(redis,job_id,'cleanup')
   if bool(cleanup):
      exists = True
      try:
         hdfs.status(path)
      except ServiceError as err:
         if err.status_code==404:
            exists = False
         else:
            logger.error('Cannot get information on path {}, status {} : {}'.format(path,err.status_code,err.message))
      logger.info('Cleanup path {}'.format(path))
      if not exists or hdfs.remove(path,recursive=True):
         logger.info('Cleanup completed for {}'.format(job_id))
         task_delete(redis,task_id)
         set_property(redis,job_id,'cleanup','SUCCEEDED')
      else:
         logger.error('Cannot delete path {}'.format(path))



operations = {
   'track' : _track_job,
   'job-cleanup' : _job_cleanup
}

running = True

def _update_tasks(app):
   logger = logging.getLogger(__name__)
   redis = _get_redis(app)
   try:
      logger.info('Checking tasks...')
      for id in task_list(redis):
         if not running:
            break
         logger.info('Task {}'.format(id))
         task = task_get(redis,id)

         if not task_lock(redis,id):
            logger.warn('Task {} is locked'.format(id))
            continue

         task_type = task.get('type')
         if task_type is None:
            logger.warn('Task {} has no type'.format(id))
            continue

         operation = operations.get(task_type)
         if operation is None:
            logger.warn('No operation for type {}'.format(task_type))
            continue

         try:
            operation(app,task,verbose=False)
         except:
            logger.error('Task {} operation {} failed'.format(id,task_type), exc_info=True)

         task_unlock(redis,id)
      logger.info('Done checking tasks.')
   except:
      logger.error('Exception during processing tasks', exc_info=True)

threads = []

def shutdown_event(signal, frame):
   global running
   running = False
   logger = logging.getLogger(__name__)
   logger.info("Shutdown event occurred...")
   for event,thread in threads:
      event.set()
      logger.info('Waiting for {} to join ...'.format(thread.name))
      thread.join()
   sys.exit(0)

signal.signal(signal.SIGINT, shutdown_event)
signal.signal(signal.SIGTERM, shutdown_event)
signal.signal(signal.SIGQUIT, shutdown_event)

def create_app(name):

   fapp = Flask(name)

   fapp.register_blueprint(service_ui,url_prefix='/')
   fapp.register_blueprint(service_api,url_prefix='/api')
   fapp.register_blueprint(assets)
   fapp.register_blueprint(cluster_api,url_prefix='/api/cluster')

   @fapp.before_request
   def check_auth():
      if request.authorization is None:
         return error_response(401,'Authorization required')

   return fapp

def start_task_queue(app,task_updater=_update_tasks):
   global running
   global threads
   logger = logging.getLogger(__name__)

   event = threading.Event()
   def run_check(app):
      while running:
         task_updater(app)
         event.wait(timeout=30)
   thread = threading.Thread(name=_task_thread_name(),target=run_check,args=[app])
   threads.append((event,thread))
   logger.info('Setup background thread {}'.format(thread.name))
   thread.start()
