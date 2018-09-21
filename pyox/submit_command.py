from pyhadoopapi.oozie import Oozie, Workflow
from pyhadoopapi.webhdfs import WebHDFS
from pyhadoopapi.client import ServiceError
import argparse
import json

def merge_properties(property_files,properties):
   target = {}
   if property_files is not None:
      for propfile in property_files:
         with open(propfile[0]) as propfilein:
            data = json.load(propfilein)
            for name in data:
               target[name] = data[name]
   if properties is not None:
      for prop in properties:
         target[prop[0]] = prop[1]
   return target if len(target)>0 else None


def submit_command(args):
   cmdparser = argparse.ArgumentParser(prog='pyhadoopapi submit',description='submit action')
   cmdparser.add_argument('path')
   cmdparser.add_argument('action',choices=['map-reduce','pig','fs','java','spark','workflow','hive','hive2'])
   cmdparser.add_argument(
      '--name',
      nargs='?',
      dest='name',
      default='workflow',
      help="the workflow name")
   cmdparser.add_argument(
      '--pipes',
      action='store_true',
      default=False,
      help="pipes map-reduce action (instead of streaming)")
   cmdparser.add_argument(
      '--rm','--prepare-delete',
      dest='deletes',
      action='append',
      nargs=1,
      help="delete a path")
   cmdparser.add_argument(
      '--mkdir','--prepare-mkdir',
      dest='mkdirs',
      action='append',
      nargs=1,
      help="create a path")
   cmdparser.add_argument(
      '--mv',
      dest='moves',
      action='append',
      metavar=('source','target'),
      nargs=2,
      help="move a path")
   cmdparser.add_argument(
      '--chmod',
      dest='chmods',
      action='append',
      metavar=('path','permissions'),
      nargs=2,
      help="change permissions on a path")
   cmdparser.add_argument(
      '--job-tracker',
      nargs='?',
      dest='job_tracker',
      default='${jobTracker}',
      help="the action job tracker")
   cmdparser.add_argument(
      '--name-node',
      nargs='?',
      dest='name_node',
      default='${nameNode}',
      help="the action name node")
   cmdparser.add_argument(
      '--jdbc-url',
      nargs='?',
      dest='jdbc_url',
      default='${jdbcURL}',
      help="the action JDBC connection")
   cmdparser.add_argument(
      '--job-xml',
      nargs='?',
      dest='job_xml',
      help="the action job-xml")
   cmdparser.add_argument(
      '--file',
      dest='files',
      action='append',
      nargs=1,
      help="a file for the action")
   cmdparser.add_argument(
      '--archive',
      dest='archives',
      action='append',
      nargs=1,
      help="an archive for the action")
   cmdparser.add_argument(
      '--config-property',
      dest='config_property',
      action='append',
      metavar=('name','value'),
      nargs=2,
      help="A configuration property name/value pair (e.g., name=value)")
   cmdparser.add_argument(
      '--config-properties',
      dest='config_properties',
      action='append',
      nargs=1,
      metavar=('file.json'),
      help="Configuration property name/value pairs in JSON format.")
   cmdparser.add_argument(
      '--mapper',
      nargs='?',
      dest='mapper',
      help="the streaming/pipe/etc. mapper")
   cmdparser.add_argument(
      '--reducer',
      nargs='?',
      dest='reducer',
      help="the streaming/pipe/etc. reducer")
   cmdparser.add_argument(
      '--script',
      nargs='?',
      dest='script',
      help="the command/program/script")
   cmdparser.add_argument(
      '--arg',
      dest='args',
      action='append',
      help="an argument for the action")
   cmdparser.add_argument(
      '--param',
      dest='params',
      action='append',
      help="a param for the action")
   cmdparser.add_argument(
      '--capture-output',
      action='store_true',
      default=False,
      help='capture the program output')
   cmdparser.add_argument(
      '-p','--property',
      dest='property',
      action='append',
      metavar=('name','value'),
      nargs=2,
      help="A job property name/value pair (e.g., name=value)")
   cmdparser.add_argument(
      '-P','--properties',
      dest='properties',
      action='append',
      nargs=1,
      metavar=('file.json'),
      help="Job property name/value pairs in JSON format.")
   cmdparser.add_argument(
      '-cp','--copy',
      dest='copy',
      action='append',
      metavar=('file or file=target'),
      nargs=1,
      help="A resource to copy (e.g., source or source=dest)")
   cmdparser.add_argument(
      '--credential',
      dest='credential',
      metavar=('name','type','file.json'),
      nargs=3,
      help="defines credential properties (by name)")
   cmdparser.add_argument(
      '-v','--verbose',
      action='store_true',
      dest='verbose',
      default=False,
      help="Verbose")

   if len(args.command)==0:
      cmdparser.print_help()
      return

   submit_args = cmdparser.parse_args(args.command)
   #print()
   #print(submit_args)

   client = Oozie(base=args.base,secure=args.secure,host=args.hostinfo[0],port=args.hostinfo[1],gateway=args.gateway,username=args.user[0],password=args.user[1])
   client.proxies = args.proxies
   client.verify = args.verify
   if args.verbose:
      client.enable_verbose()

   hdfs = client.createHDFSClient()

   properties = merge_properties(submit_args.properties,submit_args.property)

   config_properties = merge_properties(submit_args.config_properties,submit_args.config_property)

   files=[]
   if submit_args.copy is not None:
      for copy in submit_args.copy:
         fpath = copy[0]
         eq = fpath.find('=')
         if eq==0:
            sys.stderr.write('invalid copy argument: '+fpath)
            sys.exit(1)
         elif eq>0:
            dest = fpath[eq+1:]
            fpath = fpath[0:eq]
         else:
            slash = fpath.rfind('/')
            dest = fpath[slash+1] if slash>=0 else fpath
         files.append((fpath,dest))

   action_opts = {}
   if submit_args.mkdirs is not None or submit_args.deletes is not None:
      action_opts['prepare'] = Workflow.prepare(
         *(list(map(lambda x:Workflow.mkdir(x[0]),submit_args.mkdirs)) if submit_args.mkdirs is not None else []
           +
           list(map(lambda x:Workflow.delete(x[0]),submit_args.deletes)) if submit_args.deletes is not None else [])
      )
   if config_properties is not None:
      action_opts['configuration'] = Workflow.configuration(config_properties)
   if submit_args.action=='map-reduce':
      if submit_args.pipes:
         opts = {}
         if submit_args.mapper is not None:
            opts['map'] = submit_args.mapper
         if submit_args.reducer is not None:
            opts['reduce'] = submit_args.reducer
         if submit_args.script is not None:
            opts['program'] = submit_args.script
         streaming = Workflow.pipes(**opts)
      else:
         opts = {}
         if submit_args.mapper is not None:
            opts['mapper'] = submit_args.mapper
         if submit_args.reducer is not None:
            opts['reducer'] = submit_args.reducer
         streaming = Workflow.streaming(**opts)
      action = Workflow.map_reduce(submit_args.job_tracker,submit_args.name_node,streaming,**action_opts)
   elif submit_args.action=='spark':
      action = Workflow.spark(submit_args.job_tracker,submit_args.name_node,None,None,submit_args.script,**action_opts)
   elif submit_args.action=='hive':
      action = Workflow.hive(submit_args.job_tracker,submit_args.name_node,submit_args.script,**action_opts)
   elif submit_args.action=='hive2':
      if submit_args.params is not None:
         action_opts['param'] = submit_args.params
      if submit_args.args is not None:
         action_opts['argument'] = submit_args.args
      action = Workflow.hive2(submit_args.job_tracker,submit_args.name_node,submit_args.jdbc_url,submit_args.script,**action_opts)
   else:
      action = None

   if submit_args.credential is None:
      credential_name = None
   else:
      credential_name = submit_args.credential[0]

   workflow = \
      Workflow.start(submit_args.name,'action') \
              .action(
                 'action',
                 action,
                 credential=credential_name
              ) \
              .kill('error','Cannot run workflow {}'.format(submit_args.name))

   if credential_name is not None:
      with open(submit_args.credential[2]) as propfilein:
         credential_properties = json.load(propfilein)
         workflow.credential(credential_name,submit_args.credential[1],credential_properties)

   if submit_args.verbose:
      print()
      print(str(workflow))


   jobid = client.submit(submit_args.path,properties=properties,workflow=str(workflow),copy=files,verbose=submit_args.verbose)
   print(jobid)
