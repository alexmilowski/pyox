from .webhdfs import WebHDFS
from .oozie import Oozie
from datetime import datetime
import argparse
import sys
import os
import json
from glob import glob
import requests
import logging

def parseAuth(value):
   if value is None or len(value)==0:
      return (None,None)
   else:
      colon = value.find(':')
      if colon<0:
         return (value,None)
      else:
         return (value[0:colon],value[colon+1:])

def parseHost(value):
   if value is None or len(value)==0:
      return (None,None)
   else:
      colon = value.find(':')
      if colon<0:
         return (value,None)
      else:
         return (value[0:colon],int(value[colon+1:]))

def usage(defs):
   sys.stderr.write('One of the commands must be specified:')
   for command in sorted(defs):
      sys.stderr.write(' ')
      sys.stderr.write(command)
   sys.stderr.write('\n')

class tracker:

   def __init__(self):
      self.values = set()

   def add(self,value):
      self.values.add(value)

def hdfs_ls_command(client,argv):
   lsparser = argparse.ArgumentParser(prog='pyhadoopapi hdfs ls',description="ls")
   lsparser.add_argument(
      '-b',
      action='store_true',
      dest='reportbytes',
      default=False,
      help="Report sizes in binary")
   lsparser.add_argument(
      '-l',
      action='store_true',
      dest='detailed',
      default=False,
      help="List details")
   lsparser.add_argument(
      'paths',
      nargs='*',
      help='a list of paths')
   lsargs = lsparser.parse_args(argv)

   if len(lsargs.paths)==0:
      lsargs.paths = ['/']
   for path in lsargs.paths:
      listing = client.list_directory(path)
      max = 0;
      for name in sorted(listing):
         if len(name)>max:
            max = len(name)
      for name in sorted(listing):

         if not lsargs.detailed:
            print(name)
            continue

         info = listing[name]
         if name=='':
            name = path[path.rfind('/')+1:]
            max = len(name)
         ftype = info['type']
         size = int(info['length'])
         modtime = datetime.fromtimestamp(int(info['modificationTime'])/1e3)

         fspec = '{:'+str(max)+'}\t{}\t{}'
         fsize = '0'
         if ftype=='DIRECTORY':
            name = name + '/'
         else:
            if lsargs.reportbytes or size<1024:
               fsize = str(size)+'B'
            elif size<1048576:
               fsize = '{:0.1f}KB'.format(size/1024)
            elif size<1073741824:
               fsize = '{:0.1f}MB'.format(size/1024/1024)
            else:
               fsize = '{:0.1f}GB'.format(size/1024/1024/1024)
         print(fspec.format(name,fsize,modtime.isoformat()))

def hdfs_cat_command(client,argv):
   for path in argv:
      input = client.open(path)
      for chunk in input:
         sys.stdout.buffer.write(chunk)

def hdfs_mkdir_command(client,argv):
   for path in argv:
      if not client.make_directory(path):
         sys.stderr.write('mkdir failed: {}\n'.format(path))
         sys.exit(1)

def hdfs_mv_command(client,argv):
   if len(argv)!=3:
      sys.stderr.write('Invalid number of arguments: {}'.format(len(args.command)-1))
   if not client.mv(argv[0],argv[1]):
      sys.stderr.write('Move failed.\n')
      sys.exit(1)

def hdfs_rm_command(client,argv):
   rmparser = argparse.ArgumentParser(prog='pyhadoopapi hdfs rm',description="rm")
   rmparser.add_argument(
      '-r',
      action='store_true',
      dest='recursive',
      default=False,
      help="Recursively remove files/directories")
   rmparser.add_argument(
      'paths',
      nargs='*',
      help='a list of paths')
   rmargs = rmparser.parse_args(argv)
   for path in rmargs.paths:
      if not client.remove(path,recursive=rmargs.recursive):
         sys.stderr.write('Cannot remove: {}\n'.format(path))
         sys.exit(1)

def hdfs_cp_command(client,argv):
   cpparser = argparse.ArgumentParser(prog='pyhadoopapi hdfs cp',description="cp")
   cpparser.add_argument(
      '-f',
      action='store_true',
      dest='force',
      default=False,
      help="Force an overwrite")
   cpparser.add_argument(
      '-v',
      action='store_true',
      dest='verbose',
      default=False,
      help="Verbose")
   cpparser.add_argument(
      '-r',
      action='store_true',
      dest='recursive',
      default=False,
      help="Recursively apply wildcards")
   cpparser.add_argument(
      '-s',
      action='store_true',
      dest='sendsize',
      default=False,
      help="Send the file size")
   cpparser.add_argument(
      'paths',
      nargs='*',
      help='a list of paths')
   cpargs = cpparser.parse_args(argv)
   if len(cpargs.paths)<2:
      sys.stderr.write('At least two paths must be specified.\n')
      sys.exit(1)
   destpath = cpargs.paths[-1]
   if destpath[-1]=='/':
      # directory copy, glob files
      mkdirs = tracker()
      for pattern in cpargs.paths[:-1]:
         for source in glob(pattern,recursive=cpargs.recursive):
            size = os.path.getsize(source)
            targetpath = source
            slash = source.rfind('/')
            if source[0]=='/':
               targetpath = source[slash+1:]
            elif source[0:3]=='../':
               targetpath = source[slash+1:]
            elif slash > 0 :
               dirpath = source[0:slash]
               if dirpath not in mkdirs.values:
                  if cpargs.verbose:
                     sys.stderr.write(dirpath+'/\n')
                  if client.make_directory(destpath+dirpath):
                     mkdirs.add(dirpath)
                  else:
                     sys.stderr.write('Cannot make target directory: {}\n'.format(dirpath))
                     sys.exit(1)

            target = destpath + targetpath

            if cpargs.verbose:
               sys.stderr.write(source+' → '+target+'\n')
            with open(source,'rb') as input:
               def chunker():
                  sent =0
                  while True:
                     b = input.read(32768)
                     sent += len(b)
                     if not b:
                        if cpargs.verbose:
                           sys.stderr.write('Sent {} bytes\n'.format(sent))
                        break
                     yield b
               if not client.copy(chunker() if size<0 else input,target,size=size,overwrite=cpargs.force):
                  sys.stderr.write('Move failed.\n')
                  sys.exit(1)
   elif len(cpargs.paths)==2:
      source = cpargs.paths[0]
      size = os.path.getsize(source) if cpargs.sendsize else -1
      with open(source,'rb') as input:
         if cpargs.verbose:
            sys.stderr.write(source+'\n')
         if not client.copy(input,destpath,size=size,overwrite=cpargs.force):
            sys.stderr.write('Move failed.\n')
            sys.exit(1)
   else:
      sys.stderr.write('Target is not a directory.\n')
      sys.exit(1)


hdfs_commands = {
   'ls' : hdfs_ls_command,
   'cat' : hdfs_cat_command,
   'mkdir' : hdfs_mkdir_command,
   'mv' : hdfs_mv_command,
   'rm' : hdfs_rm_command,
   'cp' : hdfs_cp_command
}

def hdfs_command(args):

   user = parseAuth(args.auth)
   hostinfo = parseHost(args.host)
   client = WebHDFS(secure=args.secure,host=hostinfo[0],port=hostinfo[1],gateway=args.gateway,base=args.base,username=user[0],password=user[1])
   client.proxies = args.proxies
   client.verify = args.verify
   if args.verbose:
      client.enable_verbose()

   try:
      if len(args.command)==0:
         usage(hdfs_commands)
         sys.exit(1)
      func = hdfs_commands.get(args.command[0])
      if func is None:
         sys.stderr.write('Unrecognized command: {}\n'.format(args.command[0]))
         sys.exit(1)

      func(client,args.command[1:])
   except PermissionError as err:
      sys.stderr.write('Unauthorized\n');
      sys.exit(err.status)
   except IOError as err:
      sys.stderr.write(str(err)+'\n')
      if hasattr(err,'status'):
         if err.status==403:
            sys.stderr.write('Forbidden!\n')
         elif err.status==404:
            sys.stderr.write('Not found!\n')
         else:
            sys.stderr.write('status {}\n'.format(err.status))
         sys.exit(err.status)
      else:
         sys.exit(1)

   sys.exit(0)

def oozie_start_command(client,argv):
   cmdparser = argparse.ArgumentParser(prog='pyhadoopapi oozie start',description='start')
   cmdparser.add_argument(
      '-p','--property',
      dest='property',
      action='append',
      metavar=('name','value'),
      nargs=2,
      help="A property name/value pair (e.g., name=value)")
   cmdparser.add_argument(
      '-P','--properties',
      dest='properties',
      action='append',
      nargs=1,
      metavar=('file.json'),
      help="Property name/value pairs in JSON format.")
   cmdparser.add_argument(
      '-d','--definition',
      dest='definition',
      nargs=1,
      metavar=('file.xml'),
      help="The workflow definition to copy")
   cmdparser.add_argument(
      '-cp','--copy',
      dest='copy',
      action='append',
      metavar=('file or file=target'),
      nargs=1,
      help="A resource to copy (e.g., source or source=dest)")
   cmdparser.add_argument(
      '--namenode',
      nargs=1,
      metavar=('node[:port]'),
      help="The name node for jobs")
   cmdparser.add_argument(
      '--tracker',
      nargs=1,
      metavar=('node[:port]'),
      help="The job tracker for jobs")
   cmdparser.add_argument(
      '-v',
      action='store_true',
      dest='verbose',
      default=False,
      help="Verbose")
   cmdparser.add_argument(
      'path',
      help='The job path')
   args = cmdparser.parse_args(argv)
   if args.namenode is not None:
      client.namenode = args.namenode
   if args.tracker is not None:
      client.addProperty('jobTracker',args.tracker)

   job = client.newJob(args.path)
   if args.definition is not None:
      with open(args.definition,'rb') as data:
         if args.verbose:
            sys.stderr.write('{} → {}\n'.format(args.definition,'workflow.xml'))
         job.define_workflow(data,overwrite=True)
   properties = {}
   if args.properties is not None:
      for propfile in args.properties:
         with open(propfile[0]) as propfilein:
            data = json.load(propfilein)
            for name in data:
               properties[name] = data[name]
   if args.property is not None:
      for prop in args.property:
         properties[prop[0]] = prop[1]
   if args.copy is not None:
      for copy in args.copy:
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
         with open(fpath,'rb') as data:
            if args.verbose:
               sys.stderr.write('{} → {}\n'.format(fpath,dest))
            job.copy_resource(data,dest,overwrite=True)
   jobid = job.start(properties,verbose=args.verbose)
   print(jobid)

def oozie_status_command(client,argv):
   cmdparser = argparse.ArgumentParser(prog='pyhadoopapi oozie status',description='job status')
   cmdparser.add_argument(
      '-r','--raw',
      action='store_true',
      dest='raw',
      default=False,
      help="return raw JSON")
   cmdparser.add_argument(
      '-a',
      action='store_true',
      dest='actions',
      default=False,
      help="show error messages")
   cmdparser.add_argument(
      '-s','--show',
      dest='show',
      nargs='?',
      default='info',
      help="The information about the job to retrieve.")
   cmdparser.add_argument(
      '-l',
      action='store_true',
      dest='detailed',
      default=False,
      help="list details")
   cmdparser.add_argument(
      'jobids',
      nargs='*',
      help='a list job ids')
   args = cmdparser.parse_args(argv)
   if args.detailed:
      print('\t'.join(['JOB','STATUS','USER','PATH','START','END','CODE','MESSAGE']))

   for jobid in args.jobids:
      response = client.status(jobid,show=args.show)
      #print(response)
      if response[0]==404:
         if args.raw:
            sys.stdout.write('\n')
            sys.stdout.write('{{"id":"{}","status":{}}}'.format(jobid,response[0]))
            sys.stdout.write('\x1e')
         else:
            print('{}\tNOT FOUND'.format(jobid))
         print('{}}\tNOT FOUND'.format(jobid))
      elif response[0]!=200:
         if args.raw:
            sys.stdout.write('\n')
            sys.stdout.write('{{"id":"{}","status":{}}}'.format(jobid,response[0]))
            sys.stdout.write('\x1e')
         else:
            print('{}\tERROR {}'.format(jobid,response[0]))
      else:
         if args.raw or type(response[1])==str:
            if type(response[1])==str:
               sys.stdout.write(response[1])
            elif type(response[1])==bytes:
               sys.stdout.buffer.write(response[1])
            else:
               sys.stdout.write('\n')
               sys.stdout.write(json.dumps(response[1]))
               sys.stdout.write('\x1e')
         elif args.detailed:
            startTime = response[1].get('startTime')
            endTime = response[1].get('endTime')
            lastModTime = response[1].get('lastModTime')
            createdTime = response[1].get('createdTime')
            appPath = response[1].get('appPath')
            status = response[1].get('status')
            actions = response[1].get('actions')
            user = response[1].get('user')
            print('\t'.join(map(lambda x:str(x),[jobid,status,user,appPath,startTime,endTime])))
            if args.actions and actions is not None:
               for action in actions:
                  print('\t'.join(map(lambda x:str(x) if x is not None else '',[action.get('id'),action.get('status'),user,action.get('name'),action.get('startTime'),action.get('endTime'),action.get('errorCode'),action.get('errorMessage')])))
         else:
            status = response[1].get('status')
            print('{}\t{}'.format(jobid,status))
            actions = response[1].get('actions')
            if args.actions and actions is not None:
               for action in actions:
                  print('{}\t{}\t{}\t{}'.format(action.get('id'),action.get('status'),action.get('errorCode'),action.get('errorMessage')))

def oozie_ls_command(client,argv):
   cmdparser = argparse.ArgumentParser(prog='pyhadoopapi oozie status',description='job status')
   cmdparser.add_argument(
      '-a',
      action='store_true',
      dest='all',
      default=False,
      help="show all")
   cmdparser.add_argument(
      '-l',
      action='store_true',
      dest='detailed',
      default=False,
      help="list details")
   cmdparser.add_argument(
      '-s','--status',
      dest='status',
      nargs='?',
      default='RUNNING',
      help="filter by status")
   cmdparser.add_argument(
      '-o','--offset',
      nargs='?',
      dest='offset',
      type=int,
      default=0,
      metavar=('offset'),
      help="The offset at which to start")
   cmdparser.add_argument(
      '-n','--count',
      nargs='?',
      type=int,
      default=50,
      dest='count',
      metavar=('count'),
      help="The number of items to return")
   args = cmdparser.parse_args(argv)

   if args.all:
      msg = client.list_jobs(offset=args.offset,count=args.count)
   else:
      msg = client.list_jobs(offset=args.offset,count=args.count,status=args.status)
   if msg[0]==200:
      if args.detailed:
         print('\t'.join(['ID','STATUS','NAME','USER','START','END']))
         for job in msg[1]['workflows']:
            id = job['id']
            user = job['user']
            status = job['status']
            appName = job['appName']
            startTime = job['startTime']
            endTime = job['endTime']
            print('\t'.join(map(lambda x:str(x) if x is not None else '',[id,status,appName,user,startTime,endTime])))
      else:
         for job in msg[1]['workflows']:
            id = job['id']
            print(id)
   else:
      sys.stderr.write('Failed to get status, {}\n'.format(msg[0]))


oozie_commands = {
   'start' : oozie_start_command,
   'status' : oozie_status_command,
   'ls' : oozie_ls_command
}

def oozie_command(args):

   user = parseAuth(args.auth)
   hostinfo = parseHost(args.host)
   client = Oozie(base=args.base,secure=args.secure,host=hostinfo[0],port=hostinfo[1],gateway=args.gateway,username=user[0],password=user[1])
   client.proxies = args.proxies
   client.verify = args.verify
   if args.verbose:
      client.enable_verbose()

   try:
      if len(args.command)==0:
         usage(oozie_commands)
         sys.exit(1)
      func = oozie_commands.get(args.command[0])
      if func is None:
         sys.stderr.write('Unrecognized command: {}\n'.format(args.command[0]))
         sys.exit(1)

      func(client,args.command[1:])
   except PermissionError as err:
      sys.stderr.write('Unauthorized\n');
      sys.exit(err.status)
   except IOError as err:
      sys.stderr.write(str(err)+'\n')
      if hasattr(err,'status'):
         if err.status==403:
            sys.stderr.write('Forbidden!\n')
         elif err.status==404:
            sys.stderr.write('Not found!\n')
         else:
            sys.stderr.write('status {}\n'.format(err.status))
         sys.exit(err.status)
      else:
         sys.exit(1)

   sys.exit(0)


commands = {
   'hdfs' : hdfs_command,
   'oozie' : oozie_command
}

def main():
   parser = argparse.ArgumentParser(prog='pyhadoopapi',description="KNOX Client")

   parser.add_argument(
        '--base',
        nargs="?",
        help="The base URI of the service")
   parser.add_argument(
        '--host',
        nargs="?",
        default='localhost',
        help="The host of the service (may include port)")
   parser.add_argument(
        '--secure',
        action='store_true',
        default=False,
        help="Use TLS transport (https)")
   parser.add_argument(
        '--gateway',
        nargs="?",
        help="The KNOX gateway name")
   parser.add_argument(
      '--auth',
       help="The authentication for the request (colon separated username/password)")
   parser.add_argument(
      '-p','--proxy',
      dest='proxies',
      action='append',
      metavar=('protocol','url'),
      nargs=2,
      help="A protocol proxy")
   parser.add_argument(
      '--no-verify',
      dest='verify',
      action='store_false',
      default=True,
      help="Do not verify SSL certificates")
   parser.add_argument(
      '-v','--verbose',
      dest='verbose',
      action='store_true',
      default=False,
      help="Output detailed information about the request and response")

   parser.add_argument(
      'command',
      nargs=argparse.REMAINDER,
      help='The command')

   args = parser.parse_args()

   if args.proxies is not None:
      pdict = {}
      for pdef in args.proxies:
         pdict[pdef[0]] = pdef[1]
      args.proxies = pdict

   #print(args)

   if len(args.command)==0:
      usage(commands)
      parser.print_help()
      sys.exit(1)

   func = commands.get(args.command[0])
   if func is None:
      sys.stderr.write('Unrecognized command: {}\n'.format(args.command[0]))
      sys.exit(1)

   args.command = args.command[1:]

   func(args)

if __name__ == '__main__':
   main()
