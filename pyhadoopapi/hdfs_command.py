from pyhadoopapi.webhdfs import WebHDFS
from pyhadoopapi.client import ServiceError
from datetime import datetime
import argparse
import sys
import os
from os.path import isfile
from glob import glob
from math import ceil

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
      help="Report sizes in bytes")
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
   catparser = argparse.ArgumentParser(prog='pyhadoopapi hdfs cat',description="cat")
   catparser.add_argument(
      '--offset',
      type=int,
      metavar=('int'),
      help="a byte offset for the file")
   catparser.add_argument(
      '--length',
      type=int,
      metavar=('int'),
      help="the byte length to retrieve")
   catparser.add_argument(
      'paths',
      nargs='*',
      help='a list of paths')
   args = catparser.parse_args(argv)
   for path in args.paths:
      input = client.open(path,offset=args.offset,length=args.length)
      for chunk in input:
         sys.stdout.buffer.write(chunk)

def hdfs_download_command(client,argv):
   dlparser = argparse.ArgumentParser(prog='pyhadoopapi hdfs download',description="download")
   dlparser.add_argument(
      '--chunk-size',
      dest='chunk_size',
      type=int,
      metavar=('int'),
      help="The chunk size for the download")
   dlparser.add_argument(
      '-o','--output',
      dest='output',
      metavar=('file'),
      help="the output file")
   dlparser.add_argument(
      '-v',
      action='store_true',
      dest='verbose',
      default=False,
      help="Verbose")
   dlparser.add_argument(
      'source',
      help='the remote source')
   args = dlparser.parse_args(argv)
   destination = args.output
   if destination is None:
      last = args.source.rfind('/')
      destination = args.source[last+1:] if last>=0 else args.source
   if args.chunk_size is not None:
      info = client.status(args.source)
      remaining = info['length']
      offset = 0
      chunk = 0
      chunks = ceil(remaining/args.chunk_size)
      if args.verbose:
         sys.stderr.write('File size: {}\n'.format(remaining))
      with open(destination,'wb') as output:
         while remaining>0:
            chunk += 1
            if args.verbose:
               sys.stderr.write('Downloading chunk {}/{} '.format(chunk,chunks))
               sys.stderr.flush()
            length = args.chunk_size if remaining>args.chunk_size else remaining
            input = client.open(args.source,offset=offset,length=length)
            for data in input:
               output.write(data)
               if args.verbose:
                  sys.stderr.write('.')
                  sys.stderr.flush()
            output.flush()

            if args.verbose:
               sys.stderr.write('\n')
               sys.stderr.flush()

            remaining -= length
            offset += length

   else:
      input = client.open(args.source)
      with open(destination,'wb') as output:
         for chunk in input:
            output.write(chunk)

def hdfs_mkdir_command(client,argv):
   for path in argv:
      if not client.make_directory(path):
         raise ServiceError(403,'mkdir failed: {}'.format(path))

def hdfs_mv_command(client,argv):
   if len(argv)!=3:
      sys.stderr.write('Invalid number of arguments: {}'.format(len(args.command)-1))
   if not client.mv(argv[0],argv[1]):
      raise ServiceError(403,'Move failed: {} → {}'.format(argv[0],argv[1]))

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
         raise ServiceError(403,'Cannot remove: {}'.format(path))

def copy_to_destination(client,source,destpath,verbose=False,force=False):
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
            raise ServiceError(403,'Cannot make target directory: {}'.format(dirpath))

   target = destpath + targetpath

   if verbose:
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
      if not client.copy(chunker() if size<0 else input,target,size=size,overwrite=force):
         raise ServiceError(403,'Move failed: {} → {}'.format(source,target))


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
         if isfile(pattern):
            copy_to_destination(client,pattern,destpath,verbose=cpargs.verbose,force=cpargs.force)
         else:
            files = glob(pattern,recursive=cpargs.recursive)
            if len(files)==0 and cpargs.verbose:
               sys.stderr.write('Nothing matched {}\n'.format(pattern))
            for source in files:
               copy_to_destination(client,source,destpath,verbose=cpargs.verbose,force=cpargs.force)

   elif len(cpargs.paths)==2:
      source = cpargs.paths[0]
      size = os.path.getsize(source) if cpargs.sendsize else -1
      with open(source,'rb') as input:
         if cpargs.verbose:
            sys.stderr.write(source+' → '+destpath+'\n')
         if not client.copy(input,destpath,size=size,overwrite=cpargs.force):
            raise ServiceError(403,'Move failed: {} → {}'.format(source,destpath))

   else:
      raise ServiceError(400,'Target is not a directory.')

hdfs_commands = {
   'ls' : hdfs_ls_command,
   'cat' : hdfs_cat_command,
   'download' : hdfs_download_command,
   'mkdir' : hdfs_mkdir_command,
   'mv' : hdfs_mv_command,
   'rm' : hdfs_rm_command,
   'upload' : hdfs_cp_command
}

def hdfs_command(args):

   client = WebHDFS(secure=args.secure,host=args.hostinfo[0],port=args.hostinfo[1],gateway=args.gateway,base=args.base,username=args.user[0],password=args.user[1])
   client.proxies = args.proxies
   client.verify = args.verify
   if args.verbose:
      client.enable_verbose()

   if len(args.command)==0:
      raise ValueError('One of the following comamnds must be specified: {}'.format(' '.join(hdfs_commands.keys())))

   func = hdfs_commands.get(args.command[0])
   if func is None:
      raise ValueError('Unrecognized command: {}'.format(args.command[0]))

   func(client,args.command[1:])
