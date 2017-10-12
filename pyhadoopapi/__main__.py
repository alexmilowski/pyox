from pyhadoopapi.oozie import Oozie
from pyhadoopapi.client import ServiceError
from datetime import datetime
import argparse
import sys
import os
import json
from glob import glob
import requests
import logging
from requests.exceptions import ProxyError

from .hdfs_command import hdfs_command
from .oozie_command import oozie_command
from .cluster_command import cluster_command
from pyhadoopapi.submit_command import submit_command

def handle_error(err,verbose=False):
   if err.status_code==401:
      sys.stderr.write('Unauthorized (401)\n')
   elif err.status_code==403:
      sys.stderr.write('Forbidden (403)\n')
   elif err.status_code==404:
      sys.stderr.write('Not found (404)\n')
   else:
      sys.stderr.write('Status ({})\n'.format(err.status_code))
   sys.stderr.write(err.message)
   sys.stderr.write('\n')
   if verbose and err.request is not None:
      for chunk in err.request.iter_content(chunk_size=1024*32):
         sys.stderr.buffer.write(chunk)

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

commands = {
   'cluster' : cluster_command,
   'hdfs' : hdfs_command,
   'oozie' : oozie_command,
   'submit' : submit_command
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

   if args.base is None:
      args.base = os.environ.get('HADOOP_BASE')
   if args.host is None:
      args.host = os.environ.get('HADOOP_HOST')
   if args.gateway is None:
      args.gateway = os.environ.get('HADOOP_GATEWAY')
   if args.auth is None:
      args.auth = os.environ.get('HADOOP_AUTH')
   if args.proxies is None:
      http_proxy = os.environ.get('HADOOP_PROXY_HTTP')
      https_proxy = os.environ.get('HADOOP_PROXY_HTTPS')
      if http_proxy is not None:
         args.proxies = [('http',http_proxy)]
      if https_proxy is not None:
         if args.proxies is None:
            args.proxies = [('https',https_proxy)]
         else:
            args.proxies.append(('https',https_proxy))
   try:
      sys.argv.index('--no-verify')
   except ValueError:
      value = os.environ.get('HADOOP_VERIFY')
      args.verify = value=='True' or value=='true'
   try:
      sys.argv.index('--secure')
   except ValueError:
      value = os.environ.get('HADOOP_SECURE')
      args.secure = value=='True' or value=='true'


   if args.proxies is not None:
      pdict = {}
      for pdef in args.proxies:
         pdict[pdef[0]] = pdef[1]
      args.proxies = pdict

   #print(args)

   if len(args.command)==0:
      parser.print_help()
      sys.exit(1)

   func = commands.get(args.command[0])
   if func is None:
      sys.stderr.write('Unrecognized command: {}\n'.format(args.command[0]))
      sys.exit(1)

   args.command = args.command[1:]
   args.user = parseAuth(args.auth)
   args.hostinfo = parseHost(args.host)

   try:
      func(args)
   except ValueError as err:
      sys.stderr.write(str(err))
      sys.stderr.write('\n')
      sys.exit(1)
   except ServiceError as err:
      handle_error(err,verbose=args.verbose)
      sys.exit(err.status_code)
   except ProxyError as err:
      sys.stderr.write(str(err))
      sys.stderr.write('\n')
      sys.exit(1)

if __name__ == '__main__':
   main()
