from pyredis import Client
from pyhadoopapi import ClusterInformation
import argparse
import sys
import signal
import json
from time import sleep

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

parser = argparse.ArgumentParser(prog='queuelog',description="Hadoop Queue Log")

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
   '--parameter',
   dest='parameters',
   action='append',
   help="A client parameter")
parser.add_argument(
   '--client',
   default='default_client',
   help="The client module to import")
parser.add_argument(
     '--interval',
     type=int,
     default=60*5,
     nargs="?",
     help="The polling interval in seconds.")

args = parser.parse_args()

if args.proxies is not None:
   pdict = {}
   for pdef in args.proxies:
      pdict[pdef[0]] = pdef[1]
   args.proxies = pdict

args.user = parseAuth(args.auth)
args.hostinfo = parseHost(args.host)

if args.parameters is None:
   args.parameters = []

client = ClusterInformation(base=args.base,secure=args.secure,host=args.hostinfo[0],port=args.hostinfo[1],gateway=args.gateway,username=args.user[0],password=args.user[1])
client.proxies = args.proxies
client.verify = args.verify
if args.verbose:
   client.enable_verbose()

running = True

def default_client(parameters):
   def handler(info):
      sys.stdout.write('\x1e')
      sys.stdout.write(json.dumps(info))
      sys.stdout.flush()
   return handler

parts = args.client.rsplit('.', 1)
if len(parts)==1:
   handler = globals()[parts[0]](args.parameters)
else:
   handler = getattr(__import__(parts[0]),parts[1])(args.parameters)

def shutdown_handler(signal, frame):
   handler(None)
   sys.exit(0)

signal.signal(signal.SIGINT, shutdown_handler)
signal.signal(signal.SIGTERM, shutdown_handler)

while running:
   try:
      info = client.scheduler();
      handler(info)
   except ServiceError as err:
      sys.stderr.write(str(err))
   sleep(args.interval)
