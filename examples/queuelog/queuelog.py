from pyhadoopapi import ClusterInformation, ServiceError
import argparse
import sys
import signal
import json
from time import sleep
from datetime import datetime
import logging
import logging.config

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
   help="A client parameter to be passed to the handler")
parser.add_argument(
   '--client',
   default='default_client',
   help="The client module to import")
parser.add_argument(
   '--log-config',
   dest='log_config',
   help="A logging configuration (json or ini file format)")
parser.add_argument(
   '--log-prefix',
   dest='log_prefix',
   default='queue',
   help="The prefix to use for the queue log files")
parser.add_argument(
   '--log-max',
   dest='log_max',
   type=int,
   default=100*1024*1024,
   help="The log file size max")
parser.add_argument(
   '--log-period-type',
   dest='log_period_type',
   help="The log by period type")
parser.add_argument(
   '--log-period-interval',
   type=int,
   dest='log_period_interval',
   default=1,
   help="The log by period iterval")
parser.add_argument(
   '-q','--quiet',
   dest='quiet',
   action='store_true',
   default=False,
   help="Suppress the console log")
parser.add_argument(
   '--no-log-file',
   dest='log_enabled',
   action='store_false',
   default=True,
   help="Suppress the log file")
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

logger = logging.getLogger(__name__)

def default_client(parameters):
   def handler(info):
      logger.info(json.dumps(info))
   return handler

parts = args.client.rsplit('.', 1)
if len(parts)==1:
   handler = globals()[parts[0]](args.parameters)
else:
   handler = getattr(__import__(parts[0]),parts[1])(args.parameters)

if args.log_config is not None:
   extpos = args.log_config.rfind('.')
   if extpos>0 and args.log_config[extpos:]=='.json':
      with open(args.log_config) as f:
         config = json.load(f)
      logging.config.dictConfig(config)
   else:
      logging.config.fileConfig(args.log_config)
else:
   config = {
      'version' : 1,
      'disable_existing_loggers' : False,
      'formatters' : {
         'console' : {
            'format' : '%(message)s'
         },
         'jsonseq' : {
            'format' : '\x1e%(message)s'
         }
      },
      'handlers' : {
         'stdout' : {
            'level' : 'INFO',
            'class': 'logging.StreamHandler',
            'formatter' : 'console',
            'stream' : 'ext://sys.stdout'

         },
         'file' : {
            'level' : 'INFO',
            'class' : 'logging.handlers.RotatingFileHandler',
            'formatter' : 'jsonseq',
            'filename' : args.log_prefix + '.log',
            'maxBytes' : args.log_max,
            'encoding' : 'utf-8'
         }
      },
      'loggers' : {
         '' : {
            'level' : 'INFO',
            'handlers' : ['stdout','file'],
            'propagate' : True
         }
      }
   }
   if args.quiet:
      config['loggers']['']['handlers'].remove('stdout')
   if not args.log_enabled:
      config['loggers']['']['handlers'].remove('file')
      config['handlers'].pop('file')
   if args.log_period_type is not None:
      config['handlers']['file'] = {
         'level' : 'INFO',
         'class' : 'logging.handlers.TimedRotatingFileHandler',
         'formatter' : 'jsonseq',
         'filename' : args.log_prefix + '.log',
         'when' : args.log_period_type,
         'interval' : args.log_period_interval,
         'encoding' : 'utf-8'
      }

   logging.config.dictConfig(config)

def shutdown_handler(signal, frame):
   handler(None)
   sys.exit(0)

signal.signal(signal.SIGINT, shutdown_handler)
signal.signal(signal.SIGTERM, shutdown_handler)

def leafQueue(info):
   queues = info.get('queues')
   if queues is not None:
      for child in queues['queue']:
         leafQueue(child)
   if info['type']!="capacitySchedulerLeafQueueInfo":
      return;
   wrapper = {
      'at' : datetime.now().isoformat(),
      'queue' : info
   }
   handler(wrapper)

while running:
   try:
      info = client.scheduler();
      leafQueue(info)
   except ServiceError as err:
      sys.stderr.write(str(err))
   sleep(args.interval)
