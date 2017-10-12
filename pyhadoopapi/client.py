import requests
import logging
from requests.auth import HTTPBasicAuth
import sys
import os
import base64
import argparse
from types import FunctionType

try:
   from urllib3.exceptions import InsecureRequestWarning
   import urllib3
   urllib3.disable_warnings(InsecureRequestWarning)
except ImportError:
   pass
try:
   from requests.packages.urllib3.exceptions import InsecureRequestWarning
   requests.packages.urllib3.disable_warnings(InsecureRequestWarning)
except ImportError:
   pass

def verbose_log(function):
   def wrapper(self,*args,**kwargs):
      r = function(self,*args,**kwargs)
      if self.verbose:
         logger = logging.getLogger(__name__)
         for key in r.request.headers:
            value = r.request.headers[key]
            logger.debug('{}: {}'.format(key,value))

      return r
   return wrapper

_jsonType = 'application/json'

def response_data(req):
   contentType = req.headers.get('Content-Type')
   majorType = contentType[0:contentType.find('/')] if contentType is not None else 'application'
   if contentType is None:
      data = None
   elif contentType[0:len(_jsonType)]==_jsonType:
      data = req.json()
   elif majorType=='image' or majorType=='application':
      data = req.content
   else:
      data = req.text
   return data


class ServiceError(Exception):
   """Raised when a service interaction does not return a successful error code"""
   def __init__(self,status_code,message,request=None):
      self.status_code = status_code
      self.message = message
      self.request = request
      if request is not None:
         self.data = response_data(request)


class Client:

   def __init__(self,service='',base=None,secure=False,host='localhost',port=50070,gateway=None,username=None,password=None,cookies=None,bearer_token=None,bearer_token_encode=True,**extrakeywords):
      self.service = service
      self.base = base
      if self.base is not None and self.base[-1]!='/':
         self.base = self.base + '/'
      self.secure = secure
      self.host = host if host is not None else 'localhost'
      self.port = port if port is not None else 50070
      self.gateway = gateway
      self.username = username
      self.password = password
      self.cookies = cookies
      if bearer_token is not None:
         if bearer_token_encode:
            barray = self.bearer_token.encode('utf-8') if type(self.bearer_token)==str else self.bearer_token
            bearer_token = base64.b64encode(barray.encode('utf-8')).decode('utf-8')
         self.bearer_auth = 'bearer '+bearer_token
      else:
         self.bearer_auth = None
      self.proxies = None
      self.verify = True
      self.verbose = False
      self.progress = False

   def enable_verbose(self):
      self.verbose = True;
      # These two lines enable debugging at httplib level (requests->urllib3->http.client)
      # You will see the REQUEST, including HEADERS and DATA, and RESPONSE with HEADERS but without DATA.
      # The only thing missing will be the response.body which is not logged.
      try:
         import http.client as http_client
      except ImportError:
         # Python 2
         import httplib as http_client
      #http_client.HTTPConnection.debuglevel = 1

      # You must initialize logging, otherwise you'll not see debug output.
      logging.basicConfig()
      logging.getLogger().setLevel(logging.DEBUG)
      requests_log = logging.getLogger("requests.packages.urllib3")
      requests_log.setLevel(logging.DEBUG)
      requests_log.propagate = True


   def service_url(self,version='v1'):
      if self.base is not None:
         if self.gateway is None:
            return '{}{}/{}'.format(self.base,self.service,version)
         else:
            return '{}gateway/{}/{}/{}'.format(self.base,self.gateway,self.service,version)
      protocol = 'https' if self.secure else 'http'
      if self.gateway is None:
         return '{}://{}:{}/{}/{}'.format(protocol,self.host,self.port,self.service,version)
      else:
         return '{}://{}:{}/gateway/{}/{}/{}'.format(protocol,self.host,self.port,self.gateway,self.service,version)

   def auth(self):
      if self.bearer_auth is None:
         return HTTPBasicAuth(self.username,self.password) if self.username is not None else None
      else:
         return None

   def request_headers(self,headers):
      if self.bearer_auth is not None:
         if headers is None:
            headers = {'Authorization':self.bearer_auth}
         else:
            headers = headers.copy()
            headers['Authorization'] = self.bearer_auth
      return headers

   @verbose_log
   def post(self,url,params={},data=None,headers=None,allow_redirects=True):
      return requests.post(
         url,
         params=params,
         auth=self.auth(),
         cookies=self.cookies,
         data=data,
         headers=self.request_headers(headers),
         allow_redirects=allow_redirects,
         proxies=self.proxies,
         verify=self.verify)

   @verbose_log
   def put(self,url,params={},data=None,headers=None,allow_redirects=True):
      return requests.put(
         url,
         params=params,
         auth=self.auth(),
         cookies=self.cookies,
         data=data,
         headers=self.request_headers(headers),
         allow_redirects=allow_redirects,
         proxies=self.proxies,
         verify=self.verify)

   @verbose_log
   def get(self,url,params={},allow_redirects=True,stream=False):
      return requests.get(
         url,
         params=params,
         auth=self.auth(),
         cookies=self.cookies,
         headers=self.request_headers(None),
         allow_redirects=allow_redirects,
         stream=stream,
         proxies=self.proxies,
         verify=self.verify)

   @verbose_log
   def delete(self,url,params={},allow_redirects=True):
      return requests.delete(
         url,
         params=params,
         auth=self.auth(),
         cookies=self.cookies,
         headers=self.request_headers(None),
         allow_redirects=allow_redirects,
         proxies=self.proxies,
         verify=self.verify)

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

def parse_args(*params,**kwargs):
   if params is None or len(params)==0:
      params = sys.argv[1:]
   elif len(params)==1 and type(params[0])==list:
      params = params[0]

   parser = argparse.ArgumentParser(prog=kwargs.get('prog'),description=kwargs.get('description'))

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
      '-i','--progress-information',
      dest='progress',
      action='store_true',
      default=False,
      help="Output progress information about the operations")

   argument_specs = kwargs.get('arguments')
   if argument_specs is not None:
      for spec in argument_specs:
         if type(spec)==str:
            parser.add_argument(spec)
         else:
            a = []
            for n in spec:
               if type(n)==str:
                  a.append(n)
            parser.add_argument(*a,**spec[-1])

   customizer = kwargs.get('customizer')
   if customizer is not None:
      if type(customizer)!=FunctionType:
         raise ValueError('customizer is not a function: {}'.format(type(customizer)))
      customizer(parser)

   args = parser.parse_args(params)

   check_env = kwargs.get('disable_environ')
   if check_env is None or check_env:
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

   args.user = parseAuth(args.auth)
   args.hostinfo = parseHost(args.host)

   return args

class custom_params():
   def __str__(self):
      s='{'
      first = True
      for attr in dir(self):
         if not first:
            s += ', '
         if attr[0:2]!='__':
            value = getattr(self,attr)
            if type(value)==str:
               value = '\'' + value.replace('\'','\\\'')+ '\''
            else:
               value = str(value)
            s += "'{}'".format(attr)+':'+value
            first = False
      s += '}'
      return s


def make_client(kclass,*params,**kwargs):
   args = parse_args(*params,**kwargs)
   client = kclass(base=args.base,secure=args.secure,host=args.hostinfo[0],port=args.hostinfo[1],gateway=args.gateway,username=args.user[0],password=args.user[1])
   client.proxies = args.proxies
   client.verify = args.verify
   client.progress = args.progress
   if args.verbose:
      client.enable_verbose()
   customizer = kwargs.get('customizer')
   if customizer is not None:
      if type(customizer)!=FunctionType:
         raise ValueError('customizer is not a function: {}'.format(type(customizer)))
      customizer(client,args)
   argument_specs = kwargs.get('arguments')
   arguments = None
   if argument_specs is not None:
      arguments = custom_params()
      for spec in argument_specs:
         if type(spec)==str:
            setattr(arguments,spec,getattr(args,spec))
         else:
            name = spec[-1].get('dest')
            if name is not None:
               setattr(arguments,name,getattr(args,name))

   return client if arguments is None else (client,arguments)
