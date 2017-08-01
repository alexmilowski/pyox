import requests
import logging
from requests.auth import HTTPBasicAuth
import sys
import base64

try:
   from urllib3.exceptions import InsecureRequestWarning
   import urllib3
   urllib3.disable_warnings(InsecureRequestWarning)
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
