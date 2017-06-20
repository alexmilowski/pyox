import requests

from requests.packages.urllib3.exceptions import InsecureRequestWarning

requests.packages.urllib3.disable_warnings(InsecureRequestWarning)

class Client:

   def __init__(self,service='',base=None,secure=False,host='localhost',port=50070,gateway=None,username=None,password=None):
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
      self.proxies = None
      self.verify = True

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
      return (self.username,self.password) if self.username is not None else None

   def post(self,url,data=None,headers=None):
      return requests.post(
         url,
         auth=self.auth(),
         data=data,
         headers=headers,
         proxies=self.proxies,
         verify=self.verify)

   def put(self,url,data=None,headers=None,allow_redirects=True):
      return requests.put(
         url,
         auth=self.auth(),
         data=data,
         headers=headers,
         allow_redirects=allow_redirects,
         proxies=self.proxies,
         verify=self.verify)

   def get(self,url,params={},allow_redirects=True):
      return requests.get(
         url,
         params=params,
         auth=self.auth(),
         allow_redirects=allow_redirects,
         proxies=self.proxies,
         verify=self.verify)

   def delete(self,url):
      return requests.delete(
         url,
         auth=self.auth(),
         proxies=self.proxies,
         verify=self.verify)

   def _exception(self,status,message):
      error = None;
      if status==401:
         error = PermissionError(message)
      else:
         error = IOError(message)
      error.status = status
      return error;
