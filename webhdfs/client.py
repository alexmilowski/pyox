
import requests

def absolute_path(path):
   if len(path)>0 and path[0]!='/':
      path = '/'+path
   return path

class Client:

   def __init__(self,base=None,secure=False,host='localhost',port=50070,gateway=None,username=None,password=None):
      self.base = base
      self.secure = secure
      self.host = host
      self.port = port
      self.gateway = gateway
      self.username = username
      self.password = password

   def service_url(self):
      if self.base is not None:
         return self.base+'webhdfs/v1' if self.base[-1]=='/' else self.base+'/webhdfs/v1'
      protocol = 'https' if self.secure else 'http'
      if self.gateway is None:
         return '{}://{}:{}/webhdfs/v1'.format(protocol,self.host,self.port)
      else:
         return '{}://{}:{}/gateway/{}/webhdfs/v1'.format(protocol,self.host,self.port,self.gateway)

   def listDirectory(self,path):
      path = absolute_path(path)
      url = '{}{}?op=LISTSTATUS'.format(self.service_url(),path)
      #print(url)
      req = requests.get(url,auth=(self.username,self.password) if self.username is not None else None)
      #req = requests.get(url,auth=None)
      if req.status_code==200:
         data = req.json()
         result = {}
         for entry in data['FileStatuses']['FileStatus']:
            result[entry['pathSuffix']] = entry
         return result
      else:
         raise self._exception(req.status_code,'Cannot access path {}'.format(path))

   def open(self,path):
      path = absolute_path(path)
      url = '{}{}?op=OPEN'.format(self.service_url(),path)
      open_req = requests.get(url,auth=(self.username,self.password) if self.username is not None else None)
      if open_req.status_code==200:
         return open_req.iter_content(chunk_size=16384)
      else:
         raise self._exception(open_req.status_code,'Cannot open path {}'.format(path))

   def mkdir(self,path):
      path = absolute_path(path)
      url = '{}{}?op=MKDIRS'.format(self.service_url(),path)
      #print(url)
      req = requests.put(url,auth=(self.username,self.password) if self.username is not None else None)
      if req.status_code!=200:
         raise self._exception(req.status_code,'Cannot create path {}'.format(path))
      msg = req.json()
      return msg['boolean']

   def mv(self,sourcepath,destpath):
      sourcepath = absolute_path(sourcepath)
      destpath = absolute_path(destpath)
      url = '{}{}?op=RENAME&destination={}'.format(self.service_url(),sourcepath,destpath)
      #print(url)
      req = requests.put(url,auth=(self.username,self.password) if self.username is not None else None)
      if req.status_code!=200:
         raise self._exception(req.status_code,'Cannot move path {} to {}'.format(sourcepath,destpath))
      msg = req.json()
      return msg['boolean']

   def rm(self,path,recursive=False):
      path = absolute_path(path)
      recursiveParam = 'true' if recursive else 'false'
      url = '{}{}?op=DELETE&recursive={}'.format(self.service_url(),path,recursiveParam)
      #print(url)
      req = requests.delete(url,auth=(self.username,self.password) if self.username is not None else None)
      if req.status_code!=200:
         raise self._exception(req.status_code,'Cannot delete path {}'.format(path))
      msg = req.json()
      return msg['boolean']

   def cp(self,data,path,size=-1,overwrite=False):
      path = absolute_path(path)
      overwriteParam = 'true' if overwrite else 'false'
      url = '{}{}?op=CREATE&overwrite={}'.format(self.service_url(),path,overwrite)
      #print(url)
      headers = {}
      headers['Content-Type'] = 'application/octet-stream'
      if size >= 0:
         headers['Content-Length'] = str(size)
      req = requests.put(
         url,auth=(self.username,self.password) if self.username is not None else None,
         data=data,
         headers=headers)
      if req.status_code!=201:
         raise self._exception(req.status_code,'Cannot cp to path {}'.format(path))
      return True


   def _exception(self,status,message):
      error = None;
      if status==401:
         error = PermissionError(message)
      else:
         error = IOError(message)
      error.status = status
      return error;
