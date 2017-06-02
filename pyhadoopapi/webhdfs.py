
from .client import Client

import requests

def absolute_path(path):
   if len(path)>0 and path[0]!='/':
      path = '/'+path
   return path

class WebHDFS(Client):

   def __init__(self,base=None,secure=False,host='localhost',port=50070,gateway=None,username=None,password=None):
      super().__init__(service='webhdfs',base=base,secure=secure,host=host,port=port,gateway=gateway,username=username,password=password)

   def list_directory(self,path):
      path = absolute_path(path)
      url = '{}{}?op=LISTSTATUS'.format(self.service_url(),path)
      #print(url)
      req = requests.get(url,auth=self.auth())
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
      open_req = requests.get(url,auth=self.auth())
      if open_req.status_code==200:
         return open_req.iter_content(chunk_size=16384)
      else:
         raise self._exception(open_req.status_code,'Cannot open path {}'.format(path))

   def make_directory(self,path):
      path = absolute_path(path)
      url = '{}{}?op=MKDIRS'.format(self.service_url(),path)
      #print(url)
      req = requests.put(url,auth=self.auth())
      if req.status_code!=200:
         raise self._exception(req.status_code,'Cannot create path {}'.format(path))
      msg = req.json()
      return msg['boolean']

   def move(self,sourcepath,destpath):
      sourcepath = absolute_path(sourcepath)
      destpath = absolute_path(destpath)
      url = '{}{}?op=RENAME&destination={}'.format(self.service_url(),sourcepath,destpath)
      #print(url)
      req = requests.put(url,auth=self.auth())
      if req.status_code!=200:
         raise self._exception(req.status_code,'Cannot move path {} to {}'.format(sourcepath,destpath))
      msg = req.json()
      return msg['boolean']

   def remove(self,path,recursive=False):
      path = absolute_path(path)
      recursiveParam = 'true' if recursive else 'false'
      url = '{}{}?op=DELETE&recursive={}'.format(self.service_url(),path,recursiveParam)
      #print(url)
      req = requests.delete(url,auth=self.auth())
      if req.status_code!=200:
         raise self._exception(req.status_code,'Cannot delete path {}'.format(path))
      msg = req.json()
      return msg['boolean']

   def copy(self,data,path,size=-1,overwrite=False):
      path = absolute_path(path)
      overwriteParam = 'true' if overwrite else 'false'
      url = '{}{}?op=CREATE&overwrite={}'.format(self.service_url(),path,overwriteParam)
      #print(url)
      headers = {}
      headers['Content-Type'] = 'application/octet-stream'
      if size >= 0:
         headers['Content-Length'] = str(size)
      open_req = requests.put(
         url,auth=self.auth(),
         allow_redirects=False,
         headers={'Content-Length' : '0'})
      if open_req.status_code==307:
         location = open_req.headers['Location'];
         #print(location)
         req = requests.put(
            location,auth=self.auth(),
            data=data,
            headers=headers)
         if req.status_code!=201:
            raise self._exception(req.status_code,'Cannot copy to path {}'.format(path))
      else:
         raise self._exception(req.status_code,'Cannot open path {}'.format(path))
      return True
