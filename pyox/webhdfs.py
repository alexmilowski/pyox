
from pyhadoopapi.client import Client, ServiceError

def absolute_path(path):
   if len(path)>0 and path[0]!='/':
      path = '/'+path
   return path

class WebHDFS(Client):

   def __init__(self,**kwargs):
      super().__init__(**kwargs)
      self.service = 'webhdfs'
      self.read_chunk_size = 65536

   def list_directory(self,path):
      path = absolute_path(path)
      url = '{}{}'.format(self.service_url(),path)
      req = self.get(url,params={'op':'LISTSTATUS'},allow_redirects=False)
      #print(req.url)
      #req = requests.get(url,auth=None)
      if req.status_code==200:
         data = req.json()
         result = {}
         for entry in data['FileStatuses']['FileStatus']:
            result[entry['pathSuffix']] = entry
         return result
      else:
         raise ServiceError(req.status_code,'Cannot access path {}'.format(path),req)

   def open(self,path,offset=None,length=None,buffersize=None):
      path = absolute_path(path)
      url = '{}{}?op=OPEN'.format(self.service_url(),path)
      if offset is not None:
         url += '&offset={}'.format(offset)
      if length is not None:
         url += '&length={}'.format(length)
      if buffersize is not None:
         url += '&buffersize={}'.format(buffersize)
      #print(url)
      #open_req = self.get(url)
      #if open_req.status_code==200:
      #   return open_req.iter_content(chunk_size=self.read_chunk_size)
      open_req = self.get(url,allow_redirects=False)
      if open_req.status_code==307:
         location = open_req.headers['Location'];
         read_req = self.get(location,allow_redirects=False,stream=True)
         if read_req.status_code==200:
            return read_req.iter_content(chunk_size=self.read_chunk_size)
         else:
            raise ServiceError(read_req.status_code,'Cannot open datanode location {}'.format(location),read_req)
      else:
         raise ServiceError(open_req.status_code,'Cannot open path {}'.format(path),open_req)

   def make_directory(self,path,permission=None):
      path = absolute_path(path)
      url = '{}{}?op=MKDIRS'.format(self.service_url(),path)
      if permission is not None:
         url += '&permission={}'.format(permission)
      #print(url)
      req = self.put(url)
      if req.status_code!=200:
         raise ServiceError(req.status_code,'Cannot create path {}'.format(path),req)
      msg = req.json()
      return msg['boolean']

   def move(self,sourcepath,destpath):
      sourcepath = absolute_path(sourcepath)
      destpath = absolute_path(destpath)
      url = '{}{}?op=RENAME&destination={}'.format(self.service_url(),sourcepath,destpath)
      #print(url)
      req = self.put(url)
      if req.status_code!=200:
         raise ServiceError(req.status_code,'Cannot move path {} to {}'.format(sourcepath,destpath),req)
      msg = req.json()
      return msg['boolean']

   def remove(self,path,recursive=False):
      path = absolute_path(path)
      recursiveParam = 'true' if recursive else 'false'
      url = '{}{}?op=DELETE&recursive={}'.format(self.service_url(),path,recursiveParam)
      #print(url)
      req = self.delete(url)
      if req.status_code!=200:
         raise ServiceError(req.status_code,'Cannot delete path {}'.format(path),req)
      msg = req.json()
      return msg['boolean']

   def status(self,path):
      url = '{}{}?op=GETFILESTATUS'.format(self.service_url(),absolute_path(path))
      #print(url)
      req = self.get(url)
      if req.status_code!=200:
         raise ServiceError(req.status_code,'Cannot status path {}'.format(path),req)
      msg = req.json()
      return msg['FileStatus']

   def copy(self,data,path,size=-1,overwrite=False):
      path = absolute_path(path)
      overwriteParam = 'true' if overwrite else 'false'
      url = '{}{}?op=CREATE&overwrite={}'.format(self.service_url(),path,overwriteParam)
      #print(url)
      headers = {}
      headers['Content-Type'] = 'application/octet-stream'
      if size >= 0:
         headers['Content-Length'] = str(size)
      open_req = self.put(
         url,
         allow_redirects=False,
         headers={'Content-Length' : '0'})
      if open_req.status_code==307:
         location = open_req.headers['Location'];
         #print(location)
         req = self.put(
            location,
            data=data,
            headers=headers)
         if req.status_code!=201:
            raise ServiceError(req.status_code,'Cannot copy to path {}'.format(path),req)
      else:
         raise ServiceError(req.status_code,'Cannot open path {}'.format(path),open_req)
      return True

   def append(self,data,path,size=-1,buffersize=None):
      path = absolute_path(path)
      url = '{}{}?op=APPEND&overwrite={}'.format(self.service_url(),path)
      if buffersize is not None:
         url += '&buffersize={}'.format(buffersize)
      #print(url)
      open_req = self.post(
         url,
         allow_redirects=False,
         headers={'Content-Length' : '0'})
      if open_req.status_code==307:
         headers = {}
         headers['Content-Type'] = 'application/octet-stream'
         if size >= 0:
            headers['Content-Length'] = str(size)
         location = open_req.headers['Location'];
         #print(location)
         req = self.post(
            location,
            data=data,
            headers=headers)
         if req.status_code!=200:
            raise ServiceError(req.status_code,'Cannot append to path {}'.format(path),req)
      else:
         raise ServiceError(req.status_code,'Cannot append path {}'.format(path),open_req)
      return True
