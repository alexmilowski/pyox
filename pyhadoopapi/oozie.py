from .client import Client
from .webhdfs import WebHDFS
import requests
from io import StringIO

JOB_TRACKER = 'jobTracker'
NAMENODE = 'nameNode'
OOZIE_APP_PATH = 'oozie.wf.application.path'

def write_property(xml,name,value):
   xml.write('<property>\n')
   xml.write('<name>')
   xml.write(name)
   xml.write('</name>\n')
   xml.write('<value>')
   xml.write(value)
   xml.write('</value>\n')
   xml.write('</property>\n')

class Job:
   def __init__(self,oozie,path,name,namenode='sandbox'):
      self.oozie = oozie
      self.path = path
      self.name = name
      self.namenode = namenode
      self.port = port

   def copy_resource(data,resource_path):
      return self.oozie.webhdfs.copy(data,self.path + '/' + resource_path)

   def define_workflow(self,data):
      return self.copy_resource(data,'workflow.xml')


   def start(self,properties):
      xml = StringIO()
      xml.write('<?xml version="1.0" encoding="UTF-8"?>\n<configuration>\n')
      for name in properties:
         value = properties[name]
         write_property(name,value)
      for name in self.oozie.properties:
         if name not in properties:
            write_property(name,value)
      if OOZIE_APP_PATH not in properties:
         write_property(OOZIE_APP_PATH,'hdfs://{}/{}'.format(self.namenode,self.path))
      if NAMENODE not in properties:
         write_property(NAMENODE,'hdfs://{}'.format(self.namenode))
      xml.write('</configuration>\n')

      return self.oozie.start(xml.getvalue())

class Oozie(Client):

   def __init__(self,base=None,secure=False,host='localhost',port=50070,gateway=None,username=None,password=None,path=None,namenode='sandbox',tracker=None):
      super().__init__(service='oozie/v1',base=base,secure=secure,host=host,port=port,gateway=gateway,username=username,password=password)
      self.webhdfs = WebHDFS(base=base,secure=secure,host=host,port=port,gateway=gateway,username=username,password=password)
      self.default_path = path
      self.properties = {}
      self.defaultNamenode = namenode
      if tracker is not None:
         self.properties[JOB_TRACKER] = tracker

   def addProperty(self,name,value):
      self.properties[name] = value

   def removeProperty(self,name):
      return self.property.pop(name,None)

   def newJob(name,path=None,namenode=None):
      jobPath = path + '/' + name if path is not None else self.default_path + '/' + name
      return Job(self,jobPath,name,namenode=namenode if namenode is not None else self.defaultNamenode)

   def start(self,xml):
      headers = {'Content-Type' : 'application/xml; charset=UTF-8'}
      url = '{}/jobs?action=start'.format(self.service_url())
      req = requests.post(
         url,
         auth=auth(),
         data=xml,
         headers=headers)
      if req.status_code==200:
         msg = req.json()
         return msg
      else:
         self._exception(req.status_code,'Cannot start job.')
