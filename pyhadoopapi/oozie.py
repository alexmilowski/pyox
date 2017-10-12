from pyhadoopapi.client import Client, ServiceError, response_data
from pyhadoopapi.webhdfs import WebHDFS
from io import StringIO
from enum import auto,Enum
import sys
import types
import requests
import json

JOB_TRACKER = 'jobTracker'
NAMENODE = 'nameNode'
OOZIE_APP_PATH = 'oozie.wf.application.path'
_jsonType = 'application/json'

def write_property(xml,name,value):
   xml.write('<property>\n')
   xml.write('<name>')
   xml.write(name)
   xml.write('</name>\n')
   xml.write('<value>')
   if type(value)==bool:
      if value:
         xml.write('true')
      else:
         xml.write('false')
   else:
      xml.write(str(value))
   xml.write('</value>\n')
   xml.write('</property>\n')

class XMLWriter:
   def __init__(self,io):
      self.io = io
      self.open_elements = []

   def _push(self,name):
      self.open_elements.append((name,False,False))

   def _pop(self):
      return self.open_elements.pop(-1)

   def _markchild(self,text=None):
      if len(self.open_elements)==0:
         return
      current = self.open_elements[-1]
      if not current[1]:
         self.open_elements[-1] = (current[0],True,current[2] if text is None else text)
         self.io.write('>')

   def escape_attr(value):
      return str(value).replace('"','&quot;').replace('&','&amp;')

   def escape_text(value):
      return str(value).replace('&','&amp;').replace('<','&lt;')

   def empty(self,name,attrs={}):
      self.start(name,attrs)
      self.end()
      return self

   def start(self,name,attrs={}):
      self._markchild()
      for i in range(len(self.open_elements)-1):
         self.io.write('  ')
      self.io.write('<')
      self.io.write(name)
      for attr in attrs.items():
         self.io.write(' ')
         self.io.write(attr[0])
         self.io.write('="')
         self.io.write(XMLWriter.escape_attr(attr[1]))
         self.io.write('"')
      self._push(name)
      return self

   def end(self):
      current = self._pop()
      if current[1]:
         if not current[2]:
            for i in range(len(self.open_elements)-1):
               self.io.write('  ')
         self.io.write('</')
         self.io.write(current[0])
         self.io.write('>')
      else:
         self.io.write('/>')
      return self

   def text(self,value):
      self._markchild(text=True)
      self.io.write(XMLWriter.escape_text(value))
      return self

   def newline(self):
      self._markchild()
      self.io.write('\n')
      return self

   def named_child(self,name,value,all=True):
      if value is not None:
         if type(value)==list:
            if all:
               for item in value:
                  if hasattr(item,'to_xml'):
                     self.start(name)
                     item.to_xml(self)
                     self.end()
                  else:
                     self.start(name).text(str(item)).end()
                  self.newline()
            else:
               if hasattr(value[0],'to_xml'):
                  self.start(name)
                  value[0].to_xml(self)
                  self.end()
               else:
                  self.start(name).text(str(value[0])).end()
               self.newline()
         else:
            if hasattr(value,'to_xml'):
               self.start(name)
               value.to_xml(self)
               self.end()
            else:
               self.start(name).text(value).end()
            self.newline()

   def child(self,value,all=True):
      if value is not None:
         if type(value)==list:
            if all:
               for item in value:
                  if hasattr(item,'to_xml'):
                     item.to_xml(self)
                  else:
                     self.text(str(item))
                     self.newline()
            else:
               if hasattr(value[0],'to_xml'):
                  value[0].to_xml(self)
               else:
                  self.text(str(value[0]))
                  self.newline()
         else:
            if hasattr(value,'to_xml'):
               value.to_xml(self)
            else:
               self.text(str(value))
               self.newline()

   def finish(self):
      while len(self.open_elements)>0:
         self.end().newline()



class WorkflowItem:

   class Type(Enum):
      START = auto()
      END = auto()
      SWITCH = auto()
      FORK = auto()
      JOIN = auto()
      KILL = auto()
      ACTION = auto()

   def __init__(self,itemType,name,targets,**kwargs):
      self.itemType = itemType
      self.name = name
      self.targets = targets
      self.properties = kwargs

class InvalidWorkflow(ValueError):
   def __init__(self,message,errors):
      super().__init__(message)
      self.errors = errors


class XMLSerializable:
   def __str__(self):
      io = StringIO()
      self.to_xml(XMLWriter(io))
      return io.getvalue()

   def to_xml(self,xml):
      pass

class Workflow(XMLSerializable):

   def __init__(self,name,start):
      self.name = name
      self.start = start
      self.items = {}
      self.credentials = []
      self.end('end')
      self.last_action = None

   def start(name,start):
      w = Workflow(name,start)
      return w

   def end(self,name):
      self.end = name
      self.items[name] = WorkflowItem(WorkflowItem.Type.END,name,[])
      return self

   def action(self,name,action,credential=None,ok=None,error='error',retry=None):
      if self.start==None:
         self.start = name
      if name in self.items:
         raise ValueError('A workflow item named {} has already been defined.'.format(name))
      self.items[name] = WorkflowItem(WorkflowItem.Type.ACTION,name,[ok,error],action=action,credential=credential,retry=retry)
      if self.last_action is not None:
         self.last_action.targets[0] = name
      self.last_action = self.items[name]
      return self

   def switch(self,name,*cases):
      if self.start==None:
         self.start = name
      if name in self.items:
         raise ValueError('A workflow item named {} has already been defined.'.format(name))
      if len(cases)==0:
         raise ValueError('There must be at least one case.')
      defaultCount = 0
      targets = []
      for case in cases:
         if type(case)==str:
            defaultCount += 1
            targets.append(case)
         elif type(case)==tuple:
            if len(case)!=2:
               raise ValueError('Too many tuple values for case: {}'.format(str(case)))
            targets.append(case[0])
         else:
            raise ValueError('Incorrect case type: {}'.format(str(type(case))))
      if defaultCount>1:
         raise ValueError('More than one default provided on switch.')
      self.items[name] = WorkflowItem(WorkflowItem.Type.SWITCH,name,targets,cases=cases)
      return self

   def fork(self,name,*starts):
      if self.start==None:
         self.start = name
      if name in self.items:
         raise ValueError('A workflow item named {} has already been defined.'.format(name))
      if len(starts)<2:
         raise ValueError('A fork must have at least two pathes.')
      self.items[name] = WorkflowItem(WorkflowItem.Type.FORK,name,starts)
      return self


   def join(self,name,to):
      if self.start==None:
         self.start = name
      if name in self.items:
         raise ValueError('A workflow item named {} has already been defined.'.format(name))
      self.items[name] = WorkflowItem(WorkflowItem.Type.FORK,name,[to])
      return self

   def kill(self,name,message):
      if self.start==None:
         self.start = name
      if name in self.items:
         raise ValueError('A workflow item named {} has already been defined.'.format(name))
      self.items[name] = WorkflowItem(WorkflowItem.Type.KILL,name,[],message=message)
      return self

   def credential(self,cred_name,cred_type,*properties):
      action = XMLSerializable()
      action.cred_name = cred_name
      action.cred_type = cred_type
      action.properties = properties
      def to_xml(self,xml):
         xml.start('credential',{'name':self.cred_name,'type':self.cred_type}).newline()
         for item in self.properties:
            if type(item)==dict:
               for name in item:
                  value = item[name]
                  xml.start('property').newline()
                  xml.named_child('name',name)
                  xml.named_child('value',value)
                  xml.end().newline()
            else:
               xml.child(item)
         xml.end().newline()
      action.to_xml = types.MethodType(to_xml,action)
      self.credentials.append(action)
      return self

   def case(to,predicate):
      return (to,predicate)

   def default(to):
      return to

   def streaming(**kwargs):
      action = XMLSerializable()
      action.properties = kwargs
      def to_xml(self,xml):
         xml.start('streaming').newline()
         xml.named_child('mapper',self.properties.get('mapper'),all=False)
         xml.named_child('reducer',self.properties.get('reducer'),all=False)
         xml.named_child('record-reader',self.properties.get('record_reader'),all=False)
         xml.named_child('record-reader-mapping',self.properties.get('record_reader_mapping'))
         xml.named_child('env',self.properties.get('env'))
         xml.end().newline()
      action.to_xml = types.MethodType(to_xml,action)
      return action

   def pipes(**kwargs):
      action = XMLSerializable()
      action.properties = kwargs
      def to_xml(self,xml):
         xml.start('pipes').newline()
         xml.named_child('map',self.properties.get('map'),all=False)
         xml.named_child('reduce',self.properties.get('reduce'),all=False)
         xml.named_child('inputformat',self.properties.get('inputformat'),all=False)
         xml.named_child('partitioner',self.properties.get('partitioner'),all=False)
         xml.named_child('writer',self.properties.get('writer'),all=False)
         xml.named_child('program',self.properties.get('program'),all=False)
         xml.end().newline()
      action.to_xml = types.MethodType(to_xml,action)
      return action

   def delete(path):
      action = XMLSerializable()
      action.path = path
      def to_xml(self,xml):
         xml.empty('delete',{'path':self.path}).newline()
      action.to_xml = types.MethodType(to_xml,action)
      return action

   def mkdir(path):
      action = XMLSerializable()
      action.path = path
      def to_xml(self,xml):
         xml.empty('mkdir',{'path':self.path}).newline()
      action.to_xml = types.MethodType(to_xml,action)
      return action

   def prepare(*items):
      action = XMLSerializable()
      action.items = items
      def to_xml(self,xml):
         xml.start('prepare').newline()
         for item in self.items:
            item.to_xml(xml)
         xml.end().newline()
      action.to_xml = types.MethodType(to_xml,action)
      return action

   def property(name,value,description=None):
      action = XMLSerializable()
      action.name = name
      action.value = value
      if name is None:
         raise ValueError('The property name can not be missing.')
      if value is None:
         raise ValueError('The property value can not be missing.')
      action.description = description
      def to_xml(self,xml):
         xml.start('property').newline()
         xml.named_child('name',self.name)
         xml.named_child('value',self.value)
         xml.named_child('description',self.description)
         xml.end().newline()
      action.to_xml = types.MethodType(to_xml,action)
      return action

   def configuration(*items):
      action = XMLSerializable()
      action.items = items
      def to_xml(self,xml):
         xml.start('configuration').newline()
         for item in self.items:
            if type(item)==dict:
               for name in item:
                  value = item[name]
                  xml.start('property').newline()
                  xml.named_child('name',name)
                  xml.named_child('value',value)
                  xml.end().newline()
            else:
               item.to_xml(item)
         xml.end().newline()
      action.to_xml = types.MethodType(to_xml,action)
      return action

   def map_reduce(job_tracker,name_node,streaming_or_pipes,**kwargs):
      action = XMLSerializable()
      action.job_tracker = job_tracker
      action.name_node = name_node
      action.streaming_or_pipes = streaming_or_pipes
      action.properties = kwargs
      def to_xml(self,xml):
         xml.start('map-reduce').newline()
         xml.named_child('job-tracker',self.job_tracker)
         xml.named_child('name-node',self.name_node)
         xml.child(self.properties.get('prepare'))
         xml.child(self.streaming_or_pipes)
         xml.named_child('job-xml',self.properties.get('job_xml'))
         xml.child(self.properties.get('configuration'))
         xml.named_child('file',self.properties.get('file'))
         xml.named_child('archive',self.properties.get('archive'))
         xml.end().newline()
      action.to_xml = types.MethodType(to_xml,action)
      return action

   def spark(job_tracker,name_node,master,name,jar,**kwargs):
      action = XMLSerializable()
      action.job_tracker = job_tracker
      action.name_node = name_node
      action.master = master
      action.name = name
      action.jar = jar
      action.properties = kwargs
      def to_xml(self,xml):
         xml.start('spark',{'xmlns':'uri:oozie:spark-action:0.1'}).newline()
         xml.named_child('job-tracker',self.job_tracker)
         xml.named_child('name-node',self.name_node)
         xml.child(self.properties.get('prepare'))
         xml.named_child('job-xml',self.properties.get('job_xml'))
         xml.child(self.properties.get('configuration'))
         xml.named_child('master',self.master)
         xml.named_child('mode',self.properties.get('mode'))
         xml.named_child('name',self.name)
         xml.named_child('jar',self.jar)
         xml.named_child('spark-opts',self.properties.get('spark_opts'))
         xml.named_child('arg',self.properties.get('arg'))
         xml.end().newline()
      action.to_xml = types.MethodType(to_xml,action)
      return action

   def pig(job_tracker,name_node,script,**kwargs):
      action = XMLSerializable()
      action.job_tracker = job_tracker
      action.name_node = name_node
      action.script = script
      action.properties = kwargs
      def to_xml(self,xml):
         xml.start('pig').newline()
         xml.named_child('job-tracker',self.job_tracker)
         xml.named_child('name-node',self.name_node)
         xml.child(self.properties.get('prepare'))
         xml.named_child('job-xml',self.properties.get('job_xml'))
         xml.child(self.properties.get('configuration'))
         xml.named_child('script',self.script)
         xml.named_child('param',self.properties.get('param'))
         xml.named_child('argument',self.properties.get('argument'))
         xml.named_child('file',self.properties.get('file'))
         xml.named_child('archive',self.properties.get('archive'))
         xml.end().newline()
      action.to_xml = types.MethodType(to_xml,action)
      return action

   def hive(job_tracker,name_node,script,**kwargs):
      action = XMLSerializable()
      action.job_tracker = job_tracker
      action.name_node = name_node
      action.script = script
      action.properties = kwargs
      def to_xml(self,xml):
         xml.start('hive',{'xmlns':'uri:oozie:hive-action:0.3'}).newline()
         xml.named_child('job-tracker',self.job_tracker)
         xml.named_child('name-node',self.name_node)
         xml.child(self.properties.get('prepare'))
         xml.named_child('job-xml',self.properties.get('job_xml'))
         xml.child(self.properties.get('configuration'))
         xml.named_child('script',self.script)
         xml.named_child('param',self.properties.get('param'))
         xml.named_child('file',self.properties.get('file'))
         xml.named_child('archive',self.properties.get('archive'))
         xml.end().newline()
      action.to_xml = types.MethodType(to_xml,action)
      return action

   def hive2(job_tracker,name_node,jdbc_url,script,**kwargs):
      action = XMLSerializable()
      action.job_tracker = job_tracker
      action.name_node = name_node
      action.jdbc_url = jdbc_url
      action.script = script
      action.properties = kwargs
      def to_xml(self,xml):
         xml.start('hive2',{'xmlns':'uri:oozie:hive2-action:0.1'}).newline()
         xml.named_child('job-tracker',self.job_tracker)
         xml.named_child('name-node',self.name_node)
         xml.child(self.properties.get('prepare'))
         xml.named_child('job-xml',self.properties.get('job_xml'))
         xml.child(self.properties.get('configuration'))
         xml.named_child('jdbc-url',self.jdbc_url)
         xml.named_child('password',self.properties.get('password'))
         xml.named_child('script',self.script)
         xml.named_child('param',self.properties.get('param'))
         xml.named_child('argument',self.properties.get('argument'))
         xml.named_child('file',self.properties.get('file'))
         xml.named_child('archive',self.properties.get('archive'))
         xml.end().newline()
      action.to_xml = types.MethodType(to_xml,action)
      return action

   def ssh(host,command,*args,capture_output=False):
      action = XMLSerializable()
      action.host = host
      action.command = command
      action.args = args
      action.capture_output = capture_output
      def to_xml(self,xml):
         xml.start('ssh').newline()
         xml.named_child('host',self.host)
         xml.named_child('command',self.command)
         for arg in self.args:
            xml.named_child('args',arg)
         if self.capture_output:
            xml.start('capture-output').end().newline()
         xml.end().newline()
      action.to_xml = types.MethodType(to_xml,action)
      return action

   def shell(job_tracker,name_node,command,**kwargs):
      action = XMLSerializable()
      action.job_tracker = job_tracker
      action.name_node = name_node
      action.command = command
      action.properties = kwargs
      def to_xml(self,xml):
         xml.start('shell',{'xmlns':'uri:oozie:shell-action:0.1'}).newline()
         xml.named_child('job-tracker',self.job_tracker)
         xml.named_child('name-node',self.name_node)
         xml.child(self.properties.get('prepare'))
         xml.named_child('job-xml',self.properties.get('job_xml'))
         xml.child(self.properties.get('configuration'))
         xml.named_child('exec',self.command)
         xml.named_child('argument',self.properties.get('argument'))
         xml.named_child('file',self.properties.get('file'))
         xml.named_child('archive',self.properties.get('archive'))
         if self.properties.get('capture_output'):
            xml.start('capture-output').end().newline()
         xml.end().newline()
      action.to_xml = types.MethodType(to_xml,action)
      return action

   def sub_workflow(app_path,configuration=None,propagate_configuration=False):
      action = XMLSerializable()
      action.app_path
      action.configuration = configuration
      action.propagate_configuration = propagate_configuration
      def to_xml(self,xml):
         xml.start('sub-workflow').newline()
         xml.named_child('app-path',self.app_path)

         if self.propagate_configuration:
            xml.start('propagate-configuration').end().newline()
         xml.child(self.configuration)
         xml.end().newline()
      action.to_xml = types.MethodType(to_xml,action)
      return action

   def fs(*operations):
      action = XMLSerializable()
      action.operations = operations;
      def to_xml(self,xml):
         xml.start('fs').newline()
         xml.child(action.operations)
         xml.end()
      action.to_xml = types.MethodType(to_xml,action)
      return action

   def java(job_tracker,name_node,main_class,**kwargs):
      action = XMLSerializable()
      action.job_tracker = job_tracker
      action.name_node = name_node
      action.main_class = main_class
      action.properties = kwargs
      def to_xml(self,xml):
         xml.start('java').newline()
         xml.named_child('job-tracker',self.job_tracker)
         xml.named_child('name-node',self.name_node)
         xml.child(self.properties.get('prepare'))
         xml.named_child('job-xml',self.properties.get('job_xml'))
         xml.child(self.properties.get('configuration'))
         xml.named_child('main-class',self.main_class)
         xml.named_child('java-opts',self.properties.get('java_opts'))
         xml.named_child('arg',self.properties.get('arg'))
         xml.named_child('file',self.properties.get('file'))
         xml.named_child('archive',self.properties.get('archive'))
         if self.properties.get('capture_output'):
            xml.start('capture-output').end().newline()
         xml.end().newline()
      action.to_xml = types.MethodType(to_xml,action)
      return action

   def check(self):
      errors = []
      if self.start is None:
         errors.append('The start target has not been defined.')
      if self.start is not None and self.items.get(self.start) is None:
         errors.append('The start {} target has not been defined.'.format(str(self.start)))
      if self.end is None and self.items.get('end') is None:
         errors.append('The end target has not been defined.')
      for item in self.items.values():
         for name in item.targets:
            sname = str(name)
            if self.items.get(sname) is None:
               errors.append('Item {} referenced undefined target {}'.format(str(item.name),sname))
      if len(errors)>0:
         raise InvalidWorkflow('The workflow {} is invalid.'.format(self.name),errors)
      return self

   def to_xml(self,xml):
      xml.start('workflow-app',{'xmlns' : 'uri:oozie:workflow:0.5', 'name' : self.name}).newline()
      if len(self.credentials)>0:
         xml.start('credentials').newline()
         xml.child(self.credentials)
         xml.end().newline()
      if self.start is not None:
         xml.empty('start',{'to':self.start}).newline()
      for item in self.items.values():
         if item.itemType==WorkflowItem.Type.ACTION:
            attrs = {'name':item.name}
            credential = item.properties.get('credential')
            if credential is not None:
               attrs['cred'] = str(credential)
            retry = item.properties.get('retry')
            if retry is not None:
               attrs['retry-max'] = retry[0]
               attrs['retry-interval'] = retry[1]
            xml.start('action',attrs).newline()
            action = item.properties.get('action')
            if action is not None:
               action.to_xml(xml)
            ok = item.targets[0]
            if ok is None:
               ok = self.end
            xml.empty('ok',{'to':ok}).newline() \
               .empty('error',{'to':item.targets[1]}).newline() \
               .end().newline()
         elif item.itemType==WorkflowItem.Type.SWITCH:
            default = None
            xml.start('decision',{'name':item.name}).newline()
            xml.start('switch').newline()
            for case in item.properties['cases']:
               if type(case)==str:
                  default = case
               else:
                  xml.start('case',{'to':case[0]})
                  xml.text(case[1])
                  xml.end().newline()
            if default is not None:
               xml.empty('default',{'to':default}).newline()
            xml.end().newline()
            xml.end().newline()
         elif item.itemType==WorkflowItem.Type.FORK:
            xml.start('fork',{'name':item.name}).newline()
            for start in item.targets:
               xml.empty('path',{'start':start}).newline()
            xml.end().newline()
         elif item.itemType==WorkflowItem.Type.JOIN:
            xml.empty('join',{'name':item.name,'to':item.targets[0]}).newline()
         elif item.itemType==WorkflowItem.Type.KILL:
            xml.start('kill',{'name':item.name}).newline()
            xml.named_child('message',item.properties.get('message'))
            xml.end().newline()

      if self.end is not None:
         xml.empty('end',{'name':self.end}).newline()
      xml.finish()


class Job:
   def __init__(self,oozie,path,namenode='sandbox',verbose=False):
      self.oozie = oozie
      self.path = path
      if self.path[0]!='/':
         self.path = '/'+self.path
      if self.path[-1]=='/':
         self.path = self.path[0:-1]
      self.namenode = namenode
      self.hdfs = self.oozie.createHDFSClient()
      self.verbose = verbose
      self.progress = self.oozie.progress
      if self.verbose:
         sys.stderr.write('Creating workflow directory {} ...\n'.format(path))

      if not self.hdfs.make_directory(path):
         sys.stderr.write('Cannot create {}\n'.format(path))
         return

   def copy_resource(self,data,resource_path,overwrite=False):
      return self.hdfs.copy(data,self.path + '/' + resource_path,overwrite=overwrite)

   def define_workflow(self,data,overwrite=False):
      if type(data)==Workflow:
         data = StringIO(str(data))
      return self.copy_resource(data,'workflow.xml',overwrite=overwrite)


   def start(self,properties,verbose=False):
      xml = StringIO()
      xml.write('<?xml version="1.0" encoding="UTF-8"?>\n<configuration>\n')
      if properties is not None:
         for name in properties:
            value = properties[name]
            write_property(xml,name,value)
      for name in self.oozie.properties:
         if name not in properties:
            write_property(xml,name,value)
      if properties is not None and OOZIE_APP_PATH not in properties:
         write_property(xml,OOZIE_APP_PATH,'hdfs://{}{}/workflow.xml'.format(self.namenode,self.path))
      if properties is not None and NAMENODE not in properties:
         write_property(xml,NAMENODE,'hdfs://{}'.format(self.namenode))
      xml.write('</configuration>\n')

      if verbose or self.verbose:
         sys.stderr.write(xml.getvalue())
         sys.stderr.write('\n')
      if verbose or self.verbose or self.progress:
         sys.stderr.write('Requesting job start...\n')
      return self.oozie.start(xml.getvalue())

class Oozie(Client):

   def __init__(self,**kwargs):
      super().__init__(**kwargs)
      self.service = 'oozie'
      self.properties = {}
      self.defaultNamenode = kwargs.get('namenode')
      if self.defaultNamenode is None:
         self.defaultNamenode = 'sandbox'
      tracker = kwargs.get('tracker')
      if tracker is not None:
         self.properties[JOB_TRACKER] = tracker

   def createHDFSClient(self):
      webhdfs = WebHDFS(base=self.base,secure=self.secure,host=self.host,port=self.port,gateway=self.gateway,username=self.username,password=self.password,cookies=self.cookies)
      webhdfs.bearer_auth = self.bearer_auth
      webhdfs.proxies = self.proxies
      webhdfs.verify = self.verify
      if self.verbose:
         webhdfs.enable_verbose()
      return webhdfs


   def addProperty(self,name,value):
      self.properties[name] = value

   def removeProperty(self,name):
      return self.property.pop(name,None)

   def newJob(self,path,namenode=None,verbose=False):
      return Job(self,path,namenode=namenode if namenode is not None else self.defaultNamenode,verbose=verbose)

   def start(self,xml):
      headers = {'Content-Type' : 'application/xml; charset=UTF-8'}
      url = '{}/jobs'.format(self.service_url())
      req = self.post(url,params={'action':'start'},data=xml,headers=headers)
      #print(req.url)
      if req.status_code==201:
         msg = req.json()
         #print(msg)
         return msg['id']
      else:
         #print(req.text)
         raise ServiceError(req.status_code,'Cannot start job.',request=req)

   def status(self,jobid,show='info'):
      url = '{}/job/{}'.format(self.service_url(version='v2'),jobid)
      req = self.get(url,params={'show':show})
      #print(req.url)
      if req.status_code==200:
         return response_data(req)
      else:
         raise ServiceError(req.status_code if req.status_code!=400 else 404,'Cannot get job information for {}'.format(jobid),request=req)

   def list_jobs(self,status=None,offset=0,count=50):
      url = '{}/jobs'.format(self.service_url(version='v2'))
      params = {
         'offset' : str(offset),
         'len' : str(count)
      }
      if status is not None:
         params['filter'] = 'status='+str(status)
      req = self.get(url,params=params)
      #print(req.url)
      if req.status_code==200:
         return response_data(req)
      else:
         raise ServiceError(req.status_code,'Cannot list jobs',request=req)

   def submit(self,path,properties=None,workflow=None,copy=[],verbose=False,tracker=None):

      job = self.newJob(path,verbose=verbose)
      if workflow is not None:
         if verbose or self.progress:
            sys.stderr.write('Copying workflow.xml to {} ...\n'.format(path))
         job.define_workflow(workflow,overwrite=True)
      for info in copy:
         if type(info)==tuple:
            fpath = info[0]
            dest = info[1]
         else:
            fpath = info
            slash = fpath.rfind('/')
            dest = fpath[slash+1] if slash>=0 else fpath
         if type(fpath)==str:
            with open(fpath,'rb') as data:
               if verbose or self.progress:
                  if fpath==dest:
                     sys.stderr.write('→ {}\n'.format(dest))
                  else:
                     sys.stderr.write('{} → {}\n'.format(fpath,dest))
               job.copy_resource(data,dest,overwrite=True)
         else:
            if verbose or self.progress:
               sys.stderr.write('<data> → {}\n'.format(dest))
            job.copy_resource(fpath,dest,overwrite=True)
      jobid = job.start(properties,verbose=verbose)
      if self.progress:
         sys.stderr.write('{} job started.\n'.format(jobid))
      if tracker is not None:
         if verbose or self.progress:
            sys.stderr.write('Requesting tracking of {}\n'.format(jobid))
         if tracker[-1]=='/':
            tracker = tracker[0:-1]
         url = tracker + '/task/track/'
         track_req = requests.post(
            url,
            auth=self.auth(),
            data=json.dumps({'id' : jobid}),
            headers={'Content-Type' : 'application/json; charset=UTF-8'},
            verify=self.verify);
         if track_req.status_code!=200:
            raise ServiceError(track_req.status_code,'Cannot track job {} via {}'.format(jobid,url),request=track_req)
         if self.progress:
            sys.stderr.write('{} is being tracked.\n'.format(jobid))
      return jobid
