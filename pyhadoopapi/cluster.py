from pyhadoopapi.client import Client, ServiceError, response_data
from io import StringIO
import sys

class ClusterInformation(Client):

   def __init__(self,base=None,secure=False,host='localhost',port=50070,gateway=None,username=None,password=None,namenode='sandbox',tracker=None):
      super().__init__(service='resourcemanager',base=base,secure=secure,host=host,port=port,gateway=gateway,username=username,password=password)

   def info(self):
      url = '{}/cluster/info'.format(self.service_url())
      req = self.get(url)
      #print(req.url)
      if req.status_code!=200:
         raise ServiceError(req.status_code,'Cannot get cluster information',request=req)
      return response_data(req)['clusterInfo']

   def metrics(self):
      url = '{}/cluster/metrics'.format(self.service_url())
      req = self.get(url)
      #print(req.url)
      if req.status_code!=200:
         raise ServiceError(req.status_code,'Cannot get cluster information',request=req)
      return response_data(req)['clusterMetrics']

   def scheduler(self):
      url = '{}/cluster/scheduler'.format(self.service_url())
      req = self.get(url)
      #print(req.url)
      if req.status_code!=200:
         raise ServiceError(req.status_code,'Cannot get cluster information',request=req)
      return response_data(req)['scheduler']['schedulerInfo']
