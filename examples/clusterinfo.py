from pyox import ClusterInformation,make_client
import json

client,params = make_client(
                   ClusterInformation,
                   arguments=[ [ 'kind', {'choices' : [ 'info', 'metrics'] } ]])

if params.kind=='info':
   data = client.info()
else:
   data = client.metrics()

print(json.dumps(data,indent=True))
