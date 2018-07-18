
# Deploying the Tracker Service for Oozie Jobs on Kubernetes

## 1. Setup Redis

### 1.1 Modify the configuration
If you do not have a redis server available inside your cluster, you'll need one
for the tracker service. Modify the `redis.conf` as needed but by default you
do not need to do anything.

### 1.2 Store the configuration as a configmap

Create the configmap for redis:

```
kubectl create configmap tracker-redis-config --from-file=redis.conf
kubectl get configmap tracker-redis-config -o yaml
```

The output should look something like this:
```
apiVersion: v1
data:
  redis.conf: ""
kind: ConfigMap
metadata:
  creationTimestamp: 2018-07-17T21:40:51Z
  name: tracker-redis-config
  namespace: ws-tracker
  resourceVersion: "48714741"
  selfLink: /api/v1/namespaces/ws-tracker/configmaps/tracker-redis-config
  uid: 134b5c8f-8a0a-11e8-a213-0cc47a089490
```

### 1.3 Deploy Redis

Create the master deployment:
```
kubectl create -f redis-deployment.yaml
```

Once the pod has deployed, you can verify that you can connect to the master via:
```
kubectl exec -it $(kubectl get po -o name | grep redis-master | cut -d / -f 2) redis-cli
```

Finally, create the service for redis:

```
kubectl create -f redis-service.yaml
```

## 2. Create the Configuration

The tracker service is configured via a JSON file (see `service.json`).  The
properties identify the base URI of the Knox service, gateway name, name node,
tracker node, and various HTTP parameters (proxies and certificate verification).

Modify the service.json file as necessary to reflect how you connect to Knox
and then create the configuration and check it via:

```
kubectl create configmap tracker-config --from-file=service=service.json
kubectl get configmap tracker-config -o yaml
```

The tracker service is a microservice deployed via gunicorn. To support
parallel requests and possible long timeouts, you should set the number of
workers and the timeout (in seconds).

The gunicorn configuration can be set and verified as follows:

```
kubectl create configmap tracker-gunicorn --from-literal=workers=8 --from-literal=timeout=180
kubectl get configmap tracker-gunicorn -o yaml
```

## 3. Deploy the Tracker Microservice

```
kubectl create -f tracker-deployment.yaml
```

Finally, create the service for the tracker microservices with an ingress:

```
kubectl create -f tracker-service.yaml
```
