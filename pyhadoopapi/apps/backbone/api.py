from flask import Blueprint, g, current_app, request, Response
import json

from pyhadoopapi import ServiceError, ClusterInformation

def get_cluster_client():
   conf = current_app.config.get('KNOX')
   if conf is None:
      raise ValueError('Missing KNOX configuration')
   client = ClusterInformation(
      base=conf.get('base'),
      secure=conf.get('secure',False),
      host=conf.get('host','localhost'),
      port=conf.get('port',50070),
      gateway=conf.get('gateway'),
      username=request.authorization.username if request.authorization is not None else None,
      password=request.authorization.password if request.authorization is not None else None)
   client.proxies = conf.get('proxies')
   client.verify = conf.get('verify',True)
   return client

cluster_api = Blueprint('cluster_api',__name__)

@cluster_api.route('/')
def cluster_index():
   client = get_cluster_client()
   try:
      return Response(status=200,response=json.dumps(client.info()),mimetype="application/json; charset=utf-8")
   except ServiceError as err:
      return Response(status=err.status_code,response=err.message,mimetype="text/plain")

@cluster_api.route('/metrics')
def cluster_metrics():
   client = get_cluster_client()
   try:
      return Response(status=200,response=json.dumps(client.metrics()),mimetype="application/json; charset=utf-8")
   except ServiceError as err:
      return Response(status=err.status_code,response=err.message,mimetype="text/plain")

@cluster_api.route('/scheduler')
def cluster_scheduler():
   client = get_cluster_client()
   try:
      return Response(status=200,response=json.dumps(client.scheduler()),mimetype="application/json; charset=utf-8")
   except ServiceError as err:
      return Response(status=err.status_code,response=err.message,mimetype="text/plain")
