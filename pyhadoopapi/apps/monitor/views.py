from flask import Blueprint, render_template, Response, current_app, send_from_directory

from pyhadoopapi import ServiceError
from pyhadoopapi.apps.monitor.api import get_cluster_client
from datetime import datetime

cluster_ui = Blueprint('cluster_ui',__name__,template_folder='templates')

@cluster_ui.route('/')
def index():
   client = get_cluster_client()
   try:
      info = client.info();
      scheduler = client.scheduler();
      metrics = client.metrics();
      info['startedOn'] = datetime.fromtimestamp(info['startedOn'] / 1e3).isoformat()
      return render_template('cluster.html',info=info,scheduler=scheduler,metrics=metrics)
   except ServiceError as err:
      return Response(status=err.status_code,response=err.message if err.status_code!=401 else 'Authentication Required',mimetype="text/plain",headers={'WWW-Authenticate': 'Basic realm="Login Required"'})


assets = Blueprint('assets_ui',__name__)
@assets.route('/assets/<path:path>')
def send_asset(path):
   dir = current_app.config.get('ASSETS')
   if dir is None:
      dir = __file__[:__file__.rfind('/')] + '/assets/'
   return send_from_directory(dir, path)
