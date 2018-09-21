from flask import Blueprint, render_template, Response, current_app, send_from_directory

from pyox import ServiceError
from pyox.apps.tracker.api import get_cluster_client, error_response
from datetime import datetime

service_ui = Blueprint('service_ui',__name__,template_folder='templates')

@service_ui.route('/')
def index():
   client = get_cluster_client()
   try:
      info = client.info();
      print(info)
      timestamp = info.get('startedOn')
      if timestamp is not None:
         info['startedOn'] = datetime.fromtimestamp(timestamp / 1e3).isoformat()
      return render_template('cluster.html',metrics={},info=info)
   except ServiceError as err:
      return error_response(err.status_code,err.message)


assets = Blueprint('assets_ui',__name__)
@assets.route('/assets/<path:path>')
def send_asset(path):
   dir = current_app.config.get('ASSETS')
   if dir is None:
      dir = __file__[:__file__.rfind('/')] + '/assets/'
   return send_from_directory(dir, path)
