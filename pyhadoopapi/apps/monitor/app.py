from flask import Flask, request, g, session, redirect, abort
#from flask.ext.session import Session

app = Flask(__name__)
#Session(app)

from pyhadoopapi.apps.monitor.views import cluster_ui
from pyhadoopapi.apps.monitor.views import assets
from pyhadoopapi.apps.monitor.api import cluster_api

app.register_blueprint(cluster_ui,url_prefix='/')
app.register_blueprint(cluster_api,url_prefix='/api/cluster')
app.register_blueprint(assets)

#@app.before_request
#def before_request():
#   if 'username' in session:
#      username = session['username']
#      g.user = app.config['AUTH_SERVICE'].getUser(username)
#   authenticated = 'user' in g and g.user is not None
#
#   if request.path.startswith('/assets'):
#      return
#   if (request.path == '/' or request.path == '/logout') and not authenticated:
#      return redirect('/login')
#   if request.path != '/login' and not authenticated:
#      abort(401)
