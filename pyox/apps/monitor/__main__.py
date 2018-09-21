
from pyhadoopapi.apps.monitor.app import app
import sys

if __name__ == '__main__':
   if len(sys.argv)>1:
      app.config.from_object(sys.argv[1])
   else:
      app.config.from_envvar('WEB_CONF')
   app.run()
