
from pyox.apps.tracker.service import create_app, start_task_queue
import sys
import os
import logging
import logging.config

if __name__ == '__main__':
   logging.basicConfig(level=logging.INFO)
   logging.config.dictConfig({
       'version': 1,
       'disable_existing_loggers': False,  # this fixes the problem
       'formatters': {
           'standard': {
               'format': '%(asctime)s [%(levelname)s] %(name)s: %(message)s'
           },
       },
       'handlers': {
           'default': {
               'level':'INFO',
               'formatter' : 'standard',
               'class':'logging.StreamHandler',
           },
       },
       'loggers': {
           '': {
               'handlers': ['default'],
               'level': 'INFO',
               'propagate': True
           }
       }
   })

app = create_app('dataplatform_app')
value = os.environ.get('WEB_CONF')
if value is None:
   #print('Loading from {}'.format(sys.argv[1]))
   app.config.from_object(sys.argv[1])
else:
   #print('Loading from {}'.format(value))
   app.config.from_envvar('WEB_CONF')

if app.config.get('KEY') is None:
   key = app.config.get('KNOX').get('key')
   if key is None:
      from cryptography.fernet import Fernet, base64
      key = base64.b64encode(Fernet.generate_key()).decode('utf-8')
      print('Key generated:')
      print(key)
   app.config['KEY'] = key

if __name__ == '__main__':
   start_task_queue(app)
   app.run()
else:
   start_task_queue(app)
