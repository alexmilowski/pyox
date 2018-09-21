
# Setup Logging
import logging
import logging.config
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

# Setup Application
from pyox.apps.tracker.service import create_app, start_task_queue
from pyox import ServiceError
import json
import os


# Read service configuration from JSON file
conf_file = os.environ.get('SERVICE_CONF')

if conf_file is None:
   conf_file = '/conf/service.json'

with open(conf_file) as json_data:
   conf = json.load(json_data)

# Create application and associate with configuration
app = create_app('tracker_service')
@app.errorhandler(ServiceError)
def handle_service_error(e):
   return e.message, e.status_code

app.config['KNOX'] = conf

# The key is currently a separate dictionary item. If the encryption key does not exist, create it.
key = conf.get('key')
if key is None:
   from cryptography.fernet import Fernet, base64
   key = base64.b64encode(Fernet.generate_key()).decode('utf-8')
   print('Key generated:')
   print(key)

app.config['KEY'] = key

# Setup the Redis host name
namespace = os.environ.get('MY_NAMESPACE')
if namespace is None:
   namespace = 'default'

app.config['REDIS_HOST'] = 'redis-master.' + namespace

# Start the task queue
start_task_queue(app)

# Gunicorn takes care the of invocation of Flask at this point
