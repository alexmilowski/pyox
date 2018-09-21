from redis import Redis
from redis.lock import Lock
import logging
import base64
import os
from datetime import datetime
from cryptography.fernet import Fernet

from uuid import uuid4

TASK_LIST_KEY = 'dataplatform.service.tasks'

def task_list(redis):
   return redis.hkeys(TASK_LIST_KEY)

def task_authenticate(redis,key,username,password):
   id = uuid4()
   auth = username+':'+password
   bauth = auth.encode('utf-8')
   if type(key)==str:
      key = key.encode('utf-8')
   f = Fernet(key)
   redis.set(id,f.encrypt(bauth))
   return id

def task_authentication(redis,key,id):
   if type(key)==str:
      key = key.encode('utf-8')
   f = Fernet(key)
   bauth = redis.get(id)
   if type(bauth)==str:
      bauth = bauth.encode('utf-8')
   auth = f.decrypt(bauth).decode('utf-8')
   return auth.split(':')

def task_create(redis,**kwargs):
   task_id = uuid4()
   for name in kwargs:
      redis.hset(task_id,name,kwargs[name])
   redis.hset(TASK_LIST_KEY,task_id,datetime.now().isoformat())
   return task_id

def task_lock(redis,id,timeout=60):
   lock_name = id+'.lock'
   if redis.setnx(lock_name,datetime.now().isoformat()):
      return True
   else:
      tstamp = redis.get(lock_name)
      locked_on = datetime.strptime(tstamp,"%Y-%m-%dT%H:%M:%S.%f")
      delta = datetime.now() - locked_on
      if delta.seconds > timeout:
         redis.delete(lock_name)
         return task_lock(redis,id,timeout=timeout)
      else:
         return False

def task_unlock(redis,id):
   lock_name = id+'.lock'
   redis.delete(lock_name)

def task_get(redis,id):
   task = {'id':id}
   for name in redis.hkeys(id):
      task[name] = redis.hget(id,name)
   return task

def task_set_properties(redis,id,**kwargs):
   for name in kwargs:
      redis.hset(id,name,kwargs[name])

def task_set_property(redis,id,name,value):
   redis.hset(id,name,value)

def task_delete_properties(redis,id,*args):
   for name in args:
      redis.hdel(id,name)

def task_delete_property(redis,id,name):
   redis.hdel(id,name)

def task_get_properties(redis,id,*args):
   return list(map(lambda name:redis.hdget(id,name),args))

def task_get_property(redis,id,name):
   redis.hget(id,name)

def task_delete(redis,id):
   for name in redis.hkeys(id):
      redis.hdel(id,name)
   redis.hdel(TASK_LIST_KEY,id)
