from pyhadoopapi.cluster import ClusterInformation
from pyhadoopapi.client import ServiceError
from datetime import datetime
import argparse
import json

def cluster_info_command(client,argv):
   cmdparser = argparse.ArgumentParser(prog='pyhadoopapi cluster info',description='info')
   cmdparser.add_argument(
      '-r',
      action='store_true',
      dest='raw',
      default=False,
      help="Raw")
   cmdparser.add_argument(
      '-p',
      action='store_true',
      dest='pretty',
      default=False,
      help="Pretty print JSON")
   cmdparser.add_argument(
      '--status',
      action='store_true',
      dest='show_status',
      default=False,
      help="Show status")
   cmdparser.add_argument(
      '--version',
      action='store_true',
      dest='show_version',
      default=False,
      help="Show Version")
   cmdparser.add_argument(
      '-a',
      action='store_true',
      dest='all',
      default=False,
      help="Show all information")
   args = cmdparser.parse_args(argv)

   info = client.info()
   if args.raw:
      if args.pretty:
         print(json.dumps(info,sort_keys=True,indent=3))
      else:
         print(info)
      return

   id = info['id']

   startedOn = datetime.fromtimestamp(info['startedOn'] / 1e3).isoformat()
   version = info['resourceManagerVersion']
   state = info['state']

   if args.show_version and not args.all:
      print(version)
   elif args.all:
      build = info['resourceManagerVersionBuiltOn']
      hadoopBuild = info['hadoopBuildVersion']
      builtOn = info['hadoopVersionBuiltOn']
      print('started:\t{}'.format(startedOn))
      print('version:\t{}'.format(version))
      print('build:\t\t{}'.format(build))
      print('hadoop:\t\t{}'.format(hadoopBuild))
      print('built on:\t{}'.format(builtOn))

   if args.show_status or args.all:
      haState = info['haState']
      zookeeperState = info['haZooKeeperConnectionState']
      print('state:\t\t{}'.format(state))
      print('ha:\t\t{}'.format(haState))
      print('zookeeper:\t{}'.format(zookeeperState))

   if not args.show_version and not args.show_status and not args.all:
      print('{} is {} at {}'.format(version,state,startedOn))

metrics_labels = {
   "Applications" : [
      ("appsSubmitted","Submitted"),
      ("appsPending","Pending"),
      ("appsRunning","Running"),
      ("appsCompleted","Completed"),
      ("appsFailed","Failed"),
      ("appsKilled","Killed"),
   ],
   "Memory" : [
      ("allocatedMB","Allocated"),
      ("reservedMB","Reserved"),
      ("availableMB","Available"),
      ("totalMB","Total"),
   ],
   "Cores" : [
      ("reservedVirtualCores","Reserved"),
      ("availableVirtualCores","Available"),
      ("allocatedVirtualCores","Allocated"),
      ("totalVirtualCores","Total"),
   ],
   "Containers" : [
      ("containersAllocated","Allocated"),
      ("containersReserved","Reserved"),
      ("containersPending","Pending"),
   ],
   "Nodes" : {
      ("activeNodes","Active"),
      ("lostNodes","Lost"),
      ("unhealthyNodes","Unhealthy"),
      ("decommissionedNodes","Decommissioned"),
      ("rebootedNodes","Rebooted"),
      ("totalNodes","Total")
   }
}

def cluster_metrics_command(client,argv):
   cmdparser = argparse.ArgumentParser(prog='pyhadoopapi cluster metrics',description='metrics')
   cmdparser.add_argument(
      '-r',
      action='store_true',
      dest='raw',
      default=False,
      help="Raw")
   cmdparser.add_argument(
      '-p',
      action='store_true',
      dest='pretty',
      default=False,
      help="Pretty print JSON")

   args = cmdparser.parse_args(argv)

   metrics = client.metrics()

   if args.raw:
      if args.pretty:
         print(json.dumps(metrics,sort_keys=True,indent=3))
      else:
         print(metrics)
      return

   for key in sorted(metrics_labels.keys()):
      properties = metrics_labels[key]
      print('\n'+key+':\n')
      for prop in properties:
         print('{:>15s}: {}'.format(prop[1],metrics.get(prop[0])))

def print_queue(depth,queueInfo,show_users=False):
   indent = depth*3
   width = 32 - indent
   formatstr = '{'+(':'+str(indent) if depth>0 else '')+'}{:'+str(width)+'} {:6.2f} {:6.2f} {:6.2f}'
   active = queueInfo.get('numActiveApplications')
   pending = queueInfo.get('numPendingApplications')
   if active is not None:
      formatstr += ' {:3d} {:3d}'
   print(formatstr.format('',queueInfo.get('queueName'),queueInfo.get('capacity'),queueInfo.get('maxCapacity'),queueInfo.get('usedCapacity'),active,pending))
   if show_users:
      users = queueInfo.get('users')
      if users is not None:
         formatstr = '{'+(':'+str(indent+3) if depth>0 else '')+'}{:'+str(width-3)+'} {:3} {:3} {:4} {:>8}'
         print(formatstr.format('','USERNAME','ACT','PEN','CORE','MEMORY'))
         for user in users['user']:
            resourcesUsed = user['resourcesUsed']
            print(formatstr.format('',user['username'],user['numActiveApplications'],user['numPendingApplications'],resourcesUsed['vCores'],resourcesUsed['memory']))

   queues = queueInfo.get('queues')
   if queues is not None:
      for queue in queues['queue']:
         print_queue(depth+1,queue,show_users=show_users)


def cluster_scheduler_command(client,argv):
   cmdparser = argparse.ArgumentParser(prog='pyhadoopapi cluster scheduler',description='scheduler')
   cmdparser.add_argument(
      '-r',
      action='store_true',
      dest='raw',
      default=False,
      help="Raw")
   cmdparser.add_argument(
      '-p',
      action='store_true',
      dest='pretty',
      default=False,
      help="Pretty print JSON")
   cmdparser.add_argument(
      '--users',
      action='store_true',
      dest='show_users',
      default=False,
      help="Show users in queues")

   args = cmdparser.parse_args(argv)

   schedulerInfo = client.scheduler()

   if args.raw:
      if args.pretty:
         print(json.dumps(schedulerInfo,sort_keys=True,indent=3))
      else:
         print(schedulerInfo)
      return

   print('{:32} {:6} {:6} {:6} {:3} {:3}'.format('NAME',' MIN',' MAX','USED','ACT','PEN'))
   print_queue(0,schedulerInfo,show_users=args.show_users)


cluster_commands = {
   'info' : cluster_info_command,
   'metrics' : cluster_metrics_command,
   'scheduler' : cluster_scheduler_command
}

def cluster_command(args):

   client = ClusterInformation(base=args.base,secure=args.secure,host=args.hostinfo[0],port=args.hostinfo[1],gateway=args.gateway,username=args.user[0],password=args.user[1])
   client.proxies = args.proxies
   client.verify = args.verify
   if args.verbose:
      client.enable_verbose()

   if len(args.command)==0:
      raise ValueError('One of the following comamnds must be specified: {}'.format(' '.join(cluster_commands.keys())))

   func = cluster_commands.get(args.command[0])
   if func is None:
      raise ValueError('Unrecognized command: {}'.format(args.command[0]))

   func(client,args.command[1:])
