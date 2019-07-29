from pyox import Oozie,Workflow,make_client
import argparse

params = None
def add_args(*args):
   global params
   if type(args[0])==argparse.ArgumentParser:
      parser = args[0]
      parser.add_argument('month')
      parser.add_argument('path')

      parser.add_argument(
         '--reducers',
         nargs='?',
         dest='reducers',
         type=int,
         default=4,
         metavar=('count'),
         help='The number of reducer tasks'
      )
   else:
      client = args[0]
      args = args[1]
      params = args


# month e.g. 2018-09
# path e.g. '/data/somewhere/{year}/{month}/{day}/*'

client,params = make_client(
   Oozie,
   arguments=[
      'month',
      'path',
      ['--reducers',{
         'nargs':'?',
         'dest':'reducers',
         'type':int,
         'default':4,
         'metavar':('count'),
         'help':'The number of reducer tasks'}]]
)

date_parts = params.month.split('-')
date = {'year' : date_parts[0], 'month' : date_parts[1] if len(date_parts)>1 else None}

job_path = '/user/{}/WORK/game-pipeline-{}-job'.format(client.username,params.month)
output_month = '/user/{}/WORK/games/{}/'.format(client.username,params.month)
output_content_by_aid = output_month + 'content-by-aid/'
output_total_by_aid = output_month + 'total-by-aid/'
output_plays_by_aid = output_month + 'plays-by-aid/'
output_percent_play = output_month + 'percent-play/'
output_location_play = output_month + 'location-play/'
output_summary = output_month + 'summary/'

global_mr = {
   'mapreduce.job.queuename' : 'MY_Q',
   'mapreduce.output.fileoutputformat.compress' : 'false',
   'stream.map.output.field.separator' : ',',
   'mapreduce.map.output.key.field.separator' : ',',
   'mapreduce.output.textoutputformat.separator' : ',',
   'mapreduce.input.keyvaluelinerecordreader.key.value.separator' : ','
}

wf = Workflow.start(
   'game-pipeline-{}'.format(params.month),
   'content-mr',
   job_tracker='sandbox-RMS:8032',
   name_node='hdfs://sandbox'
   ).action(
      'content-mr',
      Workflow.map_reduce(
         Workflow.streaming(
            mapper='python game-minutes-mapper.py',
            reducer='python reduce-by-content.py'
         ),
         prepare=Workflow.prepare(Workflow.delete(output_content_by_aid)),
         configuration={**global_mr,
            'mapreduce.job.reduces' : params.reducers,
            'mapreduce.input.fileinputformat.inputdir' : params.path + '{year}/{month}/*/DLV*'.format(**date),
            'mapreduce.output.fileoutputformat.outputdir' : output_content_by_aid,
            'stream.num.map.output.key.fields' : '2',
         },
         file=[job_path+'/game-minutes-mapper.py#game-minutes-mapper.py',job_path+'/reduce-by-content.py#reduce-by-content.py']
      )
   ).action(
      'total-mr',
      Workflow.map_reduce(
         Workflow.streaming(
            mapper='python game-minutes-mapper.py',
            reducer='python reduce-by-aid.py'
         ),
         prepare=Workflow.prepare(Workflow.delete(output_total_by_aid)),
         configuration={**global_mr,
            'mapreduce.job.reduces' : params.reducers,
            'mapreduce.input.fileinputformat.inputdir' : params.path + '{year}/{month}/*/DLV*'.format(**date),
            'mapreduce.output.fileoutputformat.outputdir' : output_total_by_aid,
            'stream.num.map.output.key.fields' : '1',
         },
         file=[job_path+'/game-minutes-mapper.py#game-minutes-mapper.py',job_path+'/reduce-by-aid.py#reduce-by-aid.py']
      )
   ).action(
      'location-mr',
      Workflow.map_reduce(
         Workflow.streaming(
            mapper='python location-mapper.py',
            reducer='python count-reducer.py'
         ),
         prepare=Workflow.prepare(Workflow.delete(output_location_play)),
         configuration={**global_mr,
            'mapreduce.job.reduces' : 1,
            'mapreduce.input.fileinputformat.inputdir' : params.path + '{year}/{month}/*/DLV*'.format(**date),
            'mapreduce.output.fileoutputformat.outputdir' : output_location_play,
            'stream.num.map.output.key.fields' : '1',
         },
         file=[job_path+'/location-mapper.py#location-mapper.py',job_path+'/count-reducer.py#count-reducer.py']
      )
   ).action(
      'play-mr',
      Workflow.map_reduce(
         Workflow.streaming(
            mapper='python game-play-mapper.py',
            reducer='python count-reducer.py'
         ),
         prepare=Workflow.prepare(Workflow.delete(output_plays_by_aid)),
         configuration={**global_mr,
            'mapreduce.job.reduces' : '1',
            'mapreduce.input.fileinputformat.inputdir' : output_content_by_aid,
            'mapreduce.output.fileoutputformat.outputdir' : output_plays_by_aid,
            'stream.num.map.output.key.fields' : '1',
         },
         file=[job_path+'/game-play-mapper.py#game-play-mapper.py',job_path+'/count-reducer.py#count-reducer.py']
      )
   ).action(
      'percent-play-mr',
      Workflow.map_reduce(
         Workflow.streaming(
            mapper='python merge-mapper.py',
            reducer='python merge-reducer.py'
         ),
         prepare=Workflow.prepare(Workflow.delete(output_percent_play)),
         configuration={**global_mr,
            'mapreduce.job.reduces' : '1',
            'mapreduce.input.fileinputformat.inputdir' : output_total_by_aid + ',' + output_content_by_aid,
            'mapreduce.output.fileoutputformat.outputdir' : output_percent_play,
            'stream.num.map.output.key.fields' : '2',
         },
         file=[job_path+'/merge-mapper.py#merge-mapper.py',job_path+'/merge-reducer.py#merge-reducer.py']
      )
   ).action(
      'summary-mr',
      Workflow.map_reduce(
         Workflow.streaming(
            mapper='python summary-mapper.py',
            reducer='python count-reducer.py'
         ),
         prepare=Workflow.prepare(Workflow.delete(output_summary)),
         configuration={**global_mr,
            'mapreduce.job.reduces' : '1',
            'mapreduce.input.fileinputformat.inputdir' : output_content_by_aid + ',' + output_total_by_aid + ',' + output_plays_by_aid + ',' + output_percent_play,
            'mapreduce.output.fileoutputformat.outputdir' : output_summary,
            'stream.num.map.output.key.fields' : '1',
         },
         file=[job_path+'/summary-mapper.py#summary-mapper.py',job_path+'/count-reducer.py#count-reducer.py']
      )
   ).kill('error','Cannot run pipeline')

print(wf)

# If you had actual files and data, you could submit the job as follows:
#
# files = ['game-minutes-mapper.py','reduce-by-content.py','reduce-by-aid.py','game-play-mapper.py','location-mapper.py','count-reducer.py','merge-mapper.py','merge-reducer.py','summary-mapper.py']
#
# jobid = client.submit(
#    job_path,
#    properties={
#       'oozie.use.system.libpath' : True,
#       'user.name' : client.username
#    },
#    workflow=wf,
#    copy=files,
#    verbose=client.verbose
# )
# print(jobid)
#
