from pyox import Workflow
w = Workflow.start('spark-test','spark') \
      .action(
         'spark',
         Workflow.spark(
            'xyzzy','node','master','test','foo.jar',
            spark_opts='-X12M',
            arg=[1,2,3],
            file=['test.py','foo.py'])) \
      .kill('error','Cannot run spark')

print(str(w))
