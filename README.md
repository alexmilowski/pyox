# Python Apache Knox (Hadoop) REST Client library

## CLI Usage

The command-line client can be run by:

```
python -m pyhadoopapi
```

Currently, there are two commands supported:

 * `hdfs` - commands for interacting with WebHDFS
 * `oozie` - commands for interacting with the Oozie Service for scheduling jobs

A KNOX gateway must be specified or it defaults to `localhost:50070`:

 * `--base` - the base URI of the Knox service
 * `--host` - the host and port of the Knox service
 * `--secure` - indicates TLS (https) should be used
 * `--gateway` - the Knox gateway name
 * `--auth` - the username and password (colon separated)

 The Knox gateway can either be completely specified by the `--base` option or
 specified in parts by `--secure`, `--host`, and `--gateway`.

### hdfs commands

  * `cat` - output the contents of a resource in HDFS
  * `cp` - copy file(s) to hdfs
  * `mv` - move files within hdfs
  * `ls` - list files (long format: -l)
  * `mkdir` - create directories
  * `rm` - remove files

For more information on options, use the `-h` option:

```
python -m pyhadoopapi hdfs ls -h
```

### oozie commands

 * `ls` - list jobs (by status, detailed, etc.)
 * `start` - start a job
 * `status` - show the job status

 ```
 python -m pyhadoopapi oozie ls -h
 ```

 To start jobs on oozie you can:

  * specify a JSON properties file for job properties via `-P`
  * specify a single property via `-p name value` or `--property name value`
  * specify the workflow definition via `-d file.xml`
  * copy resources to the job path via `-cp`
  * specify the name node (`--namenode`) or job tracker (`--tracker`) to override what is in the properties

### cluster commands

 * `info` - shows basic cluster information such as versions, status, etc.
 * `metrics` - shows information about applications, containers, cores, memory, and nodes
 * `scheduler` - shows the queues and their utilization

#### cluster info

Options:

 * `-r` - output the raw JSON response
 * `-p` - pretty print the JSON
 * `--status` - output cluster status
 * `--version` - output only the hadoop version
 * `-a` - output all information

#### cluster metrics

Options:

 * `-r` - output the raw JSON response
 * `-p` - pretty print the JSON

#### cluster scheduler

Options:

 * `-r` - output the raw JSON response
 * `-p` - pretty print the JSON
 * `--users` - show user utilization of queues


## API

The CLI uses a simple API that you can embed directly in your application.  Every client object has the
same parameters (all keywords)

 * `base` - the base URI of the knox service
 * `secure` - whether SSL transport is to be used (defaults to `False`, mutually exclusive with base)
 * `host` - the host name of the KNOX service (defaults to `localhost`, mutually exclusive with base)
 * `port` - the port of the KNOX service  (defaults to `50070`, mutually exclusive with base)
 * `gateway` - the gateway name to username
 * `username` - the authentication user
 * `password` - the authentication password

A simple HDFS client example:

```python
from pyhadoopapi import WebHDFS
hdfs = WebHDFS(base='https://knox.example.com/',gateway='bigdata',username='jane',password='xyzzy')
if not hdfs.make_directory('/user/bob/data/'):
   print('Can not make directory!')
```

(more documentation is to come!)

## Oozie Workflow DSL

A workflow for a job can be constructed by a DSL.  For example, a simple shell action to copy yarn logs:

```python
from pyhadoopapi import Oozie, Workflow
from io import StringIO

# create the oozie client
oozie = Oozie(base='https://knox.example.com/',gateway='bigdata',username='jane',password='xyzzy')

# create the job directory
oozie.createHDFSClient().make_directory('/user/jane/shell/')

# a workflow with a single shell aciton
workflow = \
   Workflow.start('invoke-shell','shell') \
      .action(
         'shell',
         Workflow.shell(
            'my-job-tracker','hdfs://sandbox',
            command,
            configuration=Workflow.configuration({
               'mapred.job.queue.name' : 'my-queue'
            }),
            argument=['application_1500977774979_2776'],
            file='/user/jane/shell/copy.sh
         )
      ).kill('error','Cannot run workflow shell')

# the script to execute
script = StringIO("""#!/bin/bash
yarn logs -applicationId $1 | hdfs dfs -put - /user/jane/shell/job.log
""")

# Copy whatever is necessary and submit the job via Oozie
jobid = oozie.submit(
   '/user/jane/shell/',
   properties={
      'oozie.use.system.libpath' : True,
      'user.name' : 'jane'
   },
   workflow=workflow,
   copy=[(script,'copy.sh')]
)

```

## Monitor Web Application

A simple flask application can provide a web UI and proxy to the cluster information and scheduler queues.  The
application can be run by:

```
python -m pyhadoopapi.apps.monitor conf
```

where `conf.py` is in your python import path and contains the application configuration.  Alternatively, you
can set the environment variable `WEB_CONF` to the location of this file.

The configuration can contain any of the standard flask configuration options.  The variable `KNOX` must
be present for the configuraiton of the Apache Knox gateway.

For example, `conf.py` might contain:

```python
DEBUG=True
KNOX={
   'base' : 'https://knox.example.com',
   'gateway': 'bigdata'
}
```

Any of the client configuration keywords are available (e.g., `service`,`base`, `secure`, `host`, `port`) except
the user authentication.  The user authentication for both the API and service are passed through to the Knox
Web Service.  You must have authentication credentials for Knox to use the Web application.

Once you have the application running, you can access it at the address you have configured.  By default, this is
http://localhost:5000/
