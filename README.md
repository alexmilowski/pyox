# Python Apache Hadoop / Knox REST Client library

This client library can access the variety of REST APIs provided by Haddop
either directly or through [Apache Knox](https://knox.apache.org).  The
individual protocols are wrapped into classes that know how to interact with
the protocol and simplify access.  In addition, the usage is uniform regardless
of whether you access the service directly or through a Knox gateway.

In addition, the library is "proxy aware" in case you have additional network
proxies.

## CLI Usage
A simple command-line client allows you to access Knox over the gateway.
The command-line client can be run by:

```
python -m pyhadoopapi
```

Currently, there are two commands supported:

 * `hdfs` - commands for interacting with WebHDFS
 * `oozie` - commands for interacting with the Oozie Service for scheduling jobs
 * `submit` - a simplified single-action submit command for Oozie
 * `cluster` - cluster status and queue information

A KNOX gateway must be specified or it defaults to `localhost:50070`:

 * `--base` - the base URI of the Knox service
 * `--host` - the host and port of the Knox service
 * `--secure` - indicates TLS (https) should be used
 * `--gateway` - the Knox gateway name
 * `--auth` - the username and password (colon separated)

The Knox gateway can either be completely specified by the `--base` option or
specified in parts by `--secure`, `--host`, and `--gateway`.

A proxy for a protocol can be specified by the `-p` option and requires a protocol
scheme (e.g., `https`) and the proxy url.


### hdfs commands

```bash
python -m pyhadoopapi hdfs *command* ...
```

#### hdfs cat

Outputs the file paths to stdout.

```bash
python -m pyhadoopapi hdfs cat [--offset N] [--length N] path ...
```

Options:

  * `--length N` - output N bytes of the file
  * `--offset N` - start at N bytes offset into the file


#### hdfs download

Outputs the file paths to stdout.

```bash
python -m pyhadoopapi hdfs download [-v] [--chunk-size N] [-o file] file
```

Options:

  * `--chunk-size N` - download the file in chunks of size N bytes
  * `-v` - verbose (show download status)

#### hdfs ls

A directory or file listing.

```bash
python -m pyhadoopapi hdfs ls [-b] [-l] path ...
```

Options:

  * `-b` - show the file sizes in bytes
  * `-l` - show the file details (long format)

#### hdfs mkdir

Create directories

```bash
python -m pyhadoopapi hdfs mkdir path ...
```

#### hdfs mv

Move a file

```bash
python -m pyhadoopapi hdfs mv source destination
```

#### hdfs rm

Move a file

```bash
python -m pyhadoopapi hdfs rm [-r] path ...
```

Options:

  * `-r` - recursively remove files


#### hdfs upload

Copy a set of files/direcrories to the target destination.

```bash
python -m pyhadoopapi hdfs upload [-f] [-r] [-s] [-v] source ... destination/
```

Copy a single file to a destination.

```bash
python -m pyhadoopapi hdfs upload [-f] [-s] [-v] source destination
```

Options:

  * `-f` - force (overwrite files)
  * `-r` - recursively upload
  * `-s` - send file size
  * `-v` - verbose (show download status)



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

There are three main API classes:

 * `WebHDFS` - an HDFS client
 * `Oozie` - an Oozie workflow client
 * `ClusterInformation` - a cluster information client

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
