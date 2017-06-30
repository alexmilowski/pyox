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
