# Tracker Microservice

## Overview

The Tracker microservice interacts with Knox and Oozie to provide the current
state of jobs submitted to it for tracking. When jobs fail, it will also attempt
to find and store application container logs in a safe place for later
inspection.

In a multi-tenant hadoop cluster, the cluster policies may not fully support
the needs of the developer. Specifically, the retention period of logs files
may be too short - especially for long-running jobs. Jobs that fail an
inopportune times may have their essential information deleted before the
developer can inspect them.

The tracker service can intervene in this situation and copy the application
logs into HDFS for later perusal.

## API

All examples assume a service running at `http://tracker.example.com`.

### Task: track a job: `/api/task/track/`

The job is tracked through completion (SUCCEEDED, FAILED, or KILLED). If the
status is not 'SUCCEEDED', the application containers are inspected and the
various log files are copied to `/user/{username}/WORK/logs/{id}/{action_id}.log`.

Example:

```
curl -u user:password -H "Content-Type: text/plain" --data 0030161-180716060648641-oozie-W http://tracker.example.com/api/task/track/
```

The post body can be either:

  * `text/plain` with one job id per line
  * `application/json` as an array of strings of job ids
  * `application/json` as an object with an `"id"` property containing the job id


### Task: copy logs: `/api/task/copy-logs/`

The application containers for the job are inspected and the
various log files are copied to `/user/{username}/WORK/logs/{id}/{action_id}.log`.

Example:

```
curl -u user:password -H "Content-Type: text/plain" --data 0030161-180716060648641-oozie-W  http://tracker.example.com/api/task/copy-logs/
{"finished": false, "succeeded": false, "jobs": [{"id": "0030161-180716060648641-oozie-W", "application": "1529519049029_420001", "status": "RUNNING", "job": "0030595-180716060648641-oozie-W"}]}
```

The post body can be either:

  * `text/plain` with one job id per line
  * `application/json` as an array of strings of job ids
  * `application/json` as an object with an `"id"` property containing the job id

### Information: get job information: `/api/job/{id}`

Retrieves the job information (e.g. application container ids).
```
curl -u "user:password"  http://tracker.example.com/api/job/0030161-180716060648641-oozie-W
{"status": "SUCCEEDED", "applications-ids": ["1529519049029_420001"], "log-jobs": {}, "id": "0030161-180716060648641-oozie-W"}
```

### Information: get job status: `/api/job/{id}/status`

Retrieves the job status (maybe be cached; faster response).
```
curl -u "user:password"  http://tracker.example.com/api/job/0030161-180716060648641-oozie-W/status
{"id": "0030161-180716060648641-oozie-W", "status": "SUCCEEDED", "status_code": 200}
```


### Information: get all job logs: `/api/job/{id}/logs`

Retrieves the all the job application container log files concatenated together.

```
curl -u "user:password"  http://tracker.example.com/api/job/0030161-180716060648641-oozie-W/logs
... log text ...
```
### Information: get job application log: `/api/job/{id}/logs/{app_id}`

Retrieves a specific application container log file.

```
curl -u "user:password"  http://tracker.example.com/api/job/0030161-180716060648641-oozie-W/logs/1529519049029_420001
... log text ...
```

### Information: get copy logs status: `/api/job/{id}/logs/status`

Retrieves the status of the application container log file copy jobs.

```
curl -u "user:password"  http://tracker.example.com/api/job/0030161-180716060648641-oozie-W/logs/status
{"finished": true, "succeeded": true, "jobs": {"1529519049029_420001": {"id": "0030161-180716060648641-oozie-W", "application": "1529519049029_420001", "job": "0030595-180716060648641-oozie-W", "status": "SUCCEEDED"}}}
```
### Information: list the jobs: `/api/jobs/`

Retrieves the current set of workflow jobs.

```
curl -u "user:password"  http://tracker.example.com/api/jobs/
... loads of json ...
```

### Information: list the jobs being tracked: `/api/jobs/tracking`

Retrieves the current set of workflow jobs being tracked.

```
curl -u "user:password"  http://tracker.example.com/api/jobs/
[{"status": "SUCCEEDED", "application-ids": ["1529519049029_420001"], "action-copy-job-1529519049029_420001": "0030595-180716060648641-oozie-W", "id": "0030161-180716060648641-oozie-W", "applications": {"1529519049029_420001": {"id": "0030161-180716060648641-oozie-W", "application": "1529519049029_420001", "job": "0030595-180716060648641-oozie-W", "status": "SUCCEEDED"}}}]
```
