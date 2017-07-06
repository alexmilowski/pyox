# Queue Logging Client

This example is a simple queue log program that will log queue utilization over
time.  You can either log the information via python logging or provide a
handler function in python to store data.

## Usage

```
usage: queuelog [-h] [--base [BASE]] [--host [HOST]] [--secure]
                [--gateway [GATEWAY]] [--auth AUTH] [-p protocol url]
                [--no-verify] [-v] [--parameter PARAMETERS] [--client CLIENT]
                [--log-config LOG_CONFIG] [--log-prefix LOG_PREFIX] [-q]
                [--no-log-file] [--interval [INTERVAL]]

Hadoop Queue Log

optional arguments:
  -h, --help            show this help message and exit
  --base [BASE]         The base URI of the service
  --host [HOST]         The host of the service (may include port)
  --secure              Use TLS transport (https)
  --gateway [GATEWAY]   The KNOX gateway name
  --auth AUTH           The authentication for the request (colon separated
                        username/password)
  -p protocol url, --proxy protocol url
                        A protocol proxy
  --no-verify           Do not verify SSL certificates
  -v, --verbose         Output detailed information about the request and
                        response
  --parameter PARAMETERS
                        A client parameter to be passed to the handler
  --client CLIENT       The client module to import
  --log-config LOG_CONFIG
                        A logging configuration (json or ini file format)
  --log-prefix LOG_PREFIX
                        The prefix to use for the queue log files
  -q, --quiet           Suppress the console log
  --no-log-file         Suppress the log file
  --interval [INTERVAL]
                        The polling interval in seconds.

```
