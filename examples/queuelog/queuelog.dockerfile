FROM python:3.6

MAINTAINER Alex Miłowski <alex@milowski.com>

RUN pip install requests
RUN mkdir /logs
COPY pyhadoopapi /pyhadoopapi/
COPY examples/queuelog/queuelog.py /queuelog.py

ENTRYPOINT [ "python", "/queuelog.py","--log-prefix", "/logs/queues", "--log-period-type", "H", "--log-period-interval", "1", "--interval","60", "-q" ]
