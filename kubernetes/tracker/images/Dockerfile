FROM python:3.6

MAINTAINER Alex Miłowski <alex@milowski.com>

RUN pip install gunicorn Flask requests redis cryptography pyhadoopapi
RUN mkdir /logs
RUN mkdir /service
COPY gunicorn.conf /gunicorn.conf
COPY logging.conf /logging.conf
COPY runner.py /service/runner.py

ENV PYTHONPATH "/"
ENV SERVICE_CONF "/conf/service.json"

EXPOSE 8888

ENTRYPOINT ["/usr/local/bin/gunicorn", "--config", "/gunicorn.conf", "--log-config", "/logging.conf", "-b", ":8888", "--log-file", "/logs/gunicorn.log", "--access-logfile", "/logs/access.log", "--error-logfile", "/logs/error.log","service.runner:app"]
