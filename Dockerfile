FROM python:3

RUN pip install python-binance \
    && pip install google-cloud-bigquery

WORKDIR /srv/
