FROM python:3

RUN pip install python-binance \
    && pip install google-cloud-bigquery

WORKDIR /srv/
COPY /scripts/daily_export.py /srv/

CMD ["python", "./daily_export.py"]
