FROM python:3.7.7

ENV AIRFLOW_HOME=/usr/local/airflow
WORKDIR /usr/local/airflow/
COPY requirements.txt ./

RUN pip install --upgrade pip && \
    pip install --no-cache-dir -r requirements.txt && \
    pip install 'apache-airflow[postgres]'

RUN mkdir config
COPY scripts/entrypoint.sh /entrypoint.sh
COPY scripts/connections.sh /connections.sh
COPY config/aws.cfg /config/aws.cfg
COPY config/airflow.cfg $AIRFLOW_HOME/airflow.cfg

ENTRYPOINT ["/entrypoint.sh"]
CMD ["webserver"]
