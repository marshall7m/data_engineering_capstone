FROM python:3.7.7

ENV AIRFLOW_HOME=/usr/local/airflow
WORKDIR /usr/local/airflow/
COPY requirements.txt ./

RUN pip install --upgrade pip && \
    pip install --no-cache-dir -r requirements.txt && \
    pip install 'apache-airflow[postgres]'


COPY script/entrypoint.sh /entrypoint.sh
COPY config/airflow.cfg $AIRFLOW_HOME/airflow.cfg

ENTRYPOINT ["/entrypoint.sh"]
CMD ["webserver"]
