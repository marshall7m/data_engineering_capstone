FROM python:3.7

WORKDIR /usr/local/airflow/
ENV AIRFLOW_HOME=/usr/local/airflow
COPY requirements.txt ./

RUN pip install 'apache-airflow[aws, postgres]'
RUN pip install --upgrade pip && \
    pip install --no-cache-dir -r requirements.txt

COPY script/entrypoint.sh /entrypoint.sh
COPY config/airflow.cfg $AIRFLOW_HOME/airflow.cfg

ENTRYPOINT ["/entrypoint.sh"]
CMD ["webserver"]
