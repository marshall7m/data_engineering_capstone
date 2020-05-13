FROM puckel/docker-airflow:latest

WORKDIR /usr/local/airflow/
ARG AIRFLOW_VERSION=1.10.9
ARG AIRFLOW_USER_HOME=/usr/local/airflow
ENV AIRFLOW_HOME=${AIRFLOW_USER_HOME}
COPY requirements.txt ./

# RUN pip install --upgrade pip && \
#     pip install --no-cache-dir -r requirements.txt
#     useradd -ms /bin/bash -d ${AIRFLOW_USER_HOME} airflow
# RUN useradd -ms /bin/bash -d ${AIRFLOW_USER_HOME} airflow
COPY script/entrypoint.sh /entrypoint.sh
COPY config/airflow.cfg ${AIRFLOW_USER_HOME}/airflow.cfg

# RUN chown -R airflow: ${AIRFLOW_USER_HOME}

# EXPOSE 8080 5555 8793

USER airflow
WORKDIR ${AIRFLOW_USER_HOME}
ENTRYPOINT ["/entrypoint.sh"]
CMD ["webserver"]