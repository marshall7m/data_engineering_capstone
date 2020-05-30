#!/usr/bin/env bash

TRY_LOOP="20"

# Global defaults and back-compat
: "${AIRFLOW_HOME:="/usr/local/airflow"}"
: "${AIRFLOW__CORE__FERNET_KEY:=${FERNET_KEY:=$(python -c "from cryptography.fernet import Fernet; FERNET_KEY = Fernet.generate_key().decode(); print(FERNET_KEY)")}}"
: "${AIRFLOW__CORE__EXECUTOR:=${EXECUTOR:-Sequential}Executor}"


export \
  AIRFLOW_HOME \
  AIRFLOW__CORE__FERNET_KEY \
  AIRFLOW__CORE__EXECUTOR

case "$1" in
  webserver)
    echo 'initialize airflow db'
    airflow initdb
    echo 'upgrade db'
    airflow upgradedb
    echo 'deleting previous connections and adding connections in airflow:'
    bash /connections.sh
    if [ "$AIRFLOW__CORE__EXECUTOR" = "LocalExecutor" ] || [ "$AIRFLOW__CORE__EXECUTOR" = "SequentialExecutor" ]; 
    then
      airflow scheduler &
    fi
    exec airflow webserver -p 8080
    ;;
  worker|scheduler)
    # Give the webserver time to run initdb.
    sleep 10
    exec airflow "$@"
    ;;
  flower)
    sleep 10
    exec airflow "$@"
    ;;
  version)
    exec airflow "$@"
    ;;
  *)
    # The command is something like bash, not an airflow subcommand. Just run it in the right environment.
    exec "$@"
    ;;
esac

