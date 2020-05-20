#!/usr/bin/env bash

# Global defaults and back-compat
# : "${AIRFLOW_HOME:="/usr/local/airflow"}"
# : "${AIRFLOW__CORE__FERNET_KEY:=${FERNET_KEY:=$(python -c "from cryptography.fernet import Fernet; FERNET_KEY = Fernet.generate_key().decode(); print(FERNET_KEY)")}}"
# : "${AIRFLOW__CORE__EXECUTOR:=${EXECUTOR:-Sequential}Executor}"


# export \
#   AIRFLOW_HOME 

  # AIRFLOW_HOME \
  # AIRFLOW__CORE__EXECUTOR \
  # AIRFLOW__CORE__FERNET_KEY 

# case "$1" in
#   webserver)
#     # airflow resetdb
#     airflow initdb
#     # airflow upgradedb
#     airflow scheduler &
#     exec airflow webserver
#     ;;
#   worker|scheduler)
#     # Give the webserver time to run initdb.
#     sleep 10
#     exec airflow "$@"
#     ;;
#   flower)
#     sleep 10
#     exec airflow "$@"
#     ;;
#   version)
#     exec airflow "$@"
#     ;;
#   *)
#     # The command is something like bash, not an airflow subcommand. Just run it in the right environment.
#     exec "$@"
#     ;;
# esac
    # airflow resetdb
    airflow initdb
    # airflow upgradedb
    airflow webserver -p 8080
    airflow scheduler 


    # exec airflow webserver