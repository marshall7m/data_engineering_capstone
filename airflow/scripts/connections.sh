#!/usr/bin/env bash

#create list of all previous/default connections
declare -a arr=($(airflow connections --list))
#for every connection in airflow connection list, delete connection
for i in "${!arr[@]}"; do 
    if [ "${arr[$i]}" == '├────────────────────────────────┼─────────────────────────────┼────────────────────────────┼────────┼────────────────┼──────────────────────┼────────────────────────────────┤' ]; then
        conn_str="${arr["$(( $i + 2 ))"]}"
        echo "Deleting $conn_str"
        airflow connections --delete --conn_id $conn_str
    fi    
done

#get aws and redshift config values
. /config/redshift.cfg

# add redshift connection to airflow
airflow connections --add --conn_id redshift --conn_type postgres --conn_host $DWH_HOST \
                    --conn_login $DWH_DB_USER --conn_password $DWH_DB_PASSWORD \
                    --conn_schema $DWH_DB --conn_port $DWH_PORT

# add AWS connection to airflow

airflow connections --add --conn_id aws_credentials --conn_type aws \
                    --conn_login $KEY --conn_password $SECRET