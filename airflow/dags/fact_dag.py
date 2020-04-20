from operators import (LoadFactOperator, DataQualityOperator)
from airflow.operators.dummy_operator import DummyOperator
from airflow import DAG
from datetime import datetime

def create_fact_tables(
    start_date,
    end_date,
    schedule_interval,
    parent_dag_name,
    subdag_name,
    redshift_conn_id,
    degree_list,
    s3_data,
    origin_table,
    fact,
    sql,
    *args, **kwargs):

    """
    Subdag used to create staging table, copy data from s3 to staging table in redshift and lastly perform a data quality check.

    Keyword Arguments:
    parent_dag_name -- Parent dag name (dag_id parameter) defined in parent DAG variable (str)
    redshift_conn_id   -- Redshift connection ID configured in Airflow/admin/connection UI (str)
    aws_credentials_id -- AWS connection ID configured in Airflow/admin/connection UI (str)
    table -- Staging table name (str)
    sql -- Create staging table query (str)
    s3_bucket -- AWS S3 bucket name (str)
    s3_key -- AWS S3 bucket data directory/file (str)
    region -- Redshift cluster configured region (str)
    file_format -- File format for AWS S3 files  (currently only: 'JSON' or 'CSV') (str)
    """

    dag = DAG(
        f'{parent_dag_name}.{subdag_name}',
        start_date=start_date,
        end_date=end_date,
        schedule_interval=schedule_interval,
        **kwargs
    )

    for degree in degree_list:
        table = f'{degree}_{s3_data}_{fact}'

        start_task = DummyOperator(task_id=f'{degree}',  dag=dag)

        create_task = LoadFactOperator(
            task_id=table,
            dag=dag,
            origin_table=origin_table,
            destination_table=table,
            sql=sql,
            redshift_conn_id=redshift_conn_id,
            provide_context=True
        )

        check_task = DataQualityOperator(
            task_id=f'{degree}_data_quality_check',
            dag=dag,
            redshift_conn_id=redshift_conn_id,
            table=table,
            provide_context=True
        )

        start_task >> create_task
        create_task >> check_task

    return dag