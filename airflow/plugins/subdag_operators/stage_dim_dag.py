from operators.created_table_check import CreatedTableOperator
from operators.stage_redshift import StageToRedshiftOperator
from operators.data_quality import DataQualityOperator

from airflow.operators.dummy_operator import DummyOperator
from airflow import DAG
from datetime import datetime

def stage_dim_s3_to_redshift(
    parent_dag_name,
    child_dag_name,
    start_date,
    end_date,
    schedule_interval,
    redshift_conn_id,
    s3_data,
    create_sql,
    table,
    s3_bucket,
    s3_key,
    iam_role,
    region,
    file_format,
    *args, **kwargs):

    """
    Subdag used to create dimension table, copy data from s3 to Redshift dimension table and lastly perform a data quality check.

    Keyword Arguments:
    parent_dag_name -- Parent DAG name defined in `main_dag.py` dag object
    child_dag_name -- Child DAG name used to define subdag ID
    redshift_conn_id   -- Redshift connection ID (str)
    aws_credentials_id -- AWS connection ID (str)
    table -- Staging table name (str)
    create_sql -- Create staging table query (str)
    s3_bucket -- AWS S3 bucket name (str)
    s3_key -- AWS S3 bucket data directory/file (str)
    region -- Redshift cluster configured region (str)
    file_format -- File format for AWS S3 files  (currently only: 'JSON' or 'CSV') (str)
    """

    dag = DAG(
        dag_id=f"{parent_dag_name}.{child_dag_name}",
        start_date=start_date,
        end_date=end_date,
        schedule_interval=schedule_interval,
        **kwargs
    )

    start_task = DummyOperator(task_id=f'{table}',  dag=dag)
    
    create_task = CreatedTableOperator(
        task_id=f'create_{table}_table',
        redshift_conn_id=redshift_conn_id,
        create_sql=create_sql.format(table),
        table=table,
        provide_context=True
    )

    copy_task = StageToRedshiftOperator(
        task_id=f'staging_{table}_table',
        dag=dag,
        table=table,
        redshift_conn_id=redshift_conn_id,
        s3_bucket=s3_bucket,
        s3_key=s3_key,
        iam_role=iam_role,
        s3_data=s3_data, 
        region=region,
        file_format=file_format,
        provide_context=True
    )

    check_task = DataQualityOperator(
        task_id=f'data_quality_check_{table}',
        dag=dag,
        redshift_conn_id=redshift_conn_id,
        table=table,
        provide_context=True
    )

    start_task >> create_task
    create_task >> copy_task
    copy_task >> check_task

    return dag