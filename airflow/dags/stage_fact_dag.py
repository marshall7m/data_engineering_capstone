from plugins import CreatedTableOperator, StageToRedshiftOperator, DataQualityOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow import DAG
from datetime import datetime

def stage_fact_s3_to_redshift(
    start_date,
    end_date,
    schedule_interval,
    parent_dag_name,
    redshift_conn_id,
    degree_list,
    s3_data,
    sql,
    s3_bucket,
    s3_key,
    iam_role,
    region,
    file_format,
    extra_copy_parameters='',
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

    task_id = f'stage_{s3_data}'
    dag = DAG(
        f'{parent_dag_name}.{task_id}',
        start_date=start_date,
        end_date=end_date,
        schedule_interval=schedule_interval,
        **kwargs
    )

    for degree in degree_list:
        table = f'{degree}_{s3_data}'

        start_task = DummyOperator(task_id=f'{degree}',  dag=dag)

        create_task = CreatedTableOperator(
            task_id=f'create_{table}_table',
            redshift_conn_id=redshift_conn_id,
            sql=sql.format(table),
            table=table,
            start_date=start_date
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
            degree=degree,
            region=region,
            file_format=file_format,
            extra_copy_parameters=extra_copy_parameters,
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