from operators.created_table_check import CreatedTableOperator
from operators.stage_redshift import StageToRedshiftOperator
from operators.data_quality import DataQualityOperator
from airflow.operators.python_operator import (PythonOperator, BranchPythonOperator)
from operator_functions.staging_success_check import staging_success_check
from operators.stl_check import STLCheckOperator 

from airflow.operators.dummy_operator import DummyOperator
from airflow import DAG
from datetime import datetime

def stage_fact_s3_to_redshift(
    parent_dag_name,
    child_dag_name,
    start_date,
    end_date,
    schedule_interval,
    redshift_conn_id,
    degree_list,
    s3_data,
    create_sql,
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
    parent_dag_name -- Parent DAG name defined in `main_dag.py` dag object
    child_dag_name -- Child DAG name used to define subdag ID
    start_date -- DAG start date
    end_date -- DAG end date
    schedule_interval -- (e.g. '@monthly', '@weekly', etc.)
    redshift_conn_id   -- Redshift connection ID (str)
    degree_list -- List of degree names (list)
    aws_credentials_id -- AWS connection ID (str)
    s3_bucket -- AWS S3 bucket name (str)
    s3_date -- S3 data name used to format staging table name
    create_sql -- SQL used to create staging table 
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

    for degree in degree_list:
        table = f'{degree}_{s3_data}'
        error_table = f'{table}_errors'

        start_task = DummyOperator(task_id=f'{degree}',  dag=dag)

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
            degree=degree,
            region=region,
            file_format=file_format,
            extra_copy_parameters=extra_copy_parameters,
            provide_context=True
            )

        #push count to xcom for stl count comparison
        count_check_task = DataQualityOperator(
            task_id=f'data_quality_check_{table}',
            dag=dag,
            redshift_conn_id=redshift_conn_id,
            table=table,
            provide_context=True
        )

        check_stl_branch = STLCheckOperator(
            task_id=f'stl_check_{table}',
            table=table,
            error_table=error_table,
            redshift_conn_id=redshift_conn_id
        )

        staging_success_task = PythonOperator(
            task_id=f'staging_success_check_{table}',
            python_callable=staging_success_check,
            op_kwargs={'redshift_conn_id': redshift_conn_id, 'table': table, 'error_table': error_table},
            dag=dag,
            provide_context=True
        )

        start_task >> create_task
        create_task >> copy_task
        copy_task >> [check_stl_branch, count_check_task]
        check_stl_branch >> staging_success_task

    return dag