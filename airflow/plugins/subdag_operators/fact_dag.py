from operators.load_fact import LoadFactOperator
from operators.data_quality import DataQualityOperator
from operators.fact_branch import FactBranchOperator

from airflow.operators.dummy_operator import DummyOperator
from airflow import DAG
from datetime import datetime
from airflow.operators.python_operator import BranchPythonOperator

def create_fact_tables(
    start_date,
    end_date,
    schedule_interval,
    parent_dag_name,
    subdag_name,
    redshift_conn_id,
    degree_list,
    origin_table_format,
    destination_table_format,
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
        destination_table = destination_table_format.format(degree=degree)
        origin_tables = {table:name.format(degree=degree) for (table,name) in origin_table_format.items()}

        start_task = DummyOperator(task_id=f'{degree}',  dag=dag)

        branch_task = FactBranchOperator(
            task_id=f'{degree}_check_origin_tables',
            dag=dag,
            origin_tables=origin_tables,
            destination_table=destination_table,
            provide_context=True
        )
        
        create_task = LoadFactOperator(
            task_id=destination_table,
            dag=dag,
            sql=sql,
            redshift_conn_id=redshift_conn_id,
            destination_table=destination_table,
            origin_tables=origin_tables,
            provide_context=True
        )

        check_task = DataQualityOperator(
            task_id='data_quality_check',
            dag=dag,
            redshift_conn_id=redshift_conn_id,
            table=destination_table,
            provide_context=True
        )
        
        skip_task = DummyOperator(task_id='skipped',  dag=dag)
        
        start_task >> branch_task
        branch_task >> [create_task, skip_task]
        create_task >> check_task

    return dag