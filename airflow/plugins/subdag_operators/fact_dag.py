from operators.load_fact import LoadFactOperator
from operators.data_quality import DataQualityOperator
from operator_functions.upstream_staging_check import upstream_staging_check

from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from airflow import DAG
from datetime import datetime
from airflow.models import TaskInstance
from airflow.api.common.experimental.get_task_instance import get_task_instance



def create_fact_tables(
    parent_dag_name,
    child_dag_name,
    start_date,
    end_date,
    schedule_interval,
    redshift_conn_id,
    degree,
    origin_tables,
    destination_table,
    sql,
    upstream_subdag_id,
    *args, **kwargs):

    """
    Check if upstream staging dependencies were successful, loads data into fact table, and lastly perform a data quality check.

    Keyword Arguments:
    parent_dag_name -- Parent DAG name defined in `main_dag.py` dag object (str)
    child_dag_name -- Child DAG name used to define subdag ID (str)
    start_date -- DAG start date
    end_date -- DAG end date
    schedule_interval -- (e.g. '@monthly', '@weekly', etc.) (str)
    redshift_conn_id -- Redshift connection ID (str)
    degree -- Degree name (str)
    origin_tables -- Dictionary of staging table(s) used for sql mapping (dict)
    destination_table-- Fact table name (str)
    sql -- Fact table query (str)
    """

    dag = DAG(
        dag_id=f"{parent_dag_name}.{child_dag_name}",
        start_date=start_date,
        end_date=end_date,
        schedule_interval=schedule_interval,
        **kwargs)

    start_task = DummyOperator(task_id=f'{degree}',  dag=dag)

    upstream_check_task = PythonOperator(
        task_id=f'check_{destination_table}_dependencies',
        python_callable=upstream_staging_check,
        op_kwargs={'origin_tables':origin_tables, 'upstream_subdag_id':upstream_subdag_id},
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
    
    start_task >> upstream_check_task
    upstream_check_task >> create_task
    create_task >> check_task

    return dag