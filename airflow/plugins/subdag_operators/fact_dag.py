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
    degree_list,
    origin_table_format,
    destination_table_format,
    sql,
    upstream_subdag_id,
    *args, **kwargs):

    """
    Check if upstream staging dependencies were successful, loads data into fact table, and lastly perform a data quality check.

    Keyword Arguments:
    parent_dag_name -- Parent DAG name defined in `main_dag.py` dag object
    child_dag_name -- Child DAG name used to define subdag ID
    start_date -- DAG start date
    end_date -- DAG end date
    schedule_interval -- (e.g. '@monthly', '@weekly', etc.)
    redshift_conn_id   -- Redshift connection ID (str)
    degree_list -- List of degree names (list)
    origin_table_format -- Dictionary of table labels and staging table names used for fact table sql mapping (str)
    destination_table_format -- Fact table name to be formatted with degree name (str)
    sql -- Fact table query (str)
    """

    dag = DAG(
        dag_id=f"{parent_dag_name}.{child_dag_name}",
        start_date=start_date,
        end_date=end_date,
        schedule_interval=schedule_interval,
        **kwargs
    )

    #help
    # upstream_subdag_id = kwargs['task_instance'].upstream_task_ids

    for degree in degree_list:
        destination_table = destination_table_format.format(degree=degree)
        origin_tables = {table:name.format(degree=degree) for (table,name) in origin_table_format.items()}

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