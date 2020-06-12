from airflow import AirflowException

def upstream_staging_check(origin_tables, upstream_subdag_id, **kwargs):
    """
    Checks if staging tables dependencies were staged succesfully within staging subdag.
    
    Keyword Arguments:
    origin_tables -- Staging table names (list)
    upstream_subdag_id -- Staging tables subdag ID
    """
    # for every staging table in origin_tables list, check if table was loaded successfully
    for table in origin_tables:
        # pulls staging_success_check xcom value from upstream subdag
        upstream_staging_success = kwargs['task_instance'].xcom_pull(dag_id=upstream_subdag_id, key=f'staging_success_check_{table}')
        if upstream_staging_success == None:
            upstream_staging_success = kwargs['task_instance'].xcom_pull(key=f'staging_success_check_{table}')
        else:
            pass
        #if table wasn't loaded successfully fail task
        if upstream_staging_success == False:
            print(f'Fix {table} error table')
            raise AirflowException
        else:
            continue
    print('Cleared to run downstream tasks')
