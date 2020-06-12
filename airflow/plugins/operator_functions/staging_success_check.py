#use AirflowFailException when airflow 1.10.11 is available since raising AirflowFailException will override retries to 0 if task fails
# from airflow.exceptions import AirflowFailException
from airflow import AirflowException
from airflow.hooks.postgres_hook import PostgresHook

def staging_success_check(redshift_conn_id,
                          table,
                          error_table, 
                          overide_to_success=False, 
                          **context):

    if overide_to_success == True:
        print('Staging Check Overrided to Success')
    else:
        redshift = PostgresHook(redshift_conn_id)
        # checks if error table exists
        error_table_exists_sql = f"""
        SELECT
            MAX(
                CASE WHEN
                    table_name = '{error_table}' 
                THEN 
                    1 
                ELSE 
                    0 
                END
            ) AS table_exists from information_schema.tables
        """
        

        error_table_exists = redshift.get_records(error_table_exists_sql)
        #if error table doesn't exits then push task x_com to true for downstream check
        if error_table_exists[0][0] == 0 or error_table_exists[0] == 0:
            print('Staging Check Success')
            context['task_instance'].xcom_push(key=f'staging_success_check_{table}', value=True)
        else:
            #if error table does exists then push task x_com to true for downstream check
            print(f'{error_table} still exists. Under "Fix STL Error Table" section in `iac_notebook.ipynb`, fix stl errors in {error_table}, insert fixed rows into related staging table  \
                and delete {error_table} from Redshift data warehouse. Once those steps are completed, re-run this task.')
            context['task_instance'].xcom_push(key=f'staging_success_check_{table}', value=False)
            # raises exception to fail task
            raise AirflowException
            # raise AirflowFailException
