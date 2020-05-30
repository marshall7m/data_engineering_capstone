import pandas as pd
from sqlalchemy import create_engine
from airflow.hooks.postgres_hook import PostgresHook
from airflow.hooks.base_hook import BaseHook

def get_error_dict(redshift_conn_id):
    get_table_name = """
    SELECT DISTINCT
        svv.name,
        stl.tbl AS id
    FROM 
        SVV_DISKUSAGE svv
    RIGHT JOIN stl_load_errors stl
        ON svv.tbl = stl.tbl
    WHERE svv.name != 'None'
    """   

    error_table_exist = """
    SELECT EXISTS (
        SELECT FROM pg_tables
        WHERE  
            schemaname = 'public'
        AND    
            tablename  = '{table}'
   )
   """
    
    redshift = PostgresHook(redshift_conn_id)
    connection = BaseHook.get_connection(redshift_conn_id)
    
    DWH_DB_USER = str(connection.login)
    DWH_DB_PASSWORD = str(connection.password)
    DWH_ENDPOINT = str(connection.host)
    DWH_PORT = str(connection.port)
    DWH_DB = str(connection.schema)

    conn_string = "postgresql://{}:{}@{}:{}/{}".format(DWH_DB_USER, DWH_DB_PASSWORD, DWH_ENDPOINT, DWH_PORT, DWH_DB)
    engine = create_engine(conn_string)
    
    # gets names and table IDs of tables in stl_load_errors table
    table_df = pd.read_sql_query(get_table_name, engine)
    # creates dictionary of table names and IDs to loop over
    stl_table_dict = dict(zip(table_df['name'].apply(lambda name: name.strip()).values, table_df['id'].values))
    
    print('Staging tables within stl_load_errors table: ', list(stl_table_dict.keys()))
    
    return stl_table_dict
