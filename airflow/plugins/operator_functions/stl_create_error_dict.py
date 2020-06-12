import pandas as pd
from sqlalchemy import create_engine
from airflow.hooks.postgres_hook import PostgresHook
from airflow.hooks.base_hook import BaseHook

def get_error_dict(redshift_conn_id):

    get_table_name = """
    SELECT DISTINCT
        perm.name,
        stl.tbl AS id
    FROM 
        stl_load_errors stl
    LEFT JOIN 
        STV_TBL_PERM perm
    ON 
        stl.tbl = perm.id
    WHERE 
        perm.name != 'None'
    AND
        stl.session = (SELECT session FROM stl_load_errors ORDER BY session DESC LIMIT 1)
    """   
    
    redshift = PostgresHook(redshift_conn_id)
    
    # gets names and table IDs of tables in stl_load_errors table in current redshift session
    table_df = redshift.get_pandas_df(get_table_name)

    print('table_df: ', table_df)
    # creates dictionary of table names and IDs to loop over
    stl_table_dict = dict(zip(table_df['name'].apply(lambda name: name.strip()).values, table_df['id'].values))
    
    print('Staging tables within stl_load_errors table: ', list(stl_table_dict.keys()))
    
    return stl_table_dict