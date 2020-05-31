import pandas as pd
from sqlalchemy import create_engine
from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.hooks.base_hook import BaseHook
from psycopg2 import InternalError

def create_stl_table(redshift_conn_id,
                     table,
                     table_id,
                     context,
                     drop_error_table,
                     *args, 
                     **kwargs
                     ):

    get_column_names = """
    SELECT 
        P.column 
    FROM
        pg_table_def P
    WHERE 
        tablename = '{}'
    """
    create_error_table = """
        DROP TABLE IF EXISTS 
            {table}_errors;
        SELECT 
            {cast}
        INTO
            {table}_errors
        FROM 
            {table}
        WHERE
            1 = 0
        """

    insert_rows = """
        INSERT INTO 
            {table}_errors 
        SELECT 
            {split_part}
        FROM 
            stl_load_errors stl
        WHERE 
            stl.tbl = {id}
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

    with engine.connect().execution_options(autocommit=True) as con:
        # load column names into pandas dataframe
        col_names_df = pd.read_sql_query(get_column_names.format(table), engine)
        # put column names into list
        col_names_list = col_names_df['column'].values.tolist()

        cast_col = ""
        split_raw_line = ""
        # loop over table's column names
        for i,col in enumerate(col_names_list):
            if col == col_names_list[-1]:
                # adds CAST statement to cast_col string
                cast_col += "CAST({} AS VARCHAR) ".format(col)
                # adds split_part function to split_raw_line string
                split_raw_line += "split_part(raw_line, ',', {}) ".format(i+1)
            else:
                cast_col += "CAST({} AS VARCHAR), ".format(col)
                split_raw_line += "split_part(raw_line, ',', {}), ".format(i+1)

        format_dict = {
            'table': table, 
            'cast': cast_col,
            'split_part':split_raw_line,
            'id': table_id
        }
                
        print('Creating error table: {table}_errors'.format(**format_dict))

        # creates an empty table with duplicate columns of looped table
        if drop_error_table == True:
            formatted_create_sql = create_error_table.format(**format_dict)
            redshift.run(formatted_create_sql)
        else:
            pass

        # inserts all stl_load_errors raw_line values as strings into apporiate columns within the empty table
        formatted_insert_sql = insert_rows.format(**format_dict)
        try:
            redshift.run(formatted_insert_sql)
        except InternalError as e:
            print('Creating error table is already in progress')
        
        error_table_count = redshift.get_records('SELECT COUNT(*) FROM {table}_errors'.format(**format_dict))[0][0]
        print('{table}_errors COUNT: {}'.format(error_table_count, **format_dict))

        #get original table count and percentage of rows were errors

        #adds error table to boolean check dict, if boolean check dict doesn't exist create one with new error table
        error_table_processed = context['task_instance'].xcom_pull(key='error_table_bool_list')
        
        if error_table_processed == None:
            error_table_processed = {}
        else:
            pass
        
        error_table_processed[table] = True

        error_table_processed = context['task_instance'].xcom_push(key='error_table_bool_list', value=error_table_processed)