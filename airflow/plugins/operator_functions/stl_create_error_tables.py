import pandas as pd
from sqlalchemy import create_engine
from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.hooks.base_hook import BaseHook
from psycopg2 import InternalError

def create_stl_table(redshift_conn_id,
                     table,
                     error_table_name,
                     table_id):
    """
    Creates a Redshift table containing all stl error rows associated with the input staging table. All columns within the error table 
    will be converted to VARCHAR given that the errors may be linked to data type issues.

    Keyword Arguments:
    redshift_conn_id -- Redshift connection ID (str)
    table -- Staging table name (str)
    errror_table_name -- Name to be used to create the error table
    table_id -- The staging table's table_id defined in the stl_load_errors table
    """

    get_column_names = """
    SELECT 
        col_name 
    FROM
        (SELECT
            * 
        FROM
            pg_get_cols('{}')
        COLS(
            view_schema name, 
            view_name name, 
            col_name name, 
            col_type varchar,
            col_num int
            )
        )
    """
    
    create_error_table = """
        DROP TABLE IF EXISTS 
            {error_table_name};
        CREATE TABLE
            {error_table_name}
        (
            {cast}, 
            err_code INT,
            err_reason VARCHAR(72)
        );
        """

    insert_rows = """
        INSERT INTO 
            {error_table_name}
        SELECT 
            {split_part},
            err_code,
            err_reason
        FROM 
            stl_load_errors stl
        WHERE 
            stl.tbl = {id}
        """
            
    redshift = PostgresHook(redshift_conn_id)

    # load column names into pandas dataframe
    col_names_df = redshift.get_pandas_df(get_column_names.format(table))

    # put column names into list
    col_names_list = col_names_df['col_name'].values.tolist()

    cast_col = ""
    split_raw_line = ""
    # loop over table's column names
    for i,col in enumerate(col_names_list):
        # if last column don't include ',' at end of string
        if col == col_names_list[-1]:
            # adds CAST statement to cast_col string
            cast_col += "{} VARCHAR".format(col)
            # adds split_part function to split_raw_line string
            split_raw_line += "CAST(split_part(raw_line, ',', {}) AS VARCHAR(500))".format(i+1)
        else:
            cast_col += "{} VARCHAR, ".format(col)
            split_raw_line += "CAST(split_part(raw_line, ',', {}) AS VARCHAR(500)), ".format(i+1)

    format_dict = {
        'table': table, 
        'error_table_name': error_table_name,
        'cast': cast_col,
        'split_part':split_raw_line,
        'id': table_id
    }
    print(f'Creating error table: {error_table_name}')

    # creates an empty table with duplicate columns of looped table
    formatted_create_sql = create_error_table.format(**format_dict)
    redshift.run(formatted_create_sql)

    # inserts all stl_load_errors raw_line values as strings into apporiate columns within the empty table
    formatted_insert_sql = insert_rows.format(**format_dict)
    redshift.run(formatted_insert_sql)
    
    error_table_count = redshift.get_records(f'SELECT COUNT(*) FROM {error_table_name}')[0][0]
    table_count = redshift.get_records(f'SELECT COUNT(*) FROM {table}')[0][0]

    print(f'{table} COUNT: {table_count}')
    print(f'{error_table_name} COUNT: {error_table_count}')
    
    return error_table_name