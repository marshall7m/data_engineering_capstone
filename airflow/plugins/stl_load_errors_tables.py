import pandas as pd
from sqlalchemy import create_engine
from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.hooks.base_hook import BaseHook

class CreateSTLErrorTablesOperator(BaseOperator):
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
            {table}_{month}_errors;
        SELECT 
            {cast}
        INTO
            {table}_{month}_errors
        FROM 
            {table}
        WHERE
            1 = 0
        """

    insert_rows = """
        INSERT INTO 
            {table}_{month}_errors 
        SELECT 
            {split_part}
        FROM 
            stl_load_errors stl
        WHERE 
            stl.tbl = {id}
        """
    
    def __init__(self, 
                 redshift_conn_id,
                 *args, **kwargs):
        
        super(CreateSTLErrorTablesOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        
    def execute(self, context):
       
        redshift = PostgresHook(self.redshift_conn_id)
        connection = BaseHook.get_connection(self.redshift_conn_id)
    
        DWH_DB_USER = str(connection.login)
        DWH_DB_PASSWORD = str(connection.password)
        DWH_ENDPOINT = str(connection.host)
        DWH_PORT = str(connection.port)
        DWH_DB = str(connection.schema)
        
        conn_string = "postgresql://{}:{}@{}:{}/{}".format(DWH_DB_USER, DWH_DB_PASSWORD, DWH_ENDPOINT, DWH_PORT, DWH_DB)
        engine = create_engine(conn_string)
        
        stl_table_dict = context['task_instance'].xcom_pull(task_ids='get_stl_error_tables')
        
        if stl_table_dict == {}:
            self.log.info('No errors in stl_load_errors table')
        else:
            with engine.connect().execution_options(autocommit=True) as con:
                for name,tbl_id in stl_table_dict.items():
                    # load column names into pandas dataframe
                    col_names_df = pd.read_sql_query(CreateSTLErrorTablesOperator.get_column_names.format(name), engine)
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
                        'table': name, 
                        'month': context['execution_date'].month, 
                        'cast': cast_col,
                        'split_part':split_raw_line,
                        'id': tbl_id
                    }
                    
                    self.log.info('Creating error table: {table}_{month}_errors'.format(**format_dict))
                    # creates an empty table with duplicate columns of looped table
                    formatted_create_sql = CreateSTLErrorTablesOperator.create_error_table.format(**format_dict)
                    # inserts all stl_load_errors raw_line values as strings into apporiate columns within the empty table
                    formatted_insert_sql = CreateSTLErrorTablesOperator.insert_rows.format(**format_dict)

                    redshift.run(formatted_create_sql)
                    redshift.run(formatted_insert_sql)
                    
                    error_table_count = redshift.get_records('SELECT COUNT(*) FROM {table}_{month}_errors'.format(**format_dict))[0][0]
                    self.log.info('{table}_{month}_errors COUNT: {}'.format(error_table_count, **format_dict))