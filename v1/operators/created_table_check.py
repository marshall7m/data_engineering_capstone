from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class CreatedTableOperator(BaseOperator):

    @apply_defaults
    
    def __init__(self, create_table_dict, redshift_conn_id='', *args, **kwargs):
        """
        Iniitializes Redshift and dictionary with SQL create queries

        Keyword Arguments:
        redshift_conn_id -- Redshift connection ID configured in Airflow/admin/connection UI (str)
        create_table_dict -- Dictionary with SQL create queries. Refer to sql_queries.py (dict)
        """
        super(CreatedTableOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.create_table_dict = create_table_dict

    def execute(self, context):
        """Executes SQL create query for each table in dictionary"""
        redshift = PostgresHook(self.redshift_conn_id)
  
        self.log.info(f'CreatedTableOperator is working on:')
        
        for table,sql in self.create_table_dict.items():
            self.log.info(f'\t{table} table')
            redshift.run(sql)