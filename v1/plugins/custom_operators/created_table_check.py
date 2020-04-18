from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class CreatedTableOperator(BaseOperator):

    @apply_defaults
    
    def __init__(self, redshift_conn_id='', table='', sql='', *args, **kwargs):
        """
        Iniitializes Redshift and dictionary with SQL create queries

        Keyword Arguments:
        redshift_conn_id -- Redshift connection ID configured in Airflow/admin/connection UI (str)
        table -- Table associated with sql create query (str)
        sql -- SQL create query (str)
        """
        super(CreatedTableOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.table = table
        self.sql = sql

    def execute(self, context):
        """Executes SQL create query for each table in dictionary"""
        redshift = PostgresHook(self.redshift_conn_id)
        
        self.log.info(f'CreatedTableOperator: {self.table} ... In Progress')
        redshift.run(self.sql)
        self.log.info(f'CreatedTableOperator: {self.table} ... Done')
