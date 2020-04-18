from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class FactTableOperator(BaseOperator):

    ui_color = '#80BD9E'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id='',
                 origin_table='',
                 destination_table='',
                 sql='',
                 *args, **kwargs):
        """
        Initialize Redshift and executes SQL fact statement
        
        Keyword Arguments:
        redshift_conn_id -- Redshift connection ID configured in Airflow/admin/connection UI (str)
        table -- Dimension table name (str)
        sql -- SQL insert command to execute on dimension table (str)
        """
        super(FactTableOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.origin_table = origin_table
        self.destination_table = destination_table
        self.sql = sql

    def execute(self, context, delete_existing_data=False):
        """
        Loads data from staging table(s) to fact table
        
        Keyword Arguments:
        delete_existing_data -- Deletes existing data from table if True (bool)
        """
        redshift = PostgresHook(self.redshift_conn_id)

        self.log.info(f'FactTableOperator loading {self.destination_table} table')
        formatted_sql = self.sql.format(self.origin_table, self.destination_table)
        redshift.run(formatted_sql)
        self.log.info(f'FactTableOperator loaded {self.destination_table} table')
        
        
