from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadDimensionOperator(BaseOperator):

    ui_color = '#80BD9E'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id='',
                 table='',
                 sql='',
                 *args, **kwargs):
        """
        Initialize Redshift, dimension table, and SQL insert statement
        
        Keyword Arguments:
        redshift_conn_id -- Redshift connection ID configured in Airflow/admin/connection UI (str)
        table -- Dimension table name (str)
        sql -- SQL insert command to execute on dimension table (str)
        """
        super(LoadDimensionOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.table = table
        self.sql = sql

    def execute(self, context):
        """
        Loads data from staging table(s) to dimension table
        
        Keyword Arguments:
        delete_existing_data -- Deletes existing data from table if True (bool)
        """
        redshift = PostgresHook(self.redshift_conn_id)

        self.log.info(f'LoadDimensionOperator loading {self.table} table')
        redshift.run(self.sql)
        self.log.info(f'LoadDimensionOperator loaded {self.table} table')
        
        
