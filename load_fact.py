from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadFactOperator(BaseOperator):
    ui_color = '#F98866'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id='',
                 table='',
                 sql='',
                 *args, **kwargs):
        """
        Initialize Redshift, fact table, and SQL insert statement
        
        Keyword Arguments:
        redshift_conn_id -- Redshift connection ID configured in Airflow/admin/connection UI (str)
        table -- Fact table name (str)
        sql -- SQL insert command to execute on fact table (str)
        """

        super(LoadFactOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.table = table
        self.sql = sql

    def execute(self, context):
        """Loads data from staging table(s) to fact table"""
        redshift = PostgresHook(self.redshift_conn_id)
        
        self.log.info(f'LoadFactOperator loading {self.table} table')
        redshift.run(self.sql)
        self.log.info(f'LoadFactOperator loaded {self.table} table')
