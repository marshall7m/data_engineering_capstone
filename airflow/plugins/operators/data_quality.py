from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class DataQualityOperator(BaseOperator):

    ui_color = '#89DA59'
    """ Performs data quality checks on a given BigQuery table list.
    
    Keyword Arguments:
    redshift_conn_id -- Redshift connection ID (str)
    table -- Redshift table name to be used for the data quality check
    """
    @apply_defaults
    def __init__(self,
                 redshift_conn_id='',
                 table='',
                 *args, **kwargs):

        super(DataQualityOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.table = table
    

    def execute(self, context):
        """Checks the table count and skips downstream tasks if table count is zero"""
        redshift = PostgresHook(self.redshift_conn_id)
        self.log.info(f'DataQualityOperator is checking: {self.table}')
        records = redshift.get_records(f'SELECT COUNT(*) FROM {self.table}')
        table_count = records[0][0]
        self.log.info(f"{self.table} Count: {table_count}")
        if len(records) < 1 or len(records[0]) < 1 or records[0][0] < 1:
            try:
                self.log.info('Skipping downstream tasks')
                self.skip(context['dag_run'],
                        context['ti'].execution_date,
                        downstream_tasks)
            except Exception as e:
                print(e)
        else:
            pass
        


        