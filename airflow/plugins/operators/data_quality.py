from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class DataQualityOperator(BaseOperator):

    ui_color = '#89DA59'
    """ Performs data quality checks on a given BigQuery table list.
    
    Keyword Arguments:
    bigquery_conn_id -- BigQuery connection ID configured in Airflow/admin/connection UI (str)
    table_list -- List of SQL table names to be used for data quality check
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
        """For every table in the table list, it checks the table count, raises a value error if the table count is zero or non-existent and logs the table count"""
        redshift = PostgresHook(self.redshift_conn_id)
        self.log.info(f'DataQualityOperator is checking: {self.table}')
        records = redshift.get_records(f'SELECT COUNT(*) FROM {self.table}')
        if len(records) < 1 or len(records[0]) < 1 or records[0][0] < 1:
            raise ValueError(f"Data quality check failed. {self.table} returned no results")
        table_count = records[0][0]
        self.log.info(f"{self.table} Count: {table_count}")


        