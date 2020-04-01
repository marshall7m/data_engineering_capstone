from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class DataQualityOperator(BaseOperator):

    ui_color = '#89DA59'
    """
    Iniitializes Redshift and table list

    Keyword Arguments:
    redshift_conn_id -- Redshift connection ID configured in Airflow/admin/connection UI (str)
    table_list -- List of SQL table names to be used for data quality check
    """
    @apply_defaults
    def __init__(self,
                 redshift_conn_id='',
                 table_list=[],
                 *args, **kwargs):

        super(DataQualityOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.table_list = table_list
    

    def execute(self, context):
        """For every table in table list, check the table count, raise a value error if the table count is zero or non-existent and log table count"""
        redshift = PostgresHook(self.redshift_conn_id)
        self.log.info('DataQualityOperator is checking:')
        for table in self.table_list:
            self.log.info(f'\t{table} table')
            records = redshift.get_records(f'SELECT COUNT(*) FROM {table}')
            if len(records) < 1 or len(records[0]) < 1 or records[0][0] < 1:
                raise ValueError(f"Data quality check failed. {table} returned no results")
            table_count = records[0][0]
            logging.info(f"{table} Count: {table_count}")
        