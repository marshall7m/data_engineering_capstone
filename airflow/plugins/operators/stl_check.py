from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from operator_functions.stl_create_error_tables import create_stl_table
from operator_functions.stl_create_error_dict import get_error_dict


class STLCheckOperator(BaseOperator):
    @apply_defaults
    def __init__(self,
                redshift_conn_id,
                table,
                error_table,
                *args, **kwargs):

        super(STLCheckOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.table = table
        self.error_table = error_table

    def execute(self, context):

        """
        Creates a dictionary of tables within the stl_load_errors table and checks if the staging table is in the dictionary
        """
        stl_table_dict = get_error_dict(self.redshift_conn_id)

        stl_table_names = list(stl_table_dict.keys())
        self.log.info('Current tables within stl_load_errors: ', stl_table_names)
        
        #if table is in stl_load_errors, error table doesn't exist and start date doesn't equal execution date
        if self.table in stl_table_names:
            table_id = stl_table_dict[self.table]
            create_stl_table(self.redshift_conn_id, self.table, self.error_table, table_id)
        else:
            self.log.info(f'{self.table} was staged successfully - creating error table skipped')
