from airflow.operators.branch_operator import BaseBranchOperator
from airflow.utils.decorators import apply_defaults
from operator_functions.stl_create_error_tables import create_stl_table
from operator_functions.stl_create_error_dict import get_error_dict


class STLCheckOperator(BaseBranchOperator):
    @apply_defaults
    def __init__(self,
                redshift_conn_id,
                origin_tables,
                destination_table,
                *args, **kwargs):

        super(STLCheckOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.origin_tables = origin_tables
        self.destination_table = destination_table

    def choose_branch(self, context):
        stl_table_dict = get_error_dict(self.redshift_conn_id)

        stl_table_names = list(stl_table_dict.keys())
        self.log.info('Current tables within stl_load_errors: ', stl_table_names)

        error_table_processed = context['task_instance'].xcom_pull(key='error_table_bool_list')
        for table in self.origin_tables:
            table_id = stl_table_dict[table]
            #if table is in stl_load_errors, error table doesn't exist and start date doesn't equal execution date
            if table in stl_table_names and error_table_processed[table] == False and context['execution_date'] == context['dag'].start_date:
                create_stl_table(self.redshift_conn_id, table, table_id, drop_error_table=True)
                return 'staging_failed'
            elif table in stl_table_names and error_table_processed[table] == False and context['execution_date'] != context['dag'].start_date:
                create_stl_table(self.redshift_conn_id, table, table_id, drop_error_table=False)
                return 'staging_failed'
            else:
                return self.destination_table