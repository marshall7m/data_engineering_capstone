from airflow.operators.branch_operator import BaseBranchOperator
from airflow.utils.decorators import apply_defaults

class FactBranchOperator(BaseBranchOperator):
    def __init__(self, 
                 origin_tables, 
                 destination_table,
                 *args, **kwargs):

        super(FactBranchOperator, self).__init__(*args, **kwargs)
        self.origin_tables = origin_tables
        self.destination_table = destination_table

    def choose_branch(self, **context):
        stl_table_dict = context['task_instance'].xcom_pull(task_ids='get_stl_error_tables')
        for _,name in self.origin_tables.items():
            if name in list(stl_table_dict.keys()):
                return 'skipped'
            else:
                continue
        return self.destination_table