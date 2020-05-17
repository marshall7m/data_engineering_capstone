from airflow.plugins_manager import AirflowPlugin

import operators_functions
import operator_queries
import subdag_operators
import operators

class CustomPlugin(AirflowPlugin):
    name = "udacity_plugin"
    operators = [
        operators.StageToRedshiftOperator,
        operators.LoadFactOperator,
        operators.LoadDimensionOperator,
        operators.DataQualityOperator,
        operators.CreatedTableOperator,
        operators.CreateSTLErrorTablesOperator,
        operators.FactBranchOperator,
    ]

    subdag_operators = [
        subdag_operators.create_fact_tables,
        subdag_operators.stage_dim_s3_to_redshift,
        subdag_operators.stage_fact_s3_to_redshift
    ]

    operator_queries = operator_queries.SqlQueries

    operators_functions = [
        operators_functions.dim_branch,
        operators_functions.get_error_tables
    ]