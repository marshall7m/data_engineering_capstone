from datetime import datetime, timedelta
import os
import configparser

from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.subdag_operator import SubDagOperator
from airflow.operators.python_operator import (PythonOperator, BranchPythonOperator)

from operators.stage_redshift import StageToRedshiftOperator
from operator_queries.sql_queries import SqlQueries
from subdag_operators.stage_fact_dag import stage_fact_s3_to_redshift 
from subdag_operators.stage_dim_dag import stage_dim_s3_to_redshift
from subdag_operators.fact_dag import create_fact_tables
from operator_functions.dim_branch import dim_branch

config = configparser.ConfigParser()
config.read_file(open('/config/aws.cfg'))

default_args = {
    'owner': config.get("DWH","DWH_DB_USER"),
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
    'email_on_retry': False,
    'depends_on_past': False,
    'catchup': False
}

    # 'depends_on_past': True,


main_dag = DAG(
        dag_id='online_learning_analytics',
        description='Load monthly user and video analytics in Redshift with Airflow',
        start_date=datetime(year=2019, month=1, day=1),
        end_date=datetime(year=2019, month=2, day=1),
        schedule_interval='@monthly',
        max_active_runs=1,
        default_args=default_args)

start_operator = DummyOperator(task_id='begin_execution',  dag=main_dag)

# Dimension Table Operators
dim_branch = BranchPythonOperator(
        task_id='dim_branch',
        python_callable=dim_branch,
        provide_context=True,
        dag=main_dag)

stage_users_dim_subdag = SubDagOperator(
    subdag=stage_dim_s3_to_redshift(
        parent_dag_name=main_dag.dag_id,
        child_dag_name='stage_users_dim',
        start_date=main_dag.start_date,
        end_date=main_dag.end_date,
        schedule_interval=main_dag.schedule_interval,
        redshift_conn_id=config.get('DWH', 'DWH_CONN_ID'),
        s3_data='users_dim',
        staging_table='users_dim',
        create_sql=SqlQueries.create_users_dim,
        bucket=config.get('AWS','BUCKET'),
        s3_key='s3://{bucket}/dimension_tables/{s3_data}.{file_format}',
        iam_role=config.get('DWH', 'DWH_ROLE_ARN'),
        region=config.get('AWS', 'REGION'),
        file_format='CSV'),
    task_id='stage_users_dim',
    dag=main_dag)

stage_project_dim_subdag = SubDagOperator(
    subdag=stage_dim_s3_to_redshift(
        parent_dag_name=main_dag.dag_id,
        child_dag_name='stage_projects_dim',
        start_date=main_dag.start_date,
        end_date=main_dag.end_date,
        schedule_interval=main_dag.schedule_interval,
        redshift_conn_id=config.get('DWH', 'DWH_CONN_ID'),
        s3_data='projects_dim',
        staging_table='projects_dim',
        create_sql=SqlQueries.create_project_dim,
        bucket=config.get('AWS','BUCKET'),
        s3_key='s3://{bucket}/dimension_tables/{s3_data}.{file_format}',
        iam_role=config.get('DWH', 'DWH_ROLE_ARN'),
        region=config.get('AWS', 'REGION'),
        file_format='CSV'),
    task_id='stage_projects_dim',
    dag=main_dag)

stage_video_dim_subdag = SubDagOperator(
    subdag=stage_dim_s3_to_redshift(
        parent_dag_name=main_dag.dag_id,
        child_dag_name='stage_videos_dim',
        start_date=main_dag.start_date,
        end_date=main_dag.end_date,
        schedule_interval=main_dag.schedule_interval,
        redshift_conn_id=config.get('DWH', 'DWH_CONN_ID'),
        s3_data='videos_dim',
        staging_table='videos_dim',
        create_sql=SqlQueries.create_video_dim,
        bucket=config.get('AWS','BUCKET'),
        s3_key='s3://{bucket}/dimension_tables/{s3_data}.{file_format}',
        iam_role=config.get('DWH', 'DWH_ROLE_ARN'),
        region=config.get('AWS', 'REGION'),
        file_format='CSV'),
    task_id='stage_videos_dim',
    dag=main_dag)

skip_dims = DummyOperator(task_id='skip_dimension_tables',  dag=main_dag)

# degree_list = [
#     'data_science',
#     'data_engineering',
#     'data_analytics'
# ]

degree_list = ['data_science']

for degree in degree_list:

    # Staging fact table related data
    stage_mentor_activity_subdag = SubDagOperator(
        subdag=stage_fact_s3_to_redshift(    
            parent_dag_name=main_dag.dag_id,
            child_dag_name='stage_mentor_activity',
            start_date=main_dag.start_date,
            end_date=main_dag.end_date,
            schedule_interval=main_dag.schedule_interval,
            redshift_conn_id=config.get('DWH', 'DWH_CONN_ID'),
            staging_table=f'{degree}_mentor_activity',
            error_table=f'{degree}_mentor_activity_errors',
            degree=degree,
            bucket=config.get('AWS','BUCKET'),
            iam_role=config.get('DWH', 'DWH_ROLE_ARN'),
            region=config.get('AWS', 'REGION'),
            s3_data='mentor_activity',
            file_format='CSV',
            s3_key='s3://{bucket}/{degree}/{execution_year}/{execution_month}/{s3_data}.{file_format}',
            create_sql=SqlQueries.create_mentor_activity),
        task_id='stage_mentor_activity',
        dag=main_dag)

    stage_video_log_subdag = SubDagOperator(
        subdag=stage_fact_s3_to_redshift(  
            parent_dag_name=main_dag.dag_id,
            child_dag_name='stage_video_log',
            start_date=main_dag.start_date,
            end_date=main_dag.end_date,
            schedule_interval=main_dag.schedule_interval,
            redshift_conn_id=config.get('DWH', 'DWH_CONN_ID'),
            staging_table=f'{degree}_video_log',
            error_table=f'{degree}_video_log_errors',
            degree=degree,
            s3_data='video_log',
            bucket=config.get('AWS','BUCKET'),
            iam_role=config.get('DWH', 'DWH_ROLE_ARN'),
            region=config.get('AWS', 'REGION'),
            file_format='CSV',
            s3_key='s3://{bucket}/{degree}/{execution_year}/{execution_month}/{s3_data}.{file_format}',
            create_sql=SqlQueries.create_video_log),
        task_id='stage_video_log',
        dag=main_dag)

    stage_project_feedback_subdag = SubDagOperator(
        subdag=stage_fact_s3_to_redshift( 
            parent_dag_name=main_dag.dag_id,
            child_dag_name='stage_project_feedback',
            start_date=main_dag.start_date,
            end_date=main_dag.end_date,
            schedule_interval=main_dag.schedule_interval,
            redshift_conn_id=config.get('DWH', 'DWH_CONN_ID'),
            staging_table=f'{degree}_project_feedback',
            error_table=f'{degree}_project_feedback_errors',
            degree=degree,
            s3_data='project_feedback',
            create_sql=SqlQueries.create_projects_feedback,
            bucket=config.get('AWS','BUCKET'),
            s3_key='s3://{bucket}/{degree}/{execution_year}/{execution_month}/{s3_data}.{file_format}',
            iam_role=config.get('DWH', 'DWH_ROLE_ARN'),
            region=config.get('AWS', 'REGION'),
            file_format='CSV'),
        task_id='stage_project_feedback',
        dag=main_dag)

    stage_section_feedback_subdag = SubDagOperator(
        subdag=stage_fact_s3_to_redshift(
            parent_dag_name=main_dag.dag_id,
            child_dag_name='stage_section_feedback',
            start_date=main_dag.start_date,
            end_date=main_dag.end_date,
            schedule_interval=main_dag.schedule_interval,
            redshift_conn_id=config.get('DWH', 'DWH_CONN_ID'),
            staging_table=f'{degree}_section_feedback',
            error_table=f'{degree}_section_feedback_errors',
            degree=degree,
            s3_data='section_feedback',
            create_sql=SqlQueries.create_section_feedback,
            bucket=config.get('AWS','BUCKET'),
            s3_key='s3://{bucket}/{degree}/{execution_year}/{execution_month}/{s3_data}.{file_format}',
            iam_role=config.get('DWH', 'DWH_ROLE_ARN'),
            region=config.get('AWS', 'REGION'),
            file_format='CSV'),
        task_id='stage_section_feedback',
        dag=main_dag)

    # Fact table operators

    avg_project_rating_subdag = SubDagOperator(
        subdag=create_fact_tables(
            parent_dag_name=main_dag.dag_id,
            child_dag_name='avg_project_rating',
            start_date=main_dag.start_date,
            end_date=main_dag.end_date,
            schedule_interval=main_dag.schedule_interval,
            redshift_conn_id=config.get('DWH', 'DWH_CONN_ID'),
            degree=degree,
            origin_tables={'table_1': f'{degree}_project_feedback'},
            destination_table=f'{degree}_avg_project_rating',
            sql=SqlQueries.project_ratings,
            upstream_subdag_id=main_dag.dag_id + '.' + 'stage_video_log'),
        task_id='avg_project_rating',
        trigger_rule='all_done',
        dag=main_dag)
    
    avg_section_rating_subdag = SubDagOperator(
        subdag=create_fact_tables(
            parent_dag_name=main_dag.dag_id,
            child_dag_name='avg_section_rating',
            start_date=main_dag.start_date,
            end_date=main_dag.end_date,
            schedule_interval=main_dag.schedule_interval,
            redshift_conn_id=config.get('DWH', 'DWH_CONN_ID'),
            degree=degree,
            origin_tables={'table_1': f'{degree}_section_feedback'},
            destination_table=f'{degree}_avg_section_rating',
            sql=SqlQueries.section_ratings,
            upstream_subdag_id=main_dag.dag_id + '.' + 'stage_section_feedback'),
        task_id='avg_section_rating',
        trigger_rule='all_done',
        dag=main_dag)

    highest_mentor_activity_prompt_score_subdag = SubDagOperator(
        subdag=create_fact_tables( 
            parent_dag_name=main_dag.dag_id,
            child_dag_name='highest_prompt_score',
            start_date=main_dag.start_date,
            end_date=main_dag.end_date,
            schedule_interval=main_dag.schedule_interval,
            redshift_conn_id=config.get('DWH', 'DWH_CONN_ID'),
            degree=degree,
            origin_tables={'table_1': f'{degree}_mentor_activity'},
            destination_table=f'{degree}_highest_prompt_score',
            sql=SqlQueries.highest_mentor_activity_prompt_scores,
            upstream_subdag_id=main_dag.dag_id + '.' + 'stage_mentor_activity'),
        task_id='highest_prompt_score',
        trigger_rule='all_done',
        dag=main_dag)

    highest_mentor_activity_answer_score_subdag = SubDagOperator(
        subdag=create_fact_tables(
            parent_dag_name=main_dag.dag_id,
            child_dag_name='highest_answer_score',
            start_date=main_dag.start_date,
            end_date=main_dag.end_date,
            schedule_interval=main_dag.schedule_interval,
            redshift_conn_id=config.get('DWH', 'DWH_CONN_ID'),
            degree=degree,
            origin_tables={'table_1': f'{degree}_mentor_activity'},
            destination_table=f'{degree}_highest_answer_score',
            sql=SqlQueries.highest_mentor_activity_answer_scores,
            upstream_subdag_id=main_dag.dag_id + '.' + 'stage_mentor_activity'),
        task_id='highest_answer_score',
        trigger_rule='all_done',
        dag=main_dag)

    avg_video_views_per_user_subdag = SubDagOperator(
        subdag=create_fact_tables(
            parent_dag_name=main_dag.dag_id,
            child_dag_name='avg_video_views_per_user',
            start_date=main_dag.start_date,
            end_date=main_dag.end_date,
            schedule_interval=main_dag.schedule_interval,
            redshift_conn_id=config.get('DWH', 'DWH_CONN_ID'),
            degree=degree,
            origin_tables={'table_1': f'{degree}_video_log'},
            destination_table=f'{degree}_avg_video_views_per_user',
            sql=SqlQueries.avg_video_views_per_user,
            upstream_subdag_id=main_dag.dag_id + '.' + 'stage_video_log'),
        task_id='avg_video_views_per_user',
        trigger_rule='all_done',
        dag=main_dag)

    avg_video_view_date_range_subdag = SubDagOperator(
        subdag=create_fact_tables(
            parent_dag_name=main_dag.dag_id,
            child_dag_name='avg_video_view_range',
            start_date=main_dag.start_date,
            end_date=main_dag.end_date,
            schedule_interval=main_dag.schedule_interval,
            redshift_conn_id=config.get('DWH', 'DWH_CONN_ID'),
            degree=degree,
            origin_tables={'table_1': f'{degree}_video_log'},
            destination_table=f'{degree}_avg_video_view_range',
            sql=SqlQueries.avg_video_view_date_range,
            upstream_subdag_id=main_dag.dag_id + '.' + 'stage_video_log'),
        task_id='avg_video_view_range',
        trigger_rule='all_done',
        dag=main_dag)

    start_operator >> [dim_branch,
                    stage_mentor_activity_subdag,
                    stage_video_log_subdag,
                    stage_project_feedback_subdag,
                    stage_section_feedback_subdag]

    # staging tables
    dim_branch >> [stage_users_dim_subdag,
                stage_project_dim_subdag,
                stage_video_dim_subdag,
                skip_dims]

    # fact tables
    stage_mentor_activity_subdag >> [highest_mentor_activity_prompt_score_subdag, highest_mentor_activity_answer_score_subdag]
    stage_video_log_subdag >> [avg_video_view_date_range_subdag, avg_video_views_per_user_subdag]
    stage_project_feedback_subdag >> avg_project_rating_subdag
    stage_section_feedback_subdag >> avg_section_rating_subdag


