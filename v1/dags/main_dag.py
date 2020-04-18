from datetime import datetime, timedelta
import os
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.subdag_operator import SubDagOperator
from custom_operators import StageToRedshiftOperator
from sql_queries import SqlQueries
from stage_dag import stage_s3_to_redshift
from fact_dag import create_fact_tables

default_args = {
    'owner': 'udacity',
    #number of retry if task fails
    'retries': 3,
    #retry delay time interval
    'retry_delay': timedelta(minutes=5),
    'email_on_retry': False,
    'depends_on_past': False,
    'catchup': False
}

main_dag = DAG('udacity_analytics',
          default_args=default_args,
          description='Load monthly user and video analytics in Redshift with Airflow',
          start_date=datetime(year=2019, month=1, day=1),
          end_date=datetime(year=2019, month=12, day=1),
          schedule_interval='@monthly'
)

degree_list = [
    'data_science', 
    'data_analytics', 
    'data_engineering'
]

# TODO create dimensional task
start_operator = DummyOperator(task_id='Begin_execution',  dag=main_dag)

stage_mentor_activity_subdag = SubDagOperator(
    subdag=stage_s3_to_redshift(
        start_date=main_dag.start_date,
        end_date=main_dag.start_date,
        schedule_interval=main_dag.schedule_interval,
        parent_dag_name=main_dag.dag_id,
        redshift_conn_id='redshift',
        aws_credentials_conn_id='aws_credentials',
        s3_data='mentor_activity',
        sql=SqlQueries.create_mentor_activity,
        s3_bucket='udacity-de-capstone',
        s3_key='{degree}/{execution_year}/{execution_month}/{s3_data}',
        degree_list=degree_list,
        region='us-west-2',
        file_format='JSON'
    ),
    task_id='stage_mentor_activity',
    dag=main_dag
)

stage_video_log_subdag = SubDagOperator(
    subdag=stage_s3_to_redshift(
        start_date=main_dag.start_date,
        end_date=main_dag.start_date,
        schedule_interval=main_dag.schedule_interval,
        parent_dag_name=main_dag.dag_id,
        redshift_conn_id='redshift',
        aws_credentials_conn_id='aws_credentials',
        s3_data='video_log',
        sql=SqlQueries.create_video_log,
        s3_bucket='udacity-de-capstone',
        s3_key='{degree}/{execution_year}/{execution_month}/{s3_data}',
        degree_list=degree_list,
        region='us-west-2',
        file_format='JSON'
    ),
    task_id='stage_video_log',
    dag=main_dag
)

stage_project_feedback_subdag = SubDagOperator(
    subdag=stage_s3_to_redshift(
        start_date=main_dag.start_date,
        end_date=main_dag.start_date,
        schedule_interval=main_dag.schedule_interval,
        parent_dag_name=main_dag.dag_id,
        redshift_conn_id='redshift',
        aws_credentials_conn_id='aws_credentials',
        s3_data='project_feedback',
        sql=SqlQueries.create_projects_feedback,
        s3_bucket='udacity-de-capstone',
        s3_key='{degree}/{execution_year}/{execution_month}/{s3_data}',
        degree_list=degree_list,
        region='us-west-2',
        file_format='JSON'
    ),
    task_id='stage_project_feedback',
    dag=main_dag
)

stage_section_feedback_subdag = SubDagOperator(
    subdag=stage_s3_to_redshift(
        start_date=main_dag.start_date,
        end_date=main_dag.start_date,
        schedule_interval=main_dag.schedule_interval,
        parent_dag_name=main_dag.dag_id,
        redshift_conn_id='redshift',
        aws_credentials_conn_id='aws_credentials',
        s3_data='section_feedback',
        sql=SqlQueries.create_section_feedback,
        s3_bucket='udacity-de-capstone',
        s3_key='{degree}/{execution_year}/{execution_month}/{s3_data}',
        degree_list=degree_list,
        region='us-west-2',
        file_format='JSON'
    ),
    task_id='stage_section_feedback',
    dag=main_dag
)

load_users_dim = StageToRedshiftOperator(
    task_id='load_users_dim',
    redshift_conn_id='redshift',
    aws_credentials_id='aws_credentials',
    s3_bucket='udacity-de-capstone',
    s3_data='users_dim',
    s3_key='dimension_tables/{s3_data}',
    region='us-west-2',
    file_format='JSON',
    table='users_dim',
)

load_video_log_dim = StageToRedshiftOperator(
    task_id='load_video_log_dim',
    redshift_conn_id='redshift',
    aws_credentials_id='aws_credentials',
    s3_bucket='udacity-de-capstone',
    s3_data='video_log_dim',
    s3_key='dimension_tables/{s3_data}',
    region='us-west-2',
    file_format='JSON',
    table='video_dim',
)

load_project_dim = StageToRedshiftOperator(
    task_id='load_project_dim',
    redshift_conn_id='redshift',
    aws_credentials_id='aws_credentials',
    s3_bucket='udacity-de-capstone',
    s3_data='project_dim',
    s3_key='dimension_tables/{s3_data}',
    region='us-west-2',
    file_format='JSON',
    table='project_dim',
)

avg_project_rating_subdag = SubDagOperator(
    subdag=create_fact_tables(
        start_date=main_dag.start_date,
        end_date=main_dag.start_date,
        schedule_interval=main_dag.schedule_interval,
        parent_dag_name=main_dag.dag_id,
        subdag_name='avg_project_rating',
        redshift_conn_id='redshift',
        s3_data='project_feedback',
        origin_table='staging_{}_table',
        fact='avg_rating',
        sql=SqlQueries.project_ratings,
        degree_list=degree_list,
    ),
    task_id='avg_project_rating',
    dag=main_dag
)

avg_section_rating_subdag = SubDagOperator(
    subdag=create_fact_tables(
        start_date=main_dag.start_date,
        end_date=main_dag.start_date,
        schedule_interval=main_dag.schedule_interval,
        parent_dag_name=main_dag.dag_id,
        subdag_name='avg_section_rating',
        redshift_conn_id='redshift',
        s3_data='section_feedback',
        origin_table='staging_{}_table',
        fact='avg_rating',
        sql=SqlQueries.section_ratings,
        degree_list=degree_list,
    ),
    task_id='avg_section_rating',
    dag=main_dag
)

highest_mentor_activity_prompt_score_subdag = SubDagOperator(
    subdag=create_fact_tables(
        start_date=main_dag.start_date,
        end_date=main_dag.start_date,
        schedule_interval=main_dag.schedule_interval,
        parent_dag_name=main_dag.dag_id,
        subdag_name='highest_prompt_score',
        redshift_conn_id='redshift',
        s3_data='mentor_activity',
        origin_table='staging_{}_table',
        fact='highest_prompt_score',
        sql=SqlQueries.highest_mentor_activity_prompt_scores,
        degree_list=degree_list,
    ),
    task_id='highest_prompt_score',
    dag=main_dag
)

highest_mentor_activity_answer_score_subdag = SubDagOperator(
    subdag=create_fact_tables(
        start_date=main_dag.start_date,
        end_date=main_dag.start_date,
        schedule_interval=main_dag.schedule_interval,
        parent_dag_name=main_dag.dag_id,
        subdag_name='highest_answer_score',
        redshift_conn_id='redshift',
        s3_data='mentor_activity',
        origin_table='staging_{}_table',
        fact='highest_answer_score',
        sql=SqlQueries.highest_mentor_activity_prompt_scores,
        degree_list=degree_list,
    ),
    task_id='highest_answer_score',
    dag=main_dag
)

avg_video_views_per_user_subdag = SubDagOperator(
    subdag=create_fact_tables(
        start_date=main_dag.start_date,
        end_date=main_dag.start_date,
        schedule_interval=main_dag.schedule_interval,
        parent_dag_name=main_dag.dag_id,
        subdag_name='avg_video_views_per_user',
        redshift_conn_id='redshift',
        s3_data='video_log',
        origin_table='staging_{}_table',
        fact='avg_video_views_per_user',
        sql=SqlQueries.avg_video_views_per_user,
        degree_list=degree_list,
    ),
    task_id='avg_video_views_per_user',
    dag=main_dag
)

avg_video_view_date_range_subdag = SubDagOperator(
    subdag=create_fact_tables(
        start_date=main_dag.start_date,
        end_date=main_dag.start_date,
        schedule_interval=main_dag.schedule_interval,
        parent_dag_name=main_dag.dag_id,
        subdag_name='avg_video_view_range',
        redshift_conn_id='redshift',
        s3_data='video_log',
        origin_table='staging_{}_table',
        fact='avg_video_view_range',
        sql=SqlQueries.avg_video_view_date_range,
        degree_list=degree_list,
    ),
    task_id='avg_video_view_range',
    dag=main_dag
)

end_operator = DummyOperator(task_id='Stop_execution', dag=main_dag)


# staging tables
start_operator >> stage_mentor_activity_subdag
start_operator >> stage_video_log_subdag
start_operator >> stage_project_feedback_subdag
start_operator >> stage_section_feedback_subdag

# dimension tables
start_operator >> load_users_dim
start_operator >> load_project_dim
start_operator >> load_video_log_dim

# fact tables
stage_project_feedback_subdag >> avg_project_rating_subdag

stage_section_feedback_subdag >> avg_section_rating_subdag

stage_mentor_activity_subdag >> highest_mentor_activity_prompt_score_subdag
stage_mentor_activity_subdag >> highest_mentor_activity_answer_score_subdag

stage_video_log_subdag >> avg_video_view_date_range_subdag
stage_video_log_subdag >> avg_video_views_per_user_subdag


end_operator << [
    avg_project_rating_subdag, 
    avg_section_rating_subdag, 
    highest_mentor_activity_prompt_score_subdag, 
    highest_mentor_activity_answer_score_subdag,
    avg_video_view_date_range_subdag,
    avg_video_views_per_user_subdag
    ]