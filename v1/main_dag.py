from datetime import datetime, timedelta
import os
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from operators import (CreatedTableOperator, StageToRedshiftOperator, LoadFactOperator,
                       LoadDimensionOperator, DataQualityOperator)
from configparser import ConfigParser
from sql_queries import SqlQueries

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

dag = DAG('udacity_analytics',
          default_args=default_args,
          description='Load monthly user and video analytics in Redshift with Airflow',
          start_date=datetime(year=2019, month=1, day=1),
          end_date=datetime(year=2019, month=12, day=1),
          schedule_interval='@monthly'
)

start_operator = DummyOperator(task_id='Begin_execution',  dag=dag)

table_created_check = CreatedTableOperator(
    task_id='created_table_check',
    dag=dag,
    redshift_conn_id='redshift',
    create_table_dict = SqlQueries.create_table_dict
)

stage_users = StageToRedshiftOperator(
    task_id='stage_users',
    dag=dag,
    table='stage_users',
    redshift_conn_id='redshift',
    provide_context=True
)

stage_ds_mentor_activity = StageToRedshiftOperator(
    task_id='stage_ds_mentor_activity',
    dag=dag,
    table='stage_ds_mentor_activity',
    redshift_conn_id='redshift',
    aws_credentials_id='aws_credentials',
    s3_bucket='udacity-de-capstone',
    s3_key='data_science/{execution_date.year}/{execution_date.month}/mentor_activity.json',
    region='us-west-2',
    file_format='JSON',
    provide_context=True
)

stage_da_mentor_activity = StageToRedshiftOperator(
    task_id='stage_da_mentor_activity',
    dag=dag,
    table='stage_da_mentor_activity',
    redshift_conn_id='redshift',
    aws_credentials_id='aws_credentials',
    s3_bucket='udacity-de-capstone',
    s3_key='data_analytics/{execution_date.year}/{execution_date.month}/mentor_activity.json',
    region='us-west-2',
    file_format='JSON',
    provide_context=True
)

stage_de_mentor_activity = StageToRedshiftOperator(
    task_id='stage_de_mentor_activity',
    dag=dag,
    table='stage_de_mentor_activity',
    redshift_conn_id='redshift',
    aws_credentials_id='aws_credentials',
    s3_bucket='udacity-de-capstone',
    s3_key='data_engineering/{execution_date.year}/{execution_date.month}/mentor_activity.json',
    region='us-west-2',
    file_format='JSON',
    provide_context=True
)

stage_ds_project_feedback = StageToRedshiftOperator(
    task_id='stage_ds_project_feedback',
    dag=dag,
    table='stage_ds_project_feedback',
    redshift_conn_id='redshift',
    aws_credentials_id='aws_credentials',
    s3_bucket='udacity-de-capstone',
    s3_key='data_science/{execution_date.year}/{execution_date.month}/project_feedback.json',
    region='us-west-2',
    file_format='JSON',
    provide_context=True
)

stage_da_project_feedback = StageToRedshiftOperator(
    task_id='stage_da_project_feedback',
    dag=dag,
    table='stage_ds_project_feedback',
    redshift_conn_id='redshift',
    aws_credentials_id='aws_credentials',
    s3_bucket='udacity-de-capstone',
    s3_key='data_analytics/{execution_date.year}/{execution_date.month}/project_feedback.json',
    region='us-west-2',
    file_format='JSON',
    provide_context=True
)

stage_de_project_feedback = StageToRedshiftOperator(
    task_id='stage_de_project_feedback',
    dag=dag,
    table='stage_de_project_feedback',
    redshift_conn_id='redshift',
    aws_credentials_id='aws_credentials',
    s3_bucket='udacity-de-capstone',
    s3_key='data_engineering/{execution_date.year}/{execution_date.month}/project_feedback.json',
    region='us-west-2',
    file_format='JSON',
    provide_context=True
)

stage_ds_section_feedback = StageToRedshiftOperator(
    task_id='stage_ds_section_feedback',
    dag=dag,
    table='stage_ds_section_feedback',
    redshift_conn_id='redshift',
    aws_credentials_id='aws_credentials',
    s3_bucket='udacity-de-capstone',
    s3_key='data_science/{execution_date.year}/{execution_date.month}/section_feedback.json',
    region='us-west-2',
    file_format='JSON',
    provide_context=True
)

stage_da_section_feedback = StageToRedshiftOperator(
    task_id='stage_da_section_feedback',
    dag=dag,
    table='stage_da_section_feedback',
    redshift_conn_id='redshift',
    aws_credentials_id='aws_credentials',
    s3_bucket='udacity-de-capstone',
    s3_key='data_analytics/{execution_date.year}/{execution_date.month}/section_feedback.json',
    region='us-west-2',
    file_format='JSON',
    provide_context=True
)

stage_de_section_feedback = StageToRedshiftOperator(
    task_id='stage_de_section_feedback',
    dag=dag,
    table='stage_de_section_feedback',
    redshift_conn_id='redshift',
    aws_credentials_id='aws_credentials',
    s3_bucket='udacity-de-capstone',
    s3_key='data_engineering/{execution_date.year}/{execution_date.month}/section_feedback.json',
    region='us-west-2',
    file_format='JSON',
    provide_context=True
)

stage_ds_videos = StageToRedshiftOperator(
    task_id='stage_ds_video_log',
    dag=dag,
    table='stage_ds_video_log',
    redshift_conn_id='redshift',
    aws_credentials_id='aws_credentials',
    s3_bucket='udacity-de-capstone',
    s3_key='data_engineering/{execution_date.year}/{execution_date.month}/video_log.json',
    region='us-west-2',
    file_format='JSON',
    provide_context=True
)

stage_da_videos = StageToRedshiftOperator(
    task_id='stage_da_video_log',
    dag=dag,
    table='stage_da_video_log',
    redshift_conn_id='redshift',
    aws_credentials_id='aws_credentials',
    s3_bucket='udacity-de-capstone',
    s3_key='data_engineering/{execution_date.year}/{execution_date.month}/video_log.json',
    region='us-west-2',
    file_format='JSON',
    provide_context=True
)

stage_de_videos = StageToRedshiftOperator(
    task_id='stage_de_video_log',
    dag=dag,
    table='stage_de_video_log',
    redshift_conn_id='redshift',
    aws_credentials_id='aws_credentials',
    s3_bucket='udacity-de-capstone',
    s3_key='data_engineering/{execution_date.year}/{execution_date.month}/video_log.json',
    region='us-west-2',
    file_format='JSON',
    provide_context=True
)

load_video_lessons = LoadDimensionOperator(
    task_id='load_video_lessons',
    dag=dag,
    sql=SqlQueries.load_video_lessons,
    table='video_lessons',
    redshift_conn_id='redshift',
    provide_context=True
)

avg_ds_project_rating = LoadFactOperator(
    task_id='avg_ds_project_rating',
    dag=dag,
    origin_table='stage_ds_project_feedback',
    destination_table='avg_ds_project_rating',
    sql=SqlQueries.project_ratings,
    redshift_conn_id='redshift',
    provide_context=True
)

avg_da_project_rating = LoadFactOperator(
    task_id='avg_da_project_rating',
    dag=dag,
    origin_table='stage_da_project_feedback',
    destination_table='avg_da_project_rating',
    sql=SqlQueries.project_ratings,
    redshift_conn_id='redshift',
    provide_context=True
)

avg_de_project_rating = LoadFactOperator(
    task_id='avg_de_project_rating',
    dag=dag,
    origin_table='stage_de_project_feedback',
    destination_table='avg_de_project_rating',
    sql=SqlQueries.project_ratings,
    redshift_conn_id='redshift',
    provide_context=True
)

# run_quality_checks = DataQualityOperator(
#     task_id='run_data_quality_checks',
#     dag=dag,
#     redshift_conn_id='redshift',
#     provide_context=True
#)

end_operator = DummyOperator(task_id='Stop_execution', dag=dag)

start_operator >> table_created_check

table_created_check >> stage_users

table_created_check >> stage_ds_mentor_activity
table_created_check >> stage_ds_project_feedback
table_created_check >> stage_ds_section_feedback
table_created_check >> stage_ds_videos

table_created_check >> stage_da_mentor_activity
table_created_check >> stage_da_project_feedback
table_created_check >> stage_da_section_feedback
table_created_check >> stage_da_videos

table_created_check >> stage_de_mentor_activity
table_created_check >> stage_de_project_feedback
table_created_check >> stage_de_section_feedback
table_created_check >> stage_de_videos

stage_ds_project_feedback >> avg_ds_project_rating
stage_da_project_feedback >> avg_da_project_rating
stage_de_project_feedback >> avg_de_project_rating

stage_ds_videos >> load_video_lessons
stage_da_videos >> load_video_lessons
stage_de_videos >> load_video_lessons

# run_quality_checks >> end_operator



