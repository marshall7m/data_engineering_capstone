from datetime import datetime, timedelta
import os
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from operators import (CreatedTableOperator, StageToRedshiftOperator, LoadFactOperator,
                       LoadDimensionOperator, DataQualityOperator)
from sql_queries import SqlQueries

default_args = {
    'owner': 'udacity',
    'start_date': datetime(2019, 1, 12),
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
          description='Load and transform data in Redshift with Airflow',
          #@hourly
          schedule_interval='0 * * * *',
          start_date=datetime.utcnow()
)

start_operator = DummyOperator(task_id='Begin_execution',  dag=dag)

table_created_check = CreatedTableOperator(
    task_id='created_table_check',
    dag=dag,
    redshift_conn_id='redshift',
    create_table_dict = SqlQueries.create_table_dict
    
)

stage_events_to_redshift = StageToRedshiftOperator(
    task_id='stage_event',
    dag=dag,
    redshift_conn_id='redshift',
    aws_credentials_id='aws_credentials',
    s3_bucket='udacity-dend',
    s3_key='log_data',
    region='us-west-2',
    file_format='JSON',
    table='staging_events',
    provide_context=True
)

stage_songs_to_redshift = StageToRedshiftOperator(
    task_id='stage_songs',
    dag=dag,
    redshift_conn_id='redshift',
    aws_credentials_id='aws_credentials',
    s3_bucket='udacity-dend',
    s3_key='song_data',
    region='us-west-2',
    file_format='JSON',
    table='staging_songs',
    provide_context=True
)

load_video_lessons_dimension_table = LoadDimensionOperator(
    task_id='Load_video_lessons_table',
    dag=dag,
    redshift_conn_id='redshift',
    table='video_lessons',
    sql=SqlQueries.insert_video_lessons
)

load_video_logs_dimension_table = LoadDimensionOperator(
    task_id='Load_video_logs_table',
    dag=dag,
    redshift_conn_id='redshift',
    table='video_logs',
    sql=SqlQueries.insert_video_logs
)

load_mentor_activity = LoadDimensionOperator(
    task_id='Load_mentor_activity_table',
    dag=dag,
    redshift_conn_id='redshift',
    table='mentor_activity',
    sql=SqlQueries.insert_mentor_activity
)

load_section_review = LoadDimensionOperator(
    task_id='Load_section_feedback_table',
    dag=dag,
    redshift_conn_id='redshift',
    table='section_reviews',
    sql=SqlQueries.insert_section_feedback
)

load_project = LoadDimensionOperator(
    task_id='Load_project_feedback_table',
    dag=dag,
    redshift_conn_id='redshift',
    table='project_reviews',
    sql=SqlQueries.insert_project_feedback
)

run_quality_checks = DataQualityOperator(
    task_id='Run_data_quality_checks',
    dag=dag,
    redshift_conn_id='redshift',
    table_list=['songplays', 'artists', 'songs', 'users', 'time']
)

load_sentiment_table = LoadFactOperator(
    task_id='Load_sentiment_table',
    dag=dag,
    redshift_conn_id='redshift',
    table='sentiment',
    sql=SqlQueries.songplays_table_insert
)


end_operator = DummyOperator(task_id='Stop_execution', dag=dag)

start_operator >> table_created_check



run_quality_checks >> end_operator



