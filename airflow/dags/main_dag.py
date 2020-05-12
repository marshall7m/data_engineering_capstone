from datetime import datetime, timedelta
import os
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.subdag_operator import SubDagOperator
from airflow.operators.python_operator import (PythonOperator, BranchPythonOperator)
from plugins import StageToRedshiftOperator, CreateSTLErrorTablesOperator
from sql_queries import SqlQueries
from stage_fact_dag import stage_fact_s3_to_redshift
from stage_dim_dag import stage_dim_s3_to_redshift
from fact_dag import create_fact_tables
from dim_branch import dim_branch
from load_stl_error_dict import get_error_tables

execution_dates = {
    'date_range_1': {
        'start_date': datetime(year=2019, month=1, day=1),
        'end_date': datetime(year=2019, month=2, day=1)
    },
    'date_range_2': {
        'start_date': datetime(year=2019, month=2, day=1),
        'end_date': datetime(year=2019, month=3, day=1)
    }
}

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
          start_date=execution_dates['date_range_1']['start_date'],
          end_date=execution_dates['date_range_1']['end_date'],
          schedule_interval='@monthly'
)

# degree_list = [
#     'data_science', 
#     'data_analytics', 
#     'data_engineering'
# ]

degree_list = [
    'data_science'
]

start_operator = DummyOperator(task_id='Begin_execution',  dag=main_dag)

dim_branch = BranchPythonOperator(
    task_id='dim_branch',
    python_callable=dim_branch,
    provide_context=True,
    dag=main_dag)

skip_dims = DummyOperator(task_id='skip_dimension_tables',  dag=main_dag)

stage_mentor_activity_subdag = SubDagOperator(
    subdag=stage_fact_s3_to_redshift(
        start_date=main_dag.start_date,
        end_date=main_dag.end_date,
        schedule_interval=main_dag.schedule_interval,
        parent_dag_name=main_dag.dag_id,
        redshift_conn_id='redshift',
        s3_data='mentor_activity',
        sql=SqlQueries.create_mentor_activity,
        s3_bucket='udacity-de-capstone',
        s3_key='{degree}/{execution_year}/{execution_month}/{s3_data}.{file_format}',
        iam_role='arn:aws:iam::501460770806:role/udacity_dwh_role',
        degree_list=degree_list,
        region='us-west-2',
        file_format='CSV'
    ),
    task_id='stage_mentor_activity',
    dag=main_dag
)

stage_video_log_subdag = SubDagOperator(
    subdag=stage_fact_s3_to_redshift(
        start_date=main_dag.start_date,
        end_date=main_dag.end_date,
        schedule_interval=main_dag.schedule_interval,
        parent_dag_name=main_dag.dag_id,
        redshift_conn_id='redshift',
        s3_data='video_log',
        sql=SqlQueries.create_video_log,
        s3_bucket='udacity-de-capstone',
        s3_key='{degree}/{execution_year}/{execution_month}/{s3_data}.{file_format}',
        iam_role='arn:aws:iam::501460770806:role/udacity_dwh_role',
        degree_list=degree_list,
        region='us-west-2',
        file_format='CSV',
    ),
    task_id='stage_video_log',
    dag=main_dag
)

stage_project_feedback_subdag = SubDagOperator(
    subdag=stage_fact_s3_to_redshift(
        start_date=main_dag.start_date,
        end_date=main_dag.end_date,
        schedule_interval=main_dag.schedule_interval,
        parent_dag_name=main_dag.dag_id,
        redshift_conn_id='redshift',
        s3_data='project_feedback',
        sql=SqlQueries.create_projects_feedback,
        s3_bucket='udacity-de-capstone',
        s3_key='{degree}/{execution_year}/{execution_month}/{s3_data}.{file_format}',
        iam_role='arn:aws:iam::501460770806:role/udacity_dwh_role',
        degree_list=degree_list,
        region='us-west-2',
        file_format='CSV',
    ),
    task_id='stage_project_feedback',
    dag=main_dag
)

stage_section_feedback_subdag = SubDagOperator(
    subdag=stage_fact_s3_to_redshift(
        start_date=main_dag.start_date,
        end_date=main_dag.end_date,
        schedule_interval=main_dag.schedule_interval,
        parent_dag_name=main_dag.dag_id,
        redshift_conn_id='redshift',
        s3_data='section_feedback',
        sql=SqlQueries.create_section_feedback,
        s3_bucket='udacity-de-capstone',
        s3_key='{degree}/{execution_year}/{execution_month}/{s3_data}.{file_format}',
        iam_role='arn:aws:iam::501460770806:role/udacity_dwh_role',
        degree_list=degree_list,
        region='us-west-2',
        file_format='CSV',
    ),
    task_id='stage_section_feedback',
    dag=main_dag
)

stage_users_dim_subdag = SubDagOperator(
    subdag=stage_dim_s3_to_redshift(
        start_date=execution_dates['date_range_2']['end_date'],
        schedule_interval=main_dag.schedule_interval,
        parent_dag_name=main_dag.dag_id,
        redshift_conn_id='redshift',
        s3_data='users_dim',
        table='users_dim',
        sql=SqlQueries.create_users_dim,
        s3_bucket='udacity-de-capstone',
        s3_key='dimension_tables/{s3_data}.{file_format}',
        iam_role='arn:aws:iam::501460770806:role/udacity_dwh_role',
        region='us-west-2',
        file_format='CSV',
    ),
    task_id='stage_users_dim',
    dag=main_dag
)

stage_project_dim_subdag = SubDagOperator(
    subdag=stage_dim_s3_to_redshift(
        start_date=execution_dates['date_range_2']['start_date'],
        end_date=execution_dates['date_range_2']['end_date'],
        schedule_interval=main_dag.schedule_interval,
        parent_dag_name=main_dag.dag_id,
        redshift_conn_id='redshift',
        s3_data='projects_dim',
        table='projects_dim',
        sql=SqlQueries.create_project_dim,
        s3_bucket='udacity-de-capstone',
        s3_key='dimension_tables/{s3_data}.{file_format}',
        iam_role='arn:aws:iam::501460770806:role/udacity_dwh_role',
        region='us-west-2',
        file_format='CSV',
    ),
    task_id='stage_projects_dim',
    dag=main_dag
)

stage_video_dim_subdag = SubDagOperator(
    subdag=stage_dim_s3_to_redshift(
        start_date=execution_dates['date_range_2']['start_date'],
        end_date=execution_dates['date_range_2']['end_date'],
        schedule_interval=main_dag.schedule_interval,
        parent_dag_name=main_dag.dag_id,
        redshift_conn_id='redshift',
        s3_data='videos_dim',
        table='videos_dim',
        sql=SqlQueries.create_video_dim,
        s3_bucket='udacity-de-capstone',
        s3_key='dimension_tables/{s3_data}.{file_format}',
        iam_role='arn:aws:iam::501460770806:role/udacity_dwh_role',
        region='us-west-2',
        file_format='CSV',
    ),
    task_id='stage_videos_dim',
    dag=main_dag
)

get_stl_error_tables = PythonOperator(
    task_id='get_stl_error_tables',
    python_callable=get_error_tables,
    op_kwargs={'redshift_conn_id': 'redshift'}
)

avg_project_rating_subdag = SubDagOperator(
    subdag=create_fact_tables(
        start_date=execution_dates['date_range_2']['start_date'],
        end_date=execution_dates['date_range_2']['end_date'],
        schedule_interval=main_dag.schedule_interval,
        parent_dag_name=main_dag.dag_id,
        subdag_name='avg_project_rating',
        redshift_conn_id='redshift',
        origin_table_format={'table_1': '{degree}_project_feedback'},
        destination_table_format='{degree}_avg_project_rating',
        sql=SqlQueries.project_ratings,
        degree_list=degree_list
    ),
    task_id='avg_project_rating',
    dag=main_dag
)

avg_section_rating_subdag = SubDagOperator(
    subdag=create_fact_tables(
        start_date=execution_dates['date_range_2']['start_date'],
        end_date=execution_dates['date_range_2']['end_date'],
        schedule_interval=main_dag.schedule_interval,
        parent_dag_name=main_dag.dag_id,
        subdag_name='avg_section_rating',
        redshift_conn_id='redshift',
        origin_table_format={'table_1': '{degree}_section_feedback'},
        destination_table_format='{degree}_avg_section_rating',
        sql=SqlQueries.section_ratings,
        degree_list=degree_list
    ),
    task_id='avg_section_rating',
    dag=main_dag
)

highest_mentor_activity_prompt_score_subdag = SubDagOperator(
    subdag=create_fact_tables(
        start_date=execution_dates['date_range_2']['start_date'],
        end_date=execution_dates['date_range_2']['end_date'],
        schedule_interval=main_dag.schedule_interval,
        parent_dag_name=main_dag.dag_id,
        subdag_name='highest_prompt_score',
        redshift_conn_id='redshift',
        origin_table_format = {'table_1': '{degree}_mentor_activity'},
        destination_table_format='{degree}_highest_prompt_score',
        sql=SqlQueries.highest_mentor_activity_prompt_scores,
        degree_list=degree_list
    ),
    task_id='highest_prompt_score',
    dag=main_dag
)

highest_mentor_activity_answer_score_subdag = SubDagOperator(
    subdag=create_fact_tables(
        start_date=execution_dates['date_range_2']['start_date'],
        end_date=execution_dates['date_range_2']['end_date'],
        schedule_interval=main_dag.schedule_interval,
        parent_dag_name=main_dag.dag_id,
        subdag_name='highest_answer_score',
        redshift_conn_id='redshift',
        origin_table_format={'table_1': '{degree}_mentor_activity'},
        destination_table_format='{degree}_highest_answer_score',
        sql=SqlQueries.highest_mentor_activity_prompt_scores,
        degree_list=degree_list
    ),
    task_id='highest_answer_score',
    dag=main_dag
)

avg_video_views_per_user_subdag = SubDagOperator(
    subdag=create_fact_tables(
        start_date=execution_dates['date_range_2']['start_date'],
        end_date=execution_dates['date_range_2']['end_date'],
        schedule_interval=main_dag.schedule_interval,
        parent_dag_name=main_dag.dag_id,
        subdag_name='avg_video_views_per_user',
        redshift_conn_id='redshift',
        origin_table_format={'table_1': '{degree}_video_log'},
        destination_table_format='{degree}_avg_video_views_per_user',
        sql=SqlQueries.avg_video_views_per_user,
        degree_list=degree_list
    ),
    task_id='avg_video_views_per_user',
    dag=main_dag
)

avg_video_view_date_range_subdag = SubDagOperator(
    subdag=create_fact_tables(
        start_date=execution_dates['date_range_2']['start_date'],
        end_date=execution_dates['date_range_2']['end_date'],
        schedule_interval=main_dag.schedule_interval,
        parent_dag_name=main_dag.dag_id,
        subdag_name='avg_video_view_range',
        redshift_conn_id='redshift',
        origin_table_format={'table_1': '{degree}_video_log'},
        destination_table_format='{degree}_avg_video_view_range',
        sql=SqlQueries.avg_video_view_date_range,
        degree_list=degree_list
    ),
    task_id='avg_video_view_range',
    dag=main_dag
)

create_error_tables = CreateSTLErrorTablesOperator(
    task_id='load_stl_error_tables',
    redshift_conn_id='redshift',
    dag=main_dag,
    provide_context=True
)

end_operator = DummyOperator(task_id='Stop_execution', dag=main_dag)

start_operator >> [dim_branch,
                   stage_mentor_activity_subdag,
                   stage_video_log_subdag,
                   stage_project_feedback_subdag,
                   stage_section_feedback_subdag]

#staging tables
dim_branch >> [stage_users_dim_subdag,
               stage_project_dim_subdag,
               stage_video_dim_subdag,
               skip_dims]


get_stl_error_tables << [stage_mentor_activity_subdag,
                         stage_video_log_subdag,
                         stage_project_feedback_subdag,
                         stage_section_feedback_subdag]

# fact tables

get_stl_error_tables >> [
    avg_project_rating_subdag, 
    avg_section_rating_subdag, 
    highest_mentor_activity_prompt_score_subdag, 
    highest_mentor_activity_answer_score_subdag,
    avg_video_view_date_range_subdag,
    avg_video_views_per_user_subdag,
    create_error_tables
    ]  >> end_operator
