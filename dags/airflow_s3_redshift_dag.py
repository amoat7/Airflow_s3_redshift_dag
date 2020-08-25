from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators import (StageToRedshiftOperator, LoadFactOperator,
                                LoadDimensionOperator, DataQualityOperator)
from helpers import SqlQueries



default_args = {
    'owner': 'David',
    'depends_on_past':False,
    'catchup':False,
    'retry_delay': timedelta(minutes=5), 
    'start_date': datetime(2019, 1, 12),
    'retries':3,
    'email_on_retry':False
}

dag = DAG('airflow_s3_redshift_dag',
          default_args=default_args,
          description='Load and transform data in Redshift with Airflow',
          schedule_interval=None
        )

start_operator = DummyOperator(task_id='Begin_execution',  dag=dag)

stage_events_to_redshift = StageToRedshiftOperator(
    task_id='Stage_events',
    dag=dag,
    table = "public.staging_events",
    redshift_id = "redshift",
    aws_credentials = "aws_credentials",
    s3_bucket = "udacity-dend",
    region = "us-west-2",
    json = "s3://udacity-dend/log_json_path.json",
    s3_key = "log_data",
    ignore_headers =1
)


stage_songs_to_redshift = StageToRedshiftOperator(
    task_id='Stage_songs',
    dag=dag,
    table = "public.staging_songs",
    redshift_id = "redshift",
    aws_credentials = "aws_credentials",
    s3_bucket = "udacity-dend",
    region = "us-west-2",
    json = "auto",
    s3_key = "song_data",
    ignore_headers =1
)


load_songplays_table = LoadFactOperator(
    task_id='Load_songplays_fact_table',
    dag=dag,
    redshift_id ="redshift",
    table ="public.songplays",
    truncate = True,
    query = SqlQueries.songplay_table_insert
)

load_user_dimension_table = LoadDimensionOperator(
    task_id='Load_user_dim_table',
    dag=dag,
    redshift_id ="redshift",
    table ="public.users",
    truncate = True,
    query = SqlQueries.user_table_insert
)

load_song_dimension_table = LoadDimensionOperator(
    task_id='Load_song_dim_table',
    dag=dag,
    redshift_id ="redshift",
    table ="public.songs",
    truncate = True,
    query = SqlQueries.song_table_insert
)

load_artist_dimension_table = LoadDimensionOperator(
    task_id='Load_artist_dim_table',
    dag=dag,
    redshift_id ="redshift",
    table ="public.artists",
    truncate = True,
    query = SqlQueries.artist_table_insert
)

load_time_dimension_table = LoadDimensionOperator(
    task_id='Load_time_dim_table',
    dag=dag,
    redshift_id ="redshift",
    table ="public.time",
    truncate = True,
    query = SqlQueries.time_table_insert
)



checks = [{"sql_check": "SELECT COUNT(*) FROM songs WHERE songid IS NULL", "expected":0},
          {"sql_check": "SELECT COUNT(*) FROM artists WHERE artistid IS NULL", "expected":0}]
run_quality_checks = DataQualityOperator(
    task_id='Run_data_quality_checks',
    dag=dag,
    redshift_id = "redshift",
    checks = checks
)

end_operator = DummyOperator(task_id='Stop_execution',  dag=dag)


start_operator >> (stage_events_to_redshift, stage_songs_to_redshift) >> \
load_songplays_table >> (load_song_dimension_table, 
                         load_user_dimension_table, 
                         load_artist_dimension_table, 
                         load_time_dimension_table) >> \
run_quality_checks >> end_operator
