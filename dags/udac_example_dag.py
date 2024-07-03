from datetime import datetime, timedelta
import os
from airflow import DAG
from airflow.contrib.hooks.aws_hook import AwsHook # type: ignore
from airflow.operators.postgres_operator import PostgresOperator # type: ignore
from airflow.operators.dummy_operator import DummyOperator # type: ignore
from operators import (StageToRedshiftOperator, LoadFactOperator,
                                 LoadDimensionOperator, DataQualityOperator)

from helpers.sql_statements import SqlStatementQueries
from helpers.sql_queries import SqlQueries
# AWS_KEY = os.environ.get('AWS_KEY')
# AWS_SECRET = os.environ.get('AWS_SECRET')

default_args = {
    'owner': 'cuonglt',
    'start_date': datetime(2024, 7, 3),
    'depends_on_past': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'email_on_retry': False,
    'catchup' : False
}

sql_checks = [
    {
        'check_sql': "SELECT COUNT(*) FROM song_plays WHERE user_id IS NULL",
    },
    {
        'check_sql': "SELECT COUNT(*) FROM users WHERE first_name IS NULL",
    },
    {
        'check_sql': "SELECT COUNT(*) FROM songs WHERE title IS NULL",
    },
    {
        'check_sql': "SELECT COUNT(*) FROM artists WHERE name IS NULL",
    },
    {
        'check_sql': "SELECT COUNT(*) FROM artists WHERE name IS NULL"
    }
]

dag = DAG('udac_example_dag',
          default_args=default_args,
          description='Load and transform data in Redshift with Airflow',
          schedule_interval='@hourly'
        )


start_operator = DummyOperator(task_id='Begin_execution',  dag=dag)

stage_events_to_redshift = StageToRedshiftOperator(
    task_id='Stage_events',
    dag=dag,
    redshift_conn_id='redshift',
    aws_credentials_id='aws_credentials',
    table='staging_events',
    s3_bucket='udacity-dend',
    s3_key='log_data',
    region='us-west-2',
    json_option='log_json_path.json',
    sql_statement=SqlStatementQueries.CREATE_STAGING_EVENTS_TABLE_SQL
)

stage_songs_to_redshift = StageToRedshiftOperator(
    task_id='Stage_songs',
    dag=dag,
    redshift_conn_id='redshift',
    aws_credentials_id='aws_credentials',
    table='staging_songs',
    s3_bucket='udacity-dend',
    s3_key='song_data',
    region='us-west-2',
    json_option='',
    sql_statement=SqlStatementQueries.CREATE_STAGING_SONGS_TABLE_SQL
)

load_songplays_table = LoadFactOperator(
    task_id='Load_songplays_fact_table',
    dag=dag,
    redshift_conn_id='redshift',
    table='song_plays',
    sql_create_table=SqlStatementQueries.CREATE_STAGING_SONGPLAYS_TABLE_SQL,
    sql_statement=SqlQueries.songplay_table_insert
)

load_user_dimension_table = LoadDimensionOperator(
    task_id='Load_user_dim_table',
    dag=dag,
    redshift_conn_id='redshift',
    table='users',
    sql_create_table=SqlStatementQueries.USER_TABLE_CREATE,
    sql_statement=SqlQueries.user_table_insert,
    truncate=True
)

load_song_dimension_table = LoadDimensionOperator(
    task_id='Load_song_dim_table',
    dag=dag,
    redshift_conn_id='redshift',
    table='songs',
    sql_create_table=SqlStatementQueries.SONG_TABLE_CREATE,
    sql_statement=SqlQueries.song_table_insert,
    truncate=True
)

load_artist_dimension_table = LoadDimensionOperator(
    task_id='Load_artist_dim_table',
    dag=dag,
    redshift_conn_id='redshift',
    table='artists',
    sql_create_table=SqlStatementQueries.ARTIST_TABLE_CREATE,
    sql_statement=SqlQueries.artist_table_insert,
    truncate=True
)

load_time_dimension_table = LoadDimensionOperator(
    task_id='Load_time_dim_table',
    dag=dag,
    redshift_conn_id='redshift',
    table='times',
    sql_create_table=SqlStatementQueries.TIME_TABLE_CREATE,
    sql_statement=SqlQueries.time_table_insert,
    truncate=True
)

run_quality_checks = DataQualityOperator(
    task_id='Run_data_quality_checks',
    dag=dag,
    redshift_conn_id='redshift',
    list_checks=sql_checks
)

end_operator = DummyOperator(task_id='Stop_execution',  dag=dag)

start_operator \
>> [stage_events_to_redshift, stage_songs_to_redshift] \
>> load_songplays_table \
>> [ \
        load_user_dimension_table, \
        load_song_dimension_table, \
        load_artist_dimension_table,\
        load_time_dimension_table \
    ] \
>> run_quality_checks \
>> end_operator