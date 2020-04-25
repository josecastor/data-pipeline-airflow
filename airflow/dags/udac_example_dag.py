from datetime import datetime, timedelta
import os
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.postgres_operator import PostgresOperator
from operators import StageToRedshiftOperator, LoadFactOperator, LoadDimensionOperator, DataQualityOperator
from helpers import SqlQueries


default_args = {
    'owner': 'Castor',
    'start_date': datetime(2020, 4, 23),
    'depends_on_past': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
    'catchup': False,
    'email_on_retry': False
}

dag = DAG('etl_data_sparkify',
          default_args = default_args,
          schedule_interval = '@hourly',
          description = 'Extract, transform and load data in Redshift with Airflow'
        )

start_operator = DummyOperator(task_id='Begin_execution',  dag=dag)

drop_table = PostgresOperator(
    task_id='Drop_table',
    dag=dag,
    postgres_conn_id='redshift',
    sql=SqlQueries.table_drop
)

create_schema_table = PostgresOperator(
    task_id='Create_schema_table',
    dag=dag,
    postgres_conn_id='redshift',
    sql=SqlQueries.schema_create
)

# Create table staging events
create_staging_events_table = PostgresOperator(
    task_id='Create_staging_events_table',
    dag=dag,
    postgres_conn_id='redshift',
    sql=SqlQueries.staging_events_table_create
)

# Create table staging songs
create_staging_songs_table = PostgresOperator(
    task_id='Create_staging_songs_table',
    dag=dag,
    postgres_conn_id='redshift',
    sql=SqlQueries.staging_songs_table_create
)

# Create table songplays
create_songplays_table = PostgresOperator(
    task_id='Create_songplays_table',
    dag=dag,
    postgres_conn_id='redshift',
    sql=SqlQueries.songplay_table_create
)

# Create table users
create_users_table = PostgresOperator(
    task_id='Create_users_table',
    dag=dag,
    postgres_conn_id='redshift',
    sql=SqlQueries.user_table_create
)

# Create table songs
create_songs_table = PostgresOperator(
    task_id='Create_songs_table',
    dag=dag,
    postgres_conn_id='redshift',
    sql=SqlQueries.song_table_create
)

# Create table artists
create_artists_table = PostgresOperator(
    task_id='Create_artists_table',
    dag=dag,
    postgres_conn_id='redshift',
    sql=SqlQueries.artist_table_create
)

# Create table time
create_time_table = PostgresOperator(
    task_id='Create_time_table',
    dag=dag,
    postgres_conn_id='redshift',
    sql=SqlQueries.time_table_create
)

schema_created = DummyOperator(task_id='Schema_created', dag=dag)

# Load tables stage events
stage_events_to_redshift = StageToRedshiftOperator(
    task_id = 'Stage_events',
    dag = dag,
    aws_credentials_id = 'aws_credentials',
    redshift_conn_id = 'redshift',
    schema = 'sparkify',
    table = "staging_events",
    s3_bucket = 'udacity-dend',
    s3_key = 'log_data',
    s3_region = 'us-west-2',
    s3_format = 's3://udacity-dend/log_json_path.json'
)

# Load table stage songs
stage_songs_to_redshift = StageToRedshiftOperator(
    task_id = 'Stage_songs',
    dag = dag,
    aws_credentials_id = 'aws_credentials',
    redshift_conn_id = 'redshift',
    schema = 'sparkify',
    table = "staging_songs",
    s3_bucket = 'udacity-dend',
    s3_key = 'song_data',
    s3_region = 'us-west-2',
    s3_format = 'auto'
)

# Load table songplays
load_songplays_table = LoadFactOperator(
    task_id='Load_songplays_fact_table',
    dag=dag,
    aws_credentials_id = 'aws_credentials',
    redshift_conn_id = 'redshift',
    sql_schema = 'sparkify',
    sql = SqlQueries.songplay_table_insert
)

# Load table users
load_users_dimension_table = LoadDimensionOperator(
    task_id='Load_user_dim_table',
    dag=dag,
    aws_credentials_id = 'aws_credentials',
    redshift_conn_id = 'redshift',
    sql_schema = 'sparkify',
    sql = SqlQueries.user_table_insert
)

# Load table songs
load_songs_dimension_table = LoadDimensionOperator(
    task_id='Load_song_dim_table',
    dag=dag,
    aws_credentials_id = 'aws_credentials',
    redshift_conn_id = 'redshift',
    sql_schema = 'sparkify',
    sql = SqlQueries.song_table_insert
)

# Load table artists
load_artists_dimension_table = LoadDimensionOperator(
    task_id='Load_artist_dim_table',
    dag=dag,
    aws_credentials_id = 'aws_credentials',
    redshift_conn_id = 'redshift',
    sql_schema = 'sparkify',
    sql = SqlQueries.artist_table_insert
)

# Load table time
load_time_dimension_table = LoadDimensionOperator(
    task_id='Load_time_dim_table',
    dag=dag,
    aws_credentials_id = 'aws_credentials',
    redshift_conn_id = 'redshift',
    sql_schema = 'sparkify',
    sql = SqlQueries.time_table_insert
)

# Check load data staging tables
run_quality_checks_staging_tables = DataQualityOperator(
    task_id='Run_data_quality_checks_staging_tables',
    dag=dag,
    aws_credentials_id = 'aws_credentials',
    redshift_conn_id = 'redshift',
    sql_schema = 'sparkify',
    tables = ['staging_events','staging_songs'],
    stmts_checks = [
        {
            'sql': 'SELECT COUNT(*) FROM {}.{}',
            'op': 'gt',
            'val': 0
        }
    ] 
)

# Check load data facts and dimension tables
run_quality_checks_facts_dimension = DataQualityOperator(
    task_id='Run_data_quality_checks_facts_daminsion',
    dag=dag,
    aws_credentials_id = 'aws_credentials',
    redshift_conn_id = 'redshift',
    sql_schema = 'sparkify',    
    tables = ['songplays','users','songs','artists','time'],
    stmts_checks = [
        {
            'sql': 'SELECT COUNT(*) FROM {}.{}',
            'op': 'gt',
            'val': 0
        }
    ] 
)

end_operator = DummyOperator(task_id='Stop_execution',  dag=dag)

start_operator >> create_schema_table
create_schema_table >> drop_table

drop_table >> create_staging_songs_table
drop_table >> create_staging_events_table
drop_table >> create_songplays_table
drop_table >> create_users_table
drop_table >> create_songs_table
drop_table >> create_artists_table
drop_table >> create_time_table

create_staging_songs_table >> schema_created
create_staging_events_table >> schema_created
create_songplays_table >> schema_created
create_users_table >> schema_created
create_songs_table >> schema_created
create_artists_table >> schema_created
create_time_table >> schema_created

schema_created >> stage_events_to_redshift
schema_created >> stage_songs_to_redshift

stage_events_to_redshift >> run_quality_checks_staging_tables
stage_songs_to_redshift >> run_quality_checks_staging_tables

run_quality_checks_staging_tables >> load_songplays_table

load_songplays_table >> load_users_dimension_table
load_songplays_table >> load_songs_dimension_table
load_songplays_table >> load_artists_dimension_table
load_songplays_table >> load_time_dimension_table

load_users_dimension_table >> run_quality_checks_facts_dimension
load_songs_dimension_table >> run_quality_checks_facts_dimension
load_artists_dimension_table >> run_quality_checks_facts_dimension
load_time_dimension_table >> run_quality_checks_facts_dimension

run_quality_checks_facts_dimension >> end_operator
