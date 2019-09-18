import datetime
from decimal import Decimal
from airflow import DAG
from airflow.contrib.hooks.aws_hook import AwsHook
from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import Variable
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators import (StageToRedshiftOperator, LoadFactOperator,
                                LoadDimensionOperator, DataQualityOperator,
                               InitRedshiftOperator)
from helpers import SqlQueries


########################################################
# Get required configuration info and create connections
AWS_HOOK = AwsHook("aws_credentials")
AWS_CREDENTIALS = AWS_HOOK.get_credentials()
REDSHIFT = PostgresHook(postgres_conn_id="redshift")
DAG_CONFIG = Variable.get("udac_dend_config", deserialize_json=True)


################
# Create our DAG
default_args = {
    'owner': 'udacity',
    'start_date': datetime.datetime(2019, 1, 12),
    'email_on_retry': False,
    'retries': 3,
    'retry_delay': datetime.timedelta(minutes=5),
    'depends_on_past': False
}
dag = DAG('udac_dend_dag',
          default_args=default_args,
          description='Load and transform data in Redshift with Airflow',
          schedule_interval='0 * * * *',
          catchup=False
        )

#######################################################
# Dummy operators defining specific sections of the DAG
start_operator = DummyOperator(task_id='Begin_execution',  dag=dag)
data_quality_operator = DummyOperator(task_id='Start_data_quality_checks',  dag=dag)
end_operator = DummyOperator(task_id='Stop_execution',  dag=dag)

##########################
# Init our database schema
create_redshift_tables = InitRedshiftOperator(
    task_id='Init_redshift',
    dag=dag,
    redshift=REDSHIFT,
    aws_credentials=AWS_CREDENTIALS
)

###########################################################################################
# Stage data to staging tables.
# In order to test code only a subset of data files are currently defined using the s3_path
stage_songs_to_redshift = StageToRedshiftOperator(
    task_id='Stage_songs',
    dag=dag,
    redshift=REDSHIFT,
    aws_credentials=AWS_CREDENTIALS,
    table="staging_songs",
    s3_bucket="udacity-dend",
    s3_path="/song_data/A/A/"
    # s3_path="/song_data/"
)
stage_events_to_redshift = StageToRedshiftOperator(
    task_id='Stage_events',
    dag=dag,
    redshift=REDSHIFT,
    aws_credentials=AWS_CREDENTIALS,
    table="staging_events",
    s3_bucket="udacity-dend",
    s3_path="/log_data/2018/11/",
    # s3_path="/log_data/",
    json_path="s3://udacity-dend/log_json_path.json"
)

#################################################################
# Load songplays fact table using the sql from helpers SqlQueries
load_songplays_table = LoadFactOperator(
    task_id='Load_songplays_fact_table',
    dag=dag,
    redshift=REDSHIFT,
    sql_select=SqlQueries.songplay_table_insert,
    insert_table="songplays"
)

#######################################################
# Load dim tables using the sql from helpers SqlQueries
load_user_dimension_table = LoadDimensionOperator(
    task_id='Load_user_dim_table',
    dag=dag,
    redshift=REDSHIFT,
    sql_select=SqlQueries.user_table_insert,
    insert_table="users",
    insert_mode=DAG_CONFIG["insert_mode"]["users"]
)
load_song_dimension_table = LoadDimensionOperator(
    task_id='Load_song_dim_table',
    dag=dag,
    redshift=REDSHIFT,
    sql_select=SqlQueries.song_table_insert,
    insert_table="songs",
    insert_mode=DAG_CONFIG["insert_mode"]["songs"]
)
load_artist_dimension_table = LoadDimensionOperator(
    task_id='Load_artist_dim_table',
    dag=dag,
    redshift=REDSHIFT,
    sql_select=SqlQueries.artist_table_insert,
    insert_table="artists",
    insert_mode=DAG_CONFIG["insert_mode"]["artists"]
)
load_time_dimension_table = LoadDimensionOperator(
    task_id='Load_time_dim_table',
    dag=dag,
    redshift=REDSHIFT,
    sql_select=SqlQueries.time_table_insert,
    insert_table="time",
    insert_mode=DAG_CONFIG["insert_mode"]["time"]
)

###########################################
# Run quality checks against various tables
run_song_quality_check = DataQualityOperator(
    task_id='Run_song_quality_check',
    dag=dag,
    redshift=REDSHIFT,
    sql_select="SELECT * FROM songs WHERE songid='SOBLFFE12AF72AA5BA'",
    expected_result=[('SOBLFFE12AF72AA5BA', 'Scream', 'ARJNIUY12298900C91', 2009, Decimal('213'))]
)
run_artist_quality_check = DataQualityOperator(
    task_id='Run_artist_quality_check',
    dag=dag,
    redshift=REDSHIFT,
    sql_select="SELECT * FROM artists WHERE artistid='ARJNIUY12298900C91'",
    expected_result=[('ARJNIUY12298900C91', 'Adelitas Way', '', None, None)]
)
run_user_quality_check = DataQualityOperator(
    task_id='Run_user_quality_check',
    dag=dag,
    redshift=REDSHIFT,
    sql_select="SELECT * FROM users WHERE userid=8",
    expected_result=[(8, 'Kaylee', 'Summers', 'F', 'free')]
)
run_time_quality_check = DataQualityOperator(
    task_id='Run_time_quality_check',
    dag=dag,
    redshift=REDSHIFT,
    sql_select="SELECT * FROM time WHERE start_time='2018-11-01 21:01:46'",
    expected_result=[(datetime.datetime(2018, 11, 1, 21, 1, 46), 21, 1, 44, 11, 2018, 4)]
)

# Define the order of our DAG tasks
# Create tables
start_operator >> create_redshift_tables

# Load staging tables
create_redshift_tables >> [stage_events_to_redshift, stage_songs_to_redshift]

# Load fact table
[stage_events_to_redshift, stage_songs_to_redshift] >> load_songplays_table

# Load dim tables
load_songplays_table >> [load_user_dimension_table, load_song_dimension_table,
                         load_artist_dimension_table, load_time_dimension_table]

# Perform data quality checks
[load_user_dimension_table, load_song_dimension_table,
 load_artist_dimension_table, load_time_dimension_table] >> data_quality_operator

data_quality_operator >> [run_song_quality_check, run_artist_quality_check,
                          run_user_quality_check, run_time_quality_check]

# Finish DAG
[run_song_quality_check, run_artist_quality_check,
 run_user_quality_check, run_time_quality_check] >> end_operator
