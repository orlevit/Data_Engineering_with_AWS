import os
import sys
import pendulum
from airflow.decorators import dag
from airflow.models import Variable
from datetime import datetime, timedelta
from airflow.operators.dummy import DummyOperator

sys.path.insert(0, '/home/or/airflow/dags/project4/plugins/operators')
sys.path.insert(0, '/home/or/airflow/dags/project4/plugins/helpers')

from stage_redshift import StageToRedshiftOperator
from load_fact import LoadFactOperator
from load_dimension import LoadDimensionOperator
from data_quality import DataQualityOperator
from sql_queries import SqlQueries


default_args = {
    'owner': 'or levitas',
    'depends_on_past': False,    
    'start_date': pendulum.now(),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'catchup': False
}

@dag(
    default_args=default_args,
    description='Load and transform data in Redshift with Airflow',
    schedule_interval='@hourly'  
)
def final_project():

    start_operator = DummyOperator(task_id='Begin_execution')

    stage_events_to_redshift = StageToRedshiftOperator(
        task_id='Stage_events',
        table='staging_events',
        create_script=SqlQueries.staging_events_table_create,
        populate_script=SqlQueries.staging_copy,
        redshift_conn_id='redshift',
        s3_bucket=Variable.get('s3_bucket'),
        s3_data_loc=Variable.get('project4_logs_data'),
        aws_credentials_id="aws_credentials",
        region=Variable.get('region'),
        json_metadata="s3://{}/{}".format(Variable.get('s3_bucket'), Variable.get('project4_log_json_metadata'))
    )
     
    stage_songs_to_redshift = StageToRedshiftOperator(
        task_id='Stage_songs',
        table='staging_songs',
        create_script=SqlQueries.staging_songs_table_create,
        populate_script=SqlQueries.staging_copy,
        redshift_conn_id='redshift',
        s3_bucket=Variable.get('s3_bucket'),
        s3_data_loc=Variable.get('project4_songs_data'),
        aws_credentials_id="aws_credentials",
        region=Variable.get('region'),
        json_metadata='auto'
    )

    load_songplays_table = LoadFactOperator(
        task_id='Load_songplays_fact_table',
        redshift_conn_id='redshift',
        table='songplay',
        create_script=SqlQueries.songplay_table_create,
        populate_script=SqlQueries.songplay_table_insert,
        append_or_delete_load=APPEND_OR_DELETE_LOAD        
    )

    load_user_dimension_table = LoadDimensionOperator(
        task_id='Load_user_dim_table',
        redshift_conn_id='redshift',
        table='users',
        create_script=SqlQueries.user_table_create,    
        populate_script=SqlQueries.user_table_insert,
        append_or_delete_load=APPEND_OR_DELETE_LOAD        
    )

    load_song_dimension_table = LoadDimensionOperator(
        task_id='Load_song_dim_table',
        redshift_conn_id='redshift',
        table='songs',
        create_script=SqlQueries.song_table_create,    
        populate_script=SqlQueries.song_table_insert,
        append_or_delete_load=APPEND_OR_DELETE_LOAD
    )

    load_artist_dimension_table = LoadDimensionOperator(
        task_id='Load_artist_dim_table',
        redshift_conn_id='redshift',
        table='artists',
        create_script=SqlQueries.artist_table_create,    
        populate_script=SqlQueries.artist_table_insert,
        append_or_delete_load=APPEND_OR_DELETE_LOAD        
    )

    load_time_dimension_table = LoadDimensionOperator(
        task_id='Load_time_dim_table',
        redshift_conn_id='redshift',
        table='time',
        create_script=SqlQueries.time_table_create,    
        populate_script=SqlQueries.time_table_insert,
        append_or_delete_load=APPEND_OR_DELETE_LOAD        
    )

    run_quality_checks = DataQualityOperator(
        task_id='Run_data_quality_checks',
        redshift_conn_id='redshift',
        sql_quality_tests=SqlQueries.data_quality_tests,
        tables_list=['songplay', 'users', 'songs', 'artists', 'time']    
    )
    
    start_operator >> stage_events_to_redshift
    start_operator >> stage_songs_to_redshift
    
    stage_events_to_redshift >> load_songplays_table
    stage_songs_to_redshift >> load_songplays_table
    
    load_songplays_table >> load_user_dimension_table
    load_songplays_table >> load_song_dimension_table
    load_songplays_table >> load_artist_dimension_table
    load_songplays_table >> load_time_dimension_table
    
    load_user_dimension_table >> run_quality_checks
    load_song_dimension_table >> run_quality_checks
    load_artist_dimension_table >> run_quality_checks
    load_time_dimension_table >> run_quality_checks

final_project_dag = final_project()



