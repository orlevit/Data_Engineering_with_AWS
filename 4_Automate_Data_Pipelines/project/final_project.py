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
    'owner': 'udacity',
    'start_date': pendulum.now(),
}

@dag(
    default_args=default_args,
    description='Load and transform data in Redshift with Airflow',
    schedule_interval='0 * * * *'
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
        s3_data_loc=Variable.get('project4_log_data'),
        aws_credentials_id="aws_credentials",
        region=Variable.get('region'),
        json_metadata=Variable.get('project4_log_json_metadata')
    )
     
    stage_songs_to_redshift = StageToRedshiftOperator(
        task_id='Stage_songs',
    )

    #load_songplays_table = LoadFactOperator(
    #    task_id='Load_songplays_fact_table',
    #)

    #load_user_dimension_table = LoadDimensionOperator(
    #    task_id='Load_user_dim_table',
    #)

    #load_song_dimension_table = LoadDimensionOperator(
    #    task_id='Load_song_dim_table',
    #)

    #load_artist_dimension_table = LoadDimensionOperator(
    #    task_id='Load_artist_dim_table',
    #)

    #load_time_dimension_table = LoadDimensionOperator(
    #    task_id='Load_time_dim_table',
    #)

    #run_quality_checks = DataQualityOperator(
    #    task_id='Run_data_quality_checks',
    #)

    start_operator >> stage_events_to_redshift
    
final_project_dag = final_project()
