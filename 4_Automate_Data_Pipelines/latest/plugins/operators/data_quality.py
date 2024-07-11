from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class DataQualityOperator(BaseOperator):

    ui_color = '#89DA59'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id = "",
                 tables_list = "",
                 *args, **kwargs):

        super(DataQualityOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.tables_list = tables_list

    def execute(self, context):
        self.log.info('DataQualityOperator')
        redshift_hook = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        
        for table in self.tables_list:
            sql_query = f"SELECT COUNT(*) FROM {table}"
            records = redshift_hook.run(sql_query)
            
            if len(records) < 1 or len(records[0]) < 1:
                raise ValueError(f"Data quality check failed. {table} returned no results")
                
            num_records = records[0][0]
            
            if num_records < 1:
                raise ValueError(f"Data quality check failed. {table} contained 0 rows")
                
            logging.info(f"Data quality on table {table} check passed with {records[0][0]} records")


        
