from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadDimensionOperator(BaseOperator):

    ui_color = '#80BD9E'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 sql_query="",
                 table="",
                 clear_flag = False,
                 *args, **kwargs):

        super(LoadDimensionOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.sql_query = sql_query
        self.table = table
        self.clear_flag = clear_flag

    def execute(self, context):
        redshift = PostgresHook(postgress_conn_id = self.redshift_conn_id)
        
        if self.clear_flag:
            self.log.info(f'Clearing {self.table} table')
            redshift.run('TRUNCATE TABLE {}'.format(self.table))
        
        
        redshift.run('INSERT INTO {} {}'.format(self.table, self.sql_query))
            
            