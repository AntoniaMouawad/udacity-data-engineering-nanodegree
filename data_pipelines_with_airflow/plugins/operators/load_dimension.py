from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadDimensionOperator(BaseOperator):

    ui_color = '#80BD9E'
    truncate = 'TRUNCATE TABLE {table}'
    insert_query = 'INSERT INTO {table} {query}'
    
    @apply_defaults
    def __init__(self,
                 redshift_conn_id = "",
                 table = "",
                 query = "",
                 do_truncate = False,
                 *args, **kwargs):

        super(LoadDimensionOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.table = table
        self.query = query
        self.do_truncate = do_truncate

    def execute(self, context):
        self.log.info('Connecting to Redshift')
        redshift = PostgresHook(self.redshift_conn_id)
        self.log.info('Connection to Redshift is ready')
        
        self.log.info(f'Loading dimension table: {self.table}')
        if self.do_truncate:
            self.log.info(f'Truncating table: {self.table}')
            redshift.run(LoadDimensionOperator.truncate.format(table= self.table))
        
        redshift.run(LoadDimensionOperator.insert_query.format(table= self.table, query= self.query))
        
        self.log.info('Dimension table loaded')
