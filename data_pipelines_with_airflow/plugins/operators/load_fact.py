from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadFactOperator(BaseOperator):

    ui_color = '#F98866'
    insert_query = "INSERT INTO {table} {query}"
    @apply_defaults
    def __init__(self,
                 redshift_conn_id = "",
                 table = "",
                 query = "",
                 *args, **kwargs):

        super(LoadFactOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.table = table
        self.query = query

    def execute(self, context):
        self.log.info('Connecting to Redshift')
        redshift = PostgresHook(self.redshift_conn_id)
        self.log.info('Connection to Redshift is ready')
        
        self.log.info(f'Loading fact table: {self.table}')
        redshift.run(LoadFactOperator.insert_query.format(table= self.table, query= self.query))
        self.log.info('Fact table loaded')