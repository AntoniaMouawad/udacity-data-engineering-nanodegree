from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class DataQualityOperator(BaseOperator):

    ui_color = '#89DA59'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id = '',
                 checks = [], 
                 *args, **kwargs):

        super(DataQualityOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.checks = checks

    def execute(self, context):
        self.log.info('Connecting to Redshift')
        redshift = PostgresHook(self.redshift_conn_id)
        
        for check in self.checks:
            test_id = check['id']
            query = check['query']
            expected_res = check['expected_res']
            actual_res = redshift.get_first(query)
            if actual_res== None or expected_res != actual_res[0]:
                self.log.exception(f'Expected result differs from actual result for test id: {test_id}')
                raise ValueError(f'Expected result differs from actual result for test id: {test_id}')
            self.log.info(f'test id: {test_id} passed')
