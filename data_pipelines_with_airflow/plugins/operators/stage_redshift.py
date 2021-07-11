from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow.contrib.hooks.aws_hook import AwsHook

class StageToRedshiftOperator(BaseOperator):
        
    ui_color = '#358140'
    template_fields = ("s3_key",)
    
    @apply_defaults
    def __init__(self,
                 redshift_conn_id ="",
                 aws_credentials_id = "",
                 s3_bucket="",
                 s3_key="",
                 table = "",
                 json_format = "",
                 *args, **kwargs):

        super(StageToRedshiftOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.aws_credentials_id = aws_credentials_id
        self.s3_bucket=s3_bucket
        self.s3_key=s3_key
        self.table = table
        self.json_format = json_format
        

    def execute(self, context):
        aws_hook = AwsHook(self.aws_credentials_id)
        aws_credentials = aws_hook.get_credentials()
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        
        self.log.info(f"Clearing data from destination Redshift table: {self.table}")
        redshift.run(f"DELETE FROM {self.table}")
        
        self.log.info("Copying data from S3 to Redshift")

        rendered_key = self.s3_key.format(**context)
        s3_path = f"s3://{self.s3_bucket}/{rendered_key}"
        
        redshift.run(f"COPY {self.table} FROM '{s3_path}' ACCESS_KEY_ID '{aws_credentials.access_key}' \
            SECRET_ACCESS_KEY '{aws_credentials.secret_key}' FORMAT AS JSON '{self.json_format}'")
        





