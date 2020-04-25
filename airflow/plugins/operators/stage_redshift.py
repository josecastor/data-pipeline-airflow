from airflow.contrib.hooks.aws_hook import AwsHook
from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class StageToRedshiftOperator(BaseOperator):
    ui_color = '#358140'

    template_fields = ("s3_key",)
    copy_sql = """
        COPY {}.{}
        FROM '{}'
        ACCESS_KEY_ID '{}'
        SECRET_ACCESS_KEY '{}'
        COMPUPDATE OFF 
        REGION '{}'
        TIMEFORMAT as 'epochmillisecs' 
        FORMAT AS JSON '{}'
    """
    
    @apply_defaults
    def __init__(self,
                 aws_credentials_id = 'aws_credentials',
                 redshift_conn_id = 'redshift',
                 schema = 'public',
                 table = '',
                 s3_bucket = '',
                 s3_key = '',
                 s3_region = '',
                 s3_format = 'auto',
                 *args, **kwargs):

        super(StageToRedshiftOperator, self).__init__(*args, **kwargs)
        self.aws_credentials_id = aws_credentials_id
        self.redshift_conn_id = redshift_conn_id
        self.schema = schema
        self.table = table
        self.s3_bucket = s3_bucket
        self.s3_key = s3_key
        self.s3_region = s3_region
        self.s3_format = s3_format

    def execute(self, context):
        self.log.info('StageToRedshiftOperator start execution for staging table: {}'.format(self.table))
        aws_hook = AwsHook(self.aws_credentials_id)
        credentials = aws_hook.get_credentials()
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        self.log.info('Copying data from S3 to Redshift')
        rendered_key = self.s3_key.format(**context)
        s3_path = "s3://{}/{}".format(self.s3_bucket, rendered_key)
        formatted_sql = StageToRedshiftOperator.copy_sql.format(
            self.schema,
            self.table,
            s3_path,
            credentials.access_key,
            credentials.secret_key,
            self.s3_region,
            self.s3_format
        )
        redshift.run(formatted_sql)
        self.log.info('StageToRedshiftOperator finish execution for staging table: {}'.format(self.table))
        