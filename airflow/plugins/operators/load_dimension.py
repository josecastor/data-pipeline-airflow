from airflow.contrib.hooks.aws_hook import AwsHook
from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadDimensionOperator(BaseOperator):

    ui_color = '#80BD9E'

    @apply_defaults
    def __init__(self,
                 aws_credentials_id = 'aws_credentials',
                 redshift_conn_id = 'redshift',
                 sql_schema = 'public',
                 sql = '',
                 *args, **kwargs):

        super(LoadDimensionOperator, self).__init__(*args, **kwargs)
        self.aws_credentials_id = aws_credentials_id
        self.redshift_conn_id = redshift_conn_id
        self.sql_schema = sql_schema
        self.sql = sql

    def execute(self, context):
        self.log.info('LoadDimensionOperator initiated execution')
        aws_hook = AwsHook(self.aws_credentials_id)
        credentials = aws_hook.get_credentials()
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        self.log.info('Start :: Loading data dimension table')
        redshift.run('SET search_path TO {}; {}'.format(self.sql_schema, self.sql))
        self.log.info('Finish :: Loading data dimension table')
        self.log.info('LoadDimensionOperator finished execution')
        