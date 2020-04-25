from airflow.contrib.hooks.aws_hook import AwsHook
from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class DataQualityOperator(BaseOperator):

    ui_color = '#89DA59'

    @apply_defaults
    def __init__(self,
                 aws_credentials_id = 'aws_credentials',
                 redshift_conn_id = 'redshift',
                 sql_schema = 'public',    
                 tables = [],
                 stmts_checks = [],
                 *args, **kwargs):

        super(DataQualityOperator, self).__init__(*args, **kwargs)
        self.aws_credentials_id = aws_credentials_id
        self.redshift_conn_id = redshift_conn_id
        self.sql_schema = sql_schema    
        self.tables = tables
        self.stmts_checks = stmts_checks

    def execute(self, context):
        self.log.info('DataQualityOperator not implemented yet')
        aws_hook = AwsHook(self.aws_credentials_id)
        credentials = aws_hook.get_credentials()
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        
        for table in self.tables:
            for stmt in self.stmts_checks:
                self.log.info(stmt['sql'].format(self.sql_schema, table))
                sql = stmt['sql'].format(self.sql_schema, table)
                result = int(redshift.get_first(sql)[0])
                strError = 'Check failed: {} {} {}'.format(result, stmt['op'], stmt['val'])
                # Check greater than
                if stmt['op'] == 'gt' and result <= stmt['val']:
                    raise AssertionError(strError)
                # Check equal
                elif stmt['op'] == 'eq' and result != stmt['val']:
                    raise AssertionError(strError)
                # Check if not equal
                elif stmt['op'] == 'ne' and result == stmt['val']:
                    raise AssertionError(strError)
                    
            self.log.info('Passed check: {} {} {}'.format(result, stmt['op'], stmt['val']))