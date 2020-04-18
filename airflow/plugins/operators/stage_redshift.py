from airflow.contrib.hooks.aws_hook import AwsHook
from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class StageToRedshiftOperator(BaseOperator):
    ui_color = '#358140'
    
    #@apply_defaults

    copy_sql = """
        COPY {}
        FROM '{}'
        ACCESS_KEY_ID '{}'
        SECRET_ACCESS_KEY '{}'
        REGION '{}'
        {} 'auto'
        TRUNCATECOLUMNS BLANKSASNULL EMPTYASNULL
        {};
    """
    #         IGNOREHEADER 1 

    #TIMEFORMAT as 'epochmillisecs'
    #template variable used for formatting s3 path in execute function
    template_fields = ('s3_key',)

    def __init__(self,
                 redshift_conn_id='',
                 aws_credentials_id='',
                 s3_bucket='',
                 s3_key='',
                 degree='',
                 s3_data='',
                 region='',
                 file_format='',
                 table='',
                 *args, **kwargs):
        """
        Initialize Redshift and S3 parameters.
        
        Keyword Arguments:
        redshift_conn_id   -- Redshift connection ID configured in Airflow/admin/connection UI (str)
        aws_credentials_id -- AWS connection ID configured in Airflow/admin/connection UI (str)
        s3_bucket -- AWS S3 bucket name (str)
        s3_key -- AWS S3 bucket data directory/file (str)
        file_format -- File format for AWS S3 files  (currently only: 'JSON' or 'CSV') (str)
        table -- AWS S3 table to extract from (str)
        """
        super(StageToRedshiftOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.aws_credentials_id = aws_credentials_id
        self.s3_bucket = s3_bucket
        self.s3_key = s3_key
        self.s3_data = s3_data
        self.degree = degree
        self.region = region
        self.file_format = file_format
        self.table = table

    def execute(self, context):
        """
        Executes formatted COPY command to stage data from S3 to Redshift.
        
        Keyword Argument:
        context -- DAG context dictionary
        """

        self.log.info('StageToRedshiftOperator instantiating AWS and Redshift connection variables')
        redshift = PostgresHook(self.redshift_conn_id)
        aws = AwsHook(self.aws_credentials_id)
        credentials = aws.get_credentials()

        extra_parameters = ''
        if self.file_format.upper() == 'CSV':
            extra_parameters = " DELIMETER ',' "

        #Formats s3 key with context dictionary
        # rendered_key = self.s3_key.format(self.degree, *context, self.s3_data)
        s3_key_data = {
                        'execution_year': context['execution_date'].year, 
                        'execution_month': context['execution_date'].month,
                        'degree': self.degree,
                        's3_data': self.s3_data,
                        'file_format': self.file_format.lower()
        }
        rendered_key = self.s3_key.format(**s3_key_data)
        s3_path = 's3://{}/{}'.format(self.s3_bucket, rendered_key)
        
        formatted_copy_sql = StageToRedshiftOperator.copy_sql.format(
            self.table,
            s3_path,
            credentials.access_key,
            credentials.secret_key,
            self.region,
            self.file_format,
            extra_parameters
        )
#         formatted_update_sql = """
#         DECLARE @columnname nvarchar(100)
#         SELECT @columnname = ORDINAL_POSITION FROM INFORMATION_SCHEMA.COLUMNS
#             WHERE TABLE_NAME = '{}' AND ORDINAL_POSITION = '1'
#         UPDATE {} SET @columnname = REPLACE(@columnname, '\{' , '')::INTEGER""".format(self.table, self.table)
#         redshift.run(formatted_update_sql)

        redshift.run(formatted_copy_sql)
       