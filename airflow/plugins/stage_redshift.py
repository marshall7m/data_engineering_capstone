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
        IAM_ROLE '{}'
        REGION '{}'
        {}
        DELIMITER ','
        IGNOREHEADER 1
        DATEFORMAT 'YYYY-MM-DD'
        TIMEFORMAT 'epochmillisecs'
        TRUNCATECOLUMNS BLANKSASNULL EMPTYASNULL
        MAXERROR 100000
        {};
    """

    #template variable used for formatting s3 path in execute function
    template_fields = ('s3_key',)

    def __init__(self,
                 redshift_conn_id='',
                 s3_bucket='',
                 s3_key='',
                 iam_role='',
                 degree='',
                 s3_data='',
                 region='',
                 file_format='',
                 table='',
                 extra_copy_parameters='',
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
        self.s3_bucket = s3_bucket
        self.s3_key = s3_key
        self.iam_role = iam_role
        self.s3_data = s3_data
        self.degree = degree
        self.region = region
        self.file_format = file_format
        self.table = table
        self.extra_copy_parameters=extra_copy_parameters

    def execute(self, context):
        """
        Executes formatted COPY command to stage data from S3 to Redshift.
        
        Keyword Argument:
        context -- DAG context dictionary
        """

        self.log.info('StageToRedshiftOperator instantiating AWS and Redshift connection variables')
        redshift = PostgresHook(self.redshift_conn_id)

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
            self.iam_role,
            self.region,
            self.file_format,
            self.extra_copy_parameters
        )

        self.log.info(f'Loading {self.table}')
        redshift.run(formatted_copy_sql)
       