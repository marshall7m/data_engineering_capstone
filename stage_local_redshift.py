from airflow.contrib.hooks.aws_hook import AwsHook
from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
import pandas as pd
import os
import glob

class StageLocalToRedshiftOperator(BaseOperator):
    ui_color = '#358140'
    
    #@apply_defaults

    def __init__(self,
                 redshift_conn_id='',
                 aws_credentials_id='',
                 file_format='',
                 file_directory='',
                 sql='',
                 *args, **kwargs):
        """
        Initialize Redshift and local file parameters.
        
        Keyword Arguments:
        redshift_conn_id   -- Redshift connection ID configured in Airflow/admin/connection UI (str)
        aws_credentials_id -- AWS connection ID configured in Airflow/admin/connection UI (str)
        file_format -- File format for AWS S3 files  (currently only: 'JSON' or 'CSV') (str)
        table -- AWS S3 table to extract from (str)
        """
        super(StageLocalToRedshiftOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.aws_credentials_id = aws_credentials_id
        self.file_format = file_format
        self.file_directory = file_directory
        self.columns
        self.sql = sql

    def get_all_files(self):
        all_files = []
        for root, _, files in os.walk(self.file_directory):
            files = glob.glob(os.path.join(root,'*.' + self.file_format))
            for f in files :
                all_files.append(os.path.abspath(f))

        # print total number of files found
        num_files = len(all_files)
        print('{} {} files found in {}'.format(num_files, self.file_format, self.filepath))

        return all_files, num_files
        
    def execute(self, context):
        """
        Executes formatted insert command to stage data from local file directory to Redshift.
        
        Keyword Argument:
        context -- DAG context dictionary
        """

        self.log.info('StageToLocalRedshiftOperator instantiating AWS and Redshift connection variables')
        redshift = PostgresHook(self.redshift_conn_id)

        all_files, num_files = get_all_files()
        
        #read file into pandas dataframe 
        for i, datafile in enumerate(all_files, 1):

            if self.file_format.lower() == 'json':
                df = pd.read_json(datafile, lines=True)

            elif self.file_format.lower() == 'csv':
                df = pd.read_csv(datafile)
            
            elif self.file_format.lower() == 'xlsx':
                df = pd.read_xlsx(datafile)

            else:
                self.log.info('StageToLocalRedshiftOperator: Invalid file format. Available file formats: "csv", "json", "xlsx" ')
            
            # select needed columns
            df = df[self.columns]

            #for every row in dataframe, insert into apporiate table
            for _,row in df.iterrows():
                redshift.execute(self.sql, row)
            
            print('{}/{} files processed.'.format(i, num_files))
        
        