import pandas as pd
import os,json,psycopg2,boto3,ndjson
from datetime import datetime
from datetime import date
from dateutil import parser
import datetime as dt

from dataEngineer.L0.etl_mysql_to_redshift.accommodation_service_production.sql import read
from dataEngineer.L0.etl_mysql_to_redshift.accommodation_service_production.sql import read_no_filter
from dataEngineer.L0.etl_mysql_to_redshift.accommodation_service_production.sql import read_first
from dataEngineer.L0.etl_mysql_to_redshift.accommodation_service_production.sql import copy_command
from dataEngineer.L0.etl_mysql_to_redshift.accommodation_service_production.sql import create_table
from dataEngineer.L0.etl_mysql_to_redshift.accommodation_service_production.sql import delete
from dataEngineer.L0.etl_mysql_to_redshift.accommodation_service_production.sql import delete_no_filter


class etl_execute:
    
    def get_date(self,date):
        # time condition
        datetimes = parser.parse(date)
        date = datetimes.strftime('%Y-%m-%d')
        date_nodash = datetimes.strftime('%Y%m%d')
        
        return date,date_nodash
    
    def get_column(self,table,schema_db,conn):
        # get column
        conn_aws = conn.get_conn()
        
        query = '''
        SELECT column_name, data_type
            from information_schema.columns
        where table_name = '{table}' and table_schema = '{schema_db}'
        '''.format(table=table,schema_db=schema_db)
        
        df = pd.io.sql.read_sql(query, conn_aws)
        
        return df
    
    def create_column_table(self,df):
        # define column and type
        
        listOfDict = df.to_dict('records')
        for dicts in listOfDict:
            if dicts['data_type'] == 'enum':
                dicts['data_type'] = 'varchar(32)'
            if dicts['data_type'] == 'varchar':
                dicts['data_type'] = 'varchar(255)'
            if dicts['data_type'] == 'tinyint':
                dicts['data_type'] = 'int'
            if dicts['data_type'] == 'text':
                dicts['data_type'] = 'varchar(65535)'
            if dicts['data_type'] == 'datetime':
                dicts['data_type'] = 'timestamp'
            if dicts['data_type'] == 'double':
                dicts['data_type'] = 'float'
            if dicts['data_type'] == 'bool':
                dicts['data_type'] = 'boolean'
            if dicts['data_type'] == 'decimal':
                dicts['data_type'] = 'float'
            if dicts['data_type'] == 'json':
                dicts['data_type'] = 'varchar(65535)'
            if dicts['data_type'] == 'longtext':
                dicts['data_type'] = 'varchar(65535)'

        return listOfDict
   
    def transform_data_type(self,df,schema):
        
        for listOfDict in schema:
            if 'date' in listOfDict['data_type']:
                df[listOfDict['column_name']] = pd.to_datetime(df[listOfDict['column_name']], format='%Y-%m-%d')
                df[listOfDict['column_name']] = df[listOfDict['column_name']].dt.strftime('%Y-%m-%d')
                df[listOfDict['column_name']].loc[df[listOfDict['column_name']] == 'NaT'] = None
            elif 'timestamp' in listOfDict['data_type']:
                df[listOfDict['column_name']] = pd.to_datetime(df[listOfDict['column_name']], format='%Y-%m-%d %H:%M:%S')
                df[listOfDict['column_name']] = df[listOfDict['column_name']].dt.strftime('%Y-%m-%d %H:%M:%S')
                df[listOfDict['column_name']].loc[df[listOfDict['column_name']] == 'NaT'] = None
            elif 'password' in listOfDict['column_name']:
                df[listOfDict['column_name']] = None
            elif 'account_number' in listOfDict['column_name'] :
                df[listOfDict['column_name']] = None
        
        return df
    
    def fetch_data(self,query,conn,schema): 
        
        conn_mysql = conn.get_conn()
        print(query)
            
        # Create dataFrame from Query
        df = pd.io.sql.read_sql(query, conn_mysql)
        
        df = self.transform_data_type(df,schema)
          
        return df
        
    def df_to_json(self,df,path,table):
        '''
        Dump DataFrame to JSON
        '''
        with open(path, 'w', encoding='utf-8') as file:
            df.to_json(file, orient='records', force_ascii=False, lines=True)
                
    
    def json_to_s3(self,path,filename,table,schema_s3,s3_key_file,s3_bucket,access_key,secret_key):
        '''
        Upload Json TO S3
        '''
        keys = '{path_key}/{schema_s3}/{filename}'.format(path_key=s3_key_file,filename=filename,schema_s3=schema_s3)
        s3 = boto3.client(
        "s3",
        aws_access_key_id=access_key,
        aws_secret_access_key=secret_key
        )
        s3.upload_file(
            Bucket = s3_bucket,
            Filename=path,
            Key=keys
            )
        
        print('---------------->>>>>>>success load {table} to s3!!!'.format(table=table))
        
        return keys
            
    def main(self,date,config,configTable,conn_aws,conn_mysql,verbose=1):
        # First initial to know performance script
        if verbose:
            print('executing engine....')
            start = datetime.now()
        
        # get date from airflow scheduler    
        date,date_nodash = self.get_date(date)
            
        # Config
        path = '/tmp'
        s3_bucket = config['s3_bucket']
        access_key = config['access_key']
        secret_key = config['secret_key']
        s3_key_file = config['s3_key_file']
        table = configTable['table']
        schema_db = configTable['schema_db']
        schema_s3 = configTable['schema_s3']
        column = configTable['column']
        
        filename = '{schema_s3}_{table}_{date}.json'.format(schema_s3=schema_s3,table=table,date=date_nodash)
        path = '{path}/{filename}'.format(path=path,filename=filename)
        
        # get information table 
        # mysql
        column_table_mysql = self.get_column(table,schema_db,conn_mysql)
        schema_mysql = self.create_column_table(column_table_mysql)
        # aws
        column_table_aws = self.get_column(table,schema_db,conn_aws)
        
        # Get data
        query = read_first.query(schema_db,table)
        df = self.fetch_data(query,conn_mysql,schema_mysql)
        
        if df.empty == False:
            
            # Compare column table and datafram
            list_column_table = sorted(column_table_aws['column_name'].tolist(),key=str.lower)
            list_column_df = sorted(df.columns.tolist(),key = str.lower)
            
            if((list_column_df==list_column_table) == False):
                # create new table
                print('------------>>>>>>>On process create new table')
                query_create = create_table.query(schema_db,table,schema_mysql)
                conn_aws.run(query_create)
                # get DF all data
                query_read = read_no_filter.query(schema_db,table)
                df = self.fetch_data(query_read,conn_mysql,schema_mysql)
                # new path and filename
                filename = '{schema_s3}_{table}.json'.format(schema_s3=schema_s3,table=table)
                path = '/tmp/{filename}'.format(path=path,filename=filename)
            else:
                # condition delete data
                if isinstance(column,str):
                    delete_data = delete.query(date,schema_db,table,column)
                    query_read = read.query(schema_db,table,column,date)
                else:
                    delete_data = delete_no_filter.query(schema_db,table)
                    query_read = read_no_filter.query(schema_db,table)
                    
                conn_aws.run(delete_data)
                # get DF by flagging date
                df = self.fetch_data(query_read,conn_mysql,schema_mysql)
                filename = '{schema_s3}_{table}.json'.format(schema_s3=schema_s3,table=table)
            
            # Dump to json
            self.df_to_json(df,path,table)
            
            # Load json to S3
            key = self.json_to_s3(path,filename,table,schema_s3,s3_key_file,s3_bucket,access_key,secret_key)
            # S3 to Redshift
            query_load = copy_command.query(s3_bucket,key,access_key,secret_key,table,schema_db)
            conn_aws.run(query_load)
            print('------------>>>> success Load data on {table}'.format(table=table))
            os.remove(path)
            print('-------------------------->>> success delete data {}'.format(path))

        else:
            print('No Data!')
        if verbose:
            print('process executed in ' + str(datetime.now() - start))