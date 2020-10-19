import pandas as pd
import os,json,psycopg2,boto3,ndjson
from datetime import datetime
from datetime import date
from dateutil import parser
import datetime as dt
from dateutil.relativedelta import relativedelta

from dataEngineer.L1.datamart.sql import read
from dataEngineer.L1.datamart.sql import copy_command
from dataEngineer.L1.datamart.sql import create_table
from dataEngineer.L1.datamart.sql import delete
from dataEngineer.L1.datamart.sql import get_first_date

class etl_execute:
    
    def get_first_date(self,query,conn):
        conn_aws = conn.get_conn()
        
        df = pd.io.sql.read_sql(query, conn_aws)
        
        return str(df['date'][0])
    
    def get_list_date(self,firstDate,lastDate,condition):
        """
        get start date and end date
        create list of Date
        """  
        if condition == 0:
            startDate = parser.parse(firstDate)
            startDate = (startDate - dt.timedelta(days=60)).strftime('%Y-%m-%d')
        elif condition == 1:
            startDate = firstDate

        lastDate = parser.parse(lastDate)
        endDate = (lastDate + dt.timedelta(days=1)).strftime('%Y-%m-%d')

        cur_date = datetime.strptime(startDate, '%Y-%m-%d').date()
        end = datetime.strptime(endDate, '%Y-%m-%d').date()

        listOfDate = []
        while cur_date < end:
            listOfDate.append(cur_date.strftime('%Y-%m-%d'))
            cur_date += relativedelta(days=1)
        
        return listOfDate 
    
    def get_last_date(self,date):
        datetimes = parser.parse(date)
        last_date = (datetimes + dt.timedelta(days=0)).strftime('%Y-%m-%d')
    
        return last_date
    
    def get_column(self,table,conn,schema):
        # get column
        conn_aws = conn.get_conn()
        
        query = '''
        SELECT column_name
            from information_schema.columns
        where table_name = '{table}' and table_schema = '{schema}'
        '''.format(table=table,schema=schema)
        
        df = pd.io.sql.read_sql(query, conn_aws)
        
        return df
   
    def transform_data_type(self,df,column_datetime,column_date):
        
        # Change data type to format that json can read
        for column in column_date:
            df[column] = df[column].astype(str)
            df[column].loc[df[column] == 'None'] = None
            df[column].loc[df[column] == 'nan'] = None
            df[column].loc[df[column] == 'NaT'] = None
        
        for column in column_datetime:
            df[column] = df[column].astype(str)
            df[column].loc[df[column] == 'None'] = None
            df[column].loc[df[column] == 'nan'] = None
            df[column].loc[df[column] == 'NaT'] = None
        
        return df
    
    def fetch_data(self,query,conn): 
        
        conn_aws = conn.get_conn()
            
        # Create dataFrame from Query
        df = pd.io.sql.read_sql(query, conn_aws)
        
        # get column name
        column_datetime = [col for col in df.columns if 'time' in col]
        column_date = [col for col in df.columns if 'date' in col]
        
        # transform data type    
        df = self.transform_data_type(df,column_datetime,column_date)
        
        return df
        
    
    def add_list_type(self,df):
        # define column and type
        list_type_temp = list(df.dtypes)
        list_column_temp = list(df.columns)
        
        # get format column and type in dict
        new_list = []
        for i in range(len(list_type_temp)):
            temp_dict = {
                'columns' : list_column_temp[i],
                'types' : list_type_temp[i]
            }
            if temp_dict['types'] == 'object':
                temp_dict['types'] = 'varchar(255)'
            if temp_dict['types'] == 'int64':
                temp_dict['types'] = 'integer'
            if temp_dict['types'] == 'float64':
                temp_dict['types'] = 'float'
            if 'date' in temp_dict['columns']:
                temp_dict['types'] = 'date'
            if 'time' in temp_dict['columns']:
                if temp_dict['columns'] == 'timezone':
                    temp_dict['types'] = 'varchar(255)'
                else:
                    temp_dict['types'] = 'timestamp'
                
            new_list.append(temp_dict)
        
        return new_list
        
    def df_to_json(self,df,path,table):
        '''
        Dump DataFrame to JSON
        '''
        with open(path, 'w', encoding='utf-8') as file:
            df.to_json(file, orient='records', force_ascii=False, lines=True)  
    
    def json_to_s3(self,path,filename,table,s3_key_file,s3_bucket,access_key,secret_key):
        '''
        Upload Json TO S3
        '''
        keys = '{path_key}/{filename}'.format(path_key=s3_key_file,filename=filename)
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
            
    def main(self,date,config,conn_aws,verbose=1):
        # First initial to know performance script
        if verbose:
            print('executing engine....')
            start = datetime.now()
        
        # Config
        s3_bucket = config['s3_bucket']
        access_key = config['access_key']
        secret_key = config['secret_key']
        s3_key_file = config['s3_key_file']
        table = config['table']
        column = config['column']
        path_dags = config['path']
        schema_db = config['schema']
        
        # Get data
        query_first,query = read.query(table,path_dags)
        print(query_first)
        df = self.fetch_data(query_first,conn_aws)
        
        if df.empty == False:
            
            # Compare column table and datafram
            column_table = self.get_column(table,conn_aws,schema_db)
            list_column_table = sorted(column_table['column_name'].tolist(),key=str.lower)
            list_column_df = sorted(df.columns.tolist(),key = str.lower)
            
            if((list_column_df==list_column_table) == False):
                # create new table
                print('------------>>>>>>>On process create new table')
                query_create = create_table.query(table,self.add_list_type(df),schema_db)
                conn_aws.run(query_create)
                
                # checking data
                query_date = get_first_date.query(path_dags,table,column)
                firstDate = self.get_first_date(query_date,conn_aws)
                
                # list of date
                listOfDate = self.get_list_date(firstDate,date,1)
                
                # chunk with date
                for date in listOfDate:
                    datenodash = date.replace('-','')
                    last_date = self.get_last_date(date)
                    # get DF all data
                    query_first,query = read.query(table,path_dags,column,1,date,last_date)
                    df = self.fetch_data(query,conn_aws)
                    print('------>>>>Process in date {date}'.format(date=date))
                    # new path and filename
                    filename = '{schema}_{table}.json'.format(schema=schema_db,table=table)
                    path = '/tmp/{filename}'.format(filename=filename)
                    
                    # Dump to json
                    self.df_to_json(df,path,table)
                    
                    # Load json to S3
                    key = self.json_to_s3(path,filename,table,s3_key_file,s3_bucket,access_key,secret_key)
                    # S3 to Redshift
                    query_load = copy_command.query(s3_bucket,key,access_key,secret_key,table,schema_db)
                    conn_aws.run(query_load)
                    print('------------>>>> success Load data on {table}'.format(table=table))
                    
                    os.remove(path)
                    print('-------------------------->>> success delete data {}'.format(path))
                    
            else:
                # list of date
                listOfDate = self.get_list_date(date,date,0)
                
                # chunk with date
                for date in listOfDate:
                    datenodash = date.replace('-','')
                    last_date = self.get_last_date(date)
                    delete_data = delete.query(schema_db,table,column,1,date,last_date)
                    conn_aws.run(delete_data)
                    
                    # get DF all data
                    query_first,query = read.query(table,path_dags,column,1,date,last_date)
                    df = self.fetch_data(query,conn_aws)
                    print('------>>>>Process in date {date}'.format(date=date))
                    
                    # new path and filename
                    filename = '{schema}_{table}.json'.format(schema=schema_db,table=table)
                    path = '/tmp/{filename}'.format(filename=filename)
                    
                    # Dump to json
                    self.df_to_json(df,path,table)
                    
                    # Load json to S3
                    key = self.json_to_s3(path,filename,table,s3_key_file,s3_bucket,access_key,secret_key)
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