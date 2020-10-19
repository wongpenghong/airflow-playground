import pandas as pd
from datetime import datetime
from datetime import date
from dateutil import parser
from googleapiclient.discovery import build
from google_auth_oauthlib.flow import InstalledAppFlow,Flow
from google.auth.transport.requests import Request
import os
import pickle

from dataEngineer.L1.validation.sql import read

class etl_execute:
    
    def create_service(self,client_secret_file,api_service_name,api_version,pickle_path):
        if os.path.exists(pickle_path):
            with open(pickle_path, 'rb') as token:
                cred = pickle.load(token)

        service = build(api_service_name, api_version, credentials=cred)
        print(api_service_name, 'service created successfully')
        return service
            
    def export_data_to_sheets(self,service,gsheetId,RANGE_NAME,df):
        service.spreadsheets().values().update(
            spreadsheetId=gsheetId,
            valueInputOption='RAW',
            range=RANGE_NAME,
            body=dict(
                majorDimension='ROWS',
                values=df.T.reset_index().T.values.tolist())
        ).execute()
        print('Sheet successfully Updated')
    
    def get_compare_df(self,query,conn,source): 
            
        conn = conn.get_conn()
        
        # Create dataFrame from Query
        schema = 'schemas_source_{0}'.format(source)
        table = 'tables_source_{0}'.format(source)
        total = 'total_source_{0}'.format(source)
        
        # try except when datamrt was not built
        try:
            df = pd.io.sql.read_sql(query, conn)
            
            df.columns = [schema, table, total] 
                
            df['source'] = source
        
        except:
            df = pd.DataFrame
        
        return df
    
    
    def get_condition_check(self,df):
        if len(df)==1:
            if df['total_source_query'][0] == df['total_source_datamart'][0]:
                df['condition'] = True
            else:
                df['condition'] = False

        else:
            result = []
            for i in range(len(df)):
                if df['total_source_query'][i] == df['total_source_datamart'][i]:
                    result.append(True)
                else:
                    result.append(False)
            df['condition'] = result
        return df
    
    def main(self, conn_aws, config, verbose=1):
        # First initial to know performance script
        if verbose:
            print('executing engine....')
            start = datetime.now()
        
        credential_path = config['credential_gsheet_path']
        pickle_path = config['pickle_gsheet_path']
        
        schema = config['schema']
        if schema != 'datamart':
            schema_name = schema.replace('_',' ')
        else:
            schema_name = 'datamart holistic'
        path = config['path']
        
        listOfTable = config['listOfTable']
        
        listOfDF = []
        
        for table in listOfTable:
            query_source = read.query(table,schema,0,path)
            query_datamart = read.query(table,schema,1,path)
            
            df_source = self.get_compare_df(query_source, conn_aws, 'query')
            df_datamart = self.get_compare_df(query_datamart, conn_aws, 'datamart')
            if df_datamart.empty == False:
                df = pd.concat([df_source, df_datamart],axis=1)
                df = self.get_condition_check(df)
                listOfDF.append(df)
        
        if listOfDF != []:
            df = pd.concat(listOfDF)
            df['total_source_query'] = df['total_source_query'].astype(str)
            df['total_source_datamart'] = df['total_source_datamart'].astype(str)
            
            gsheetId = config['datamart_validation_sheet']
            
            rangeName = '{0}!A1:AA1000'.format(schema_name)
            
            service = self.create_service(credential_path, 'sheets', 'v4', pickle_path)
            
            self.export_data_to_sheets(service,gsheetId,rangeName,df)
        else:
            print('No data in this schema')
        if verbose:
            print('process executed in ' + str(datetime.now() - start))