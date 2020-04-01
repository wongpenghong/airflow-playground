import pandas as pd
import os,psutil,json,sqlalchemy,psycopg2
from datetime import datetime
from datetime import date
from dateutil import parser
import numpy as np

from mysql_to_redshift.sql import querys

PATH_AIRFLOW = os.environ['AIRFLOW_HOME']
PATH = "{path}/dags/mysql_to_redshift".format(path=PATH_AIRFLOW)

with open("{path}/config/conf.json".format(path=PATH), "r") as read_file:
    CONFIG = json.load(read_file)['config']

class etl_execute:
    def __init__(self):
        self.database = CONFIG['database_mysql']
        self.user = CONFIG['username_mysql']
        self.password = CONFIG['password_mysql']
        self.host = CONFIG['host_mysql']
        self.port = CONFIG['port_mysql']
        self.database_redshift = CONFIG['database_redshift']
        self.user_redshift = CONFIG['username_redshift']
        self.password_redshift = CONFIG['password_redshift']
        self.host_redshift = CONFIG['host_redshift']
        self.port_redshift = CONFIG['port_redshift']
    
    def _build_connection_mysql(self):
        """
        Create Connection For MySQL
        """
        database_connection = sqlalchemy.create_engine('mysql+mysqlconnector://{username}:{password}@{host}:{port}/{database}'.format(
            username=self.user,
            password=self.password, 
            host=self.host,
            port=self.port,
            database=self.database, echo=False))
        dbConnection    = database_connection.connect()
        return dbConnection
    
    def conn_redshift(self):
        """
        Create Connection For redshift
        """
        
        connection_format = 'redshift+psycopg2://{username}:{password}@{host}:{port}/{database}'.format(
            username=self.user_redshift,
            password=self.password_redshift, 
            host=self.host_redshift,
            port=self.port_redshift,
            database=self.database_redshift, echo=False)
        engine = sqlalchemy.create_engine(connection_format)
        connection = engine.connect()
        
        return connection
    
    def get_df(self,data):
        conn = self.conn_redshift()
        
        query = (
            """
                <----- your query to get unique id ------->
            """.format(data=data)
        )
        df = pd.read_sql_query(query, conn)
        
        return df
    
    def get_date(self,date):
        # time condition
        datetimes = parser.parse(date)
        date = datetimes.strftime('%Y-%m-%d')
        
        return date
    
    def transform_data_date_null(self,data,column):
        """
        Transform date
        """
        for i in column : 
            if i == 'created_date':
                data[i] = pd.to_datetime(data[i], format='%Y-%m-%d %H:%M:%S')
                # data[i] = data[i].dt.strftime('%Y-%m-%d %H:%M:%S')
                data[i].loc[data[i] == 'NaT'] = None
            elif i == 'updated_date' :
                data[i] = pd.to_datetime(data[i], format='%Y-%m-%d %H:%M:%S')
                # data[i] = data[i].dt.strftime('%Y-%m-%d %H:%M:%S')
                data[i].loc[data[i] == 'NaT'] = None
        return data
   
    def fetch_data(self,query): 
        conn = self._build_connection_mysql()
            
        # Create dataFrame from Query
        df = pd.read_sql_query(query, conn)
        last_df = self.transform_data_date_null(df,df.columns)
        
        return last_df
    
    def transform_df(self,df,table,date):
        date = datetime.strptime(date, '%Y-%m-%d').date()
        if(df.empty == False):
            if(table == 'your_table'):
                df['sent_time'] = date
        
        return df
            
    def load_to_redshift(self,table,df,condition):
        conn = self.conn_redshift()
        
        if df.empty == False:
            if condition == 1 :
                df.to_sql(table, conn, index=False, if_exists='append')
            elif condition == 2 :
                df.to_sql(table, conn, index=False, if_exists='replace')
                
            print('--------->>>>>> success load data to {table}'.format(table=table))
            return True
        
        else:
            print('--------------->>>>>>>>>No data {table} to Load!'.format(table=table))
            
            return False
        
    def splitDataFrameIntoSmaller(self,df, chunkSize = 10000): 
        listOfDf = list()
        numberChunks = len(df) // chunkSize + 1
        for i in range(numberChunks):
            listOfDf.append(df[i*chunkSize:(i+1)*chunkSize])
        return listOfDf
            
    def main(self,date,verbose=1):
        # First initial to know performance script
        if verbose:
            print('executing engine....')
            start = datetime.now()
            
        conn_redshift = self.conn_redshift()
            
        date = self.get_date(date)
        
        query = querys.query(date)
        print(query)
        
        # etl logistic
        data_pricing_new = self.fetch_data(query)
        df = self.transform_df(data_pricing_new, 'your_table',date)
        
        list_id = tuple(x for x in df.pricing_id.values)
        data_df = self.get_df(list_id)
        user_id_temp = data_df.pricing_id
        
        if df.empty == False:
            sizeMemory = df.memory_usage(deep=True).sum()
            print('size of memory usage for this Dataframe : {size} Byte'.format(size=sizeMemory))
            #Split DF
            # Exist
            exist_df = df[df.pricing_id.isin(user_id_temp)]
            
            # New
            new_df = df[~df.pricing_id.isin(user_id_temp)]
            print('-------->>>> success split DF')
            
            # Load DF to Redshift
            self.load_to_redshift('your_destination_table',new_df,1)
            temp_data = self.load_to_redshift('your_destination_table_temp',exist_df,2)
            
            if temp_data == True:
                
                # merge data Redshift
                query_update = """
                <---------- your query to update data ------->
                """
                conn_redshift.execute(query_update)
                print('------------>>>> success update data')
                
                # delete data temp Redshift
                query_delete = """
                <---------- your query to delete data from temp destination table------->
                """.format(date=date)
                conn_redshift.execute(query_delete)
                print('------------>>>> success delete data in date "{date}" '.format(date=date))
            
        else:
            print('No Data!')
            
        process = psutil.Process(os.getpid())
        print('--------------->>>>>>>>>> memory usage for this compute : {memory} Byte'.format(memory=process.memory_info().rss))
   
        if verbose:
            print('query executed in ' + str(datetime.now() - start))
