import pandas as pd
from datetime import datetime

from dataEngineer.L1.datamart.sql import checking_data
from dataEngineer.L1.datamart.task import execute_batch
from dataEngineer.L1.datamart.task import execute_dump

class execute:
    
    def check_total_data(self,query,conn):
        conn_aws = conn.get_conn()
        df = pd.io.sql.read_sql(query, conn_aws)
        
        return df['total'][0]
    
    def main(self,date,config,conn_aws,verbose=1):
        # First initial to know performance script
        if verbose:
            print('executing engine....')
            start = datetime.now()
        
        # Config
        table = config['table']
        path_dags = config['path']
        
        # checking data
        query_checking = checking_data.query(path_dags,table)
        checking = self.check_total_data(query_checking,conn_aws)
        
        if checking > 1000000:
            print('-------------enter batch etl---------------')
            pilot = execute_batch.etl_execute()
            pilot.main(date,config,conn_aws)
        elif checking < 1000000 and checking > 0:
            print('-------------enter dump etl----------------')
            pilot = execute_dump.etl_execute()
            pilot.main(date,config,conn_aws)
        else:
            print('NO DATA IN THIS TABLE!')
        
        if verbose:
            print('process executed in ' + str(datetime.now() - start))