from airflow.models import DAG,Variable
from airflow.operators.python_operator import PythonOperator
from airflow.hooks.postgres_hook import PostgresHook

import json
from datetime import datetime

# import Logic each table
from dataEngineer.L1.datamart.task import checking
from dataEngineer.L1.validation import validation_datamart
from dataEngineer.externalPlugins.alerting import failed_alert


CONFIG = Variable.get('config', deserialize_json=True)
CONFIG_DATAMART = Variable.get('datamart', deserialize_json=True)
CONFIG_SLACK = Variable.get('config_slack', deserialize_json=True)

path = CONFIG_DATAMART['path_datamart']
dags_name = 'etl_datamart'

def failed_alert_slack(dags_name,**kwargs):
    token = CONFIG_SLACK['token_slack']
    channel = 'U011M862TDE'
    failed_alert.execute(token,dags_name['task_instance_key_str'],channel)
    
    return dags_name

def get_connection(**kwargs):
    '''
    get connection from admin airflow
    '''
    dictConfig = {}
    dictConfig['s3_bucket'] = CONFIG['s3_bucket']
    dictConfig['access_key'] = CONFIG['access_key']
    dictConfig['secret_key'] = CONFIG['secret_key']
    dictConfig['s3_key_file'] = 'etl/datamart'
    conn_aws = PostgresHook(postgres_conn_id='aws_qoaladata_redshift')
    
    return dictConfig,conn_aws
   
# datamart table
def execute_datamart(ds,**kwargs):
    with open("{}/config/datamart.json".format(path), "r") as read_file:
	    CONFIG_TEMP = json.load(read_file)[kwargs['table']]

    config,conn_aws = get_connection()
    
    config['schema'] = CONFIG_TEMP['schema']
    config['table'] = CONFIG_TEMP['table']
    config['column'] = CONFIG_TEMP['column']
    config['path'] = path
    
    pilot = checking.execute()
    pilot.main(ds,config,conn_aws)
    
    return 'execute_etl task has done!'

# validation
def validation(**kwargs):
    config,conn_aws = get_connection()
    
    config['schema'] = 'datamart'
    config['credential_gsheet_path'] = CONFIG_DATAMART['credential_gsheet_path']
    config['pickle_gsheet_path'] = CONFIG_DATAMART['pickle_gsheet_path']
    config['listOfTable'] = CONFIG_DATAMART['listOfTabledatamart']
    config['datamart_validation_sheet'] = CONFIG_DATAMART['datamart_validation_sheet']
    config['path'] = path
    
    pilot = validation_datamart.etl_execute()
    pilot.main(conn_aws,config)
    
    return 'execute_etl task has done!'

def create_dag(dag_id,
                schedule,
                default_args,
                CONFIG):
    dag = DAG(dag_id, default_args=default_args, schedule_interval=schedule,max_active_runs=1)
        
    with dag:
        
        execute_validation_datamart = PythonOperator(task_id = 'execute_validation', provide_context=True, python_callable = validation)   
        
        for table in CONFIG_DATAMART['listOfTable']:
            
            execute = PythonOperator(task_id = f'execute_{table}', provide_context=True, python_callable = execute_datamart, op_kwargs={'table' : table})   
        
            execute >> execute_validation_datamart
        
        return dag


schedule = None
dag_id = dags_name
args = {
    'owner': CONFIG['username'],
    'depends_on_past': False,
    'start_date': datetime.strptime("2020-10-13", '%Y-%m-%d'),
    'email': CONFIG['email'],
    'email_on_failure': False,
    'email_on_retry': False,
    'on_failure_callback': failed_alert_slack,
    'conccurency':1
}
globals()[dag_id] = create_dag(dag_id, schedule, args, CONFIG)