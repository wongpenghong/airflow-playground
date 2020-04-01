from airflow.models import DAG
from airflow.operators.python_operator import PythonOperator

import json,logging,os
from datetime import datetime, timedelta

from mysql_to_redshift.task import execute_script
from mysql_to_redshift.task import messageSlack


PATH_AIRFLOW = os.environ['AIRFLOW_HOME']
PATH = "{path}/dags/mysql_to_redshift".format(path=PATH_AIRFLOW)

def execute_etl(ds,**kwargs):
        print(ds)
        pilot = execute_script.etl_execute()
        pilot.main(ds)
        
        return 'execute_etl task has done!'
    
def send_slack(ds, **kwargs):
    messageSlack.sendMessage()

    return 'success!'

def create_dag(dag_id,
                schedule,
                default_args,
                CONFIG):
    dag = DAG(dag_id, default_args=default_args, schedule_interval=schedule,max_active_runs=1)
        
    with dag:
        
        
        execute = PythonOperator(task_id = 'execute_script', provide_context=True, python_callable = execute_etl)
        
        execute
        
        return dag

with open("{path}/config/conf.json".format(path=PATH), "r") as json_data:
    CONFIG = json.load(json_data)['config']
    schedule = CONFIG['schedule']
    dag_id = CONFIG['dag_id']
    args = {
        'owner': CONFIG['owner'],
        'depends_on_past': False,
        'start_date': datetime.strptime(CONFIG['start_date'], '%Y-%m-%d'),
        'email': CONFIG['email'],
        'email_on_failure': True,
        'email_on_retry': False,
        'on_failure_callback': send_slack,
        'conccurency':1
    }
    globals()[dag_id] = create_dag(dag_id, schedule, args, CONFIG)