from airflow.models import DAG,Variable
from airflow.operators.python_operator import PythonOperator
from airflow.hooks.postgres_hook import PostgresHook
from airflow.hooks.mysql_hook import MySqlHook
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.bash_operator import BashOperator

import json
from datetime import datetime, timedelta

# import Logic each table
from dataEngineer.L0.etl_mysql_to_redshift.accommodation_service_production.task import execute_etl


CONFIG = Variable.get('config', deserialize_json=True)

def get_connection(**kwargs):
    '''
    get connection from admin airflow
    '''
    dictConfig = {}
    dictConfig['s3_bucket'] = CONFIG['s3_bucket']
    dictConfig['access_key'] = CONFIG['access_key']
    dictConfig['secret_key'] = CONFIG['secret_key']
    dictConfig['s3_key_file'] = 'etl/accommodation_service_production/de'
    
    conn_aws = PostgresHook(postgres_conn_id='aws_qoaladata_redshift')
    conn_mysql = MySqlHook(mysql_conn_id='mysql_main_accommodation_production_replica')
    
    return dictConfig,conn_aws, conn_mysql

# benefits table
def execute_benefits(ds,**kwargs):
    config,conn_aws,conn_mysql = get_connection()
    
    configTable = {}
    configTable['table'] = 'benefits'
    configTable['schema_db'] = 'accommodation_service_production'
    configTable['schema_s3'] = 'accommodation_service_production'
    configTable['column'] = 0
    
    pilot = execute_etl.etl_execute()
    pilot.main(ds,config,configTable,conn_aws, conn_mysql)
    
    return 'execute_etl task has done!'

# bookings table
def execute_bookings(ds,**kwargs):
    config,conn_aws,conn_mysql = get_connection()
    
    configTable = {}
    configTable['table'] = 'bookings'
    configTable['schema_db'] = 'accommodation_service_production'
    configTable['schema_s3'] = 'accommodation_service_production'
    configTable['column'] = 'created_at'
    
    pilot = execute_etl.etl_execute()
    pilot.main(ds,config,configTable,conn_aws, conn_mysql)
    
    return 'execute_etl task has done!'

# claim_histories table
def execute_claim_histories(ds,**kwargs):
    config,conn_aws,conn_mysql = get_connection()
    
    configTable = {}
    configTable['table'] = 'claim_histories'
    configTable['schema_db'] = 'accommodation_service_production'
    configTable['schema_s3'] = 'accommodation_service_production'
    configTable['column'] = 0
    
    pilot = execute_etl.etl_execute()
    pilot.main(ds,config,configTable,conn_aws, conn_mysql)
    
    return 'execute_etl task has done!'

# claim_payment_amounts table
def execute_claim_payment_amounts(ds,**kwargs):
    config,conn_aws,conn_mysql = get_connection()
    
    configTable = {}
    configTable['table'] = 'claim_payment_amounts'
    configTable['schema_db'] = 'accommodation_service_production'
    configTable['schema_s3'] = 'accommodation_service_production'
    configTable['column'] = 'updated_at'
    
    pilot = execute_etl.etl_execute()
    pilot.main(ds,config,configTable,conn_aws, conn_mysql)
    
    return 'execute_etl task has done!'

# claim_payment_histories table
def execute_claim_payment_histories(ds,**kwargs):
    config,conn_aws,conn_mysql = get_connection()
    
    configTable = {}
    configTable['table'] = 'claim_payment_histories'
    configTable['schema_db'] = 'accommodation_service_production'
    configTable['schema_s3'] = 'accommodation_service_production'
    configTable['column'] = 0
    
    pilot = execute_etl.etl_execute()
    pilot.main(ds,config,configTable,conn_aws, conn_mysql)
    
    return 'execute_etl task has done!'

# claims table
def execute_claims(ds,**kwargs):
    config,conn_aws,conn_mysql = get_connection()
    
    configTable = {}
    configTable['table'] = 'claims'
    configTable['schema_db'] = 'accommodation_service_production'
    configTable['schema_s3'] = 'accommodation_service_production'
    configTable['column'] = 'updated_at'
    
    pilot = execute_etl.etl_execute()
    pilot.main(ds,config,configTable,conn_aws, conn_mysql)
    
    return 'execute_etl task has done!'

# claims_covered_users table
def execute_claims_covered_users(ds,**kwargs):
    config,conn_aws,conn_mysql = get_connection()
    
    configTable = {}
    configTable['table'] = 'claims_covered_users'
    configTable['schema_db'] = 'accommodation_service_production'
    configTable['schema_s3'] = 'accommodation_service_production'
    configTable['column'] = 0
    
    pilot = execute_etl.etl_execute()
    pilot.main(ds,config,configTable,conn_aws, conn_mysql)
    
    return 'execute_etl task has done!'

# covered_users_table
def execute_covered_users(ds,**kwargs):
    config,conn_aws,conn_mysql = get_connection()
    
    configTable = {}
    configTable['table'] = 'covered_users'
    configTable['schema_db'] = 'accommodation_service_production'
    configTable['schema_s3'] = 'accommodation_service_production'
    configTable['column'] = 0
    
    pilot = execute_etl.etl_execute()
    pilot.main(ds,config,configTable,conn_aws, conn_mysql)
    
    return 'execute_etl task has done!'

# document_types table
def execute_document_types(ds,**kwargs):
    config,conn_aws,conn_mysql = get_connection()
    
    configTable = {}
    configTable['table'] = 'document_types'
    configTable['schema_db'] = 'accommodation_service_production'
    configTable['schema_s3'] = 'accommodation_service_production'
    configTable['column'] = 0
    
    pilot = execute_etl.etl_execute()
    pilot.main(ds,config,configTable,conn_aws, conn_mysql)
    
    return 'execute_etl task has done!'

# documents table
def execute_documents(ds,**kwargs):
    config,conn_aws,conn_mysql = get_connection()
    
    configTable = {}
    configTable['table'] = 'documents'
    configTable['schema_db'] = 'accommodation_service_production'
    configTable['schema_s3'] = 'accommodation_service_production'
    configTable['column'] = 'updated_at'
    
    pilot = execute_etl.etl_execute()
    pilot.main(ds,config,configTable,conn_aws, conn_mysql)
    
    return 'execute_etl task has done!'

# locations table
def execute_locations(ds,**kwargs):
    config,conn_aws,conn_mysql = get_connection()
    
    configTable = {}
    configTable['table'] = 'locations'
    configTable['schema_db'] = 'accommodation_service_production'
    configTable['schema_s3'] = 'accommodation_service_production'
    configTable['column'] = 0
    
    pilot = execute_etl.etl_execute()
    pilot.main(ds,config,configTable,conn_aws, conn_mysql)
    
    return 'execute_etl task has done!'

# policies table
def execute_policies(ds,**kwargs):
    config,conn_aws,conn_mysql = get_connection()
    
    configTable = {}
    configTable['table'] = 'policies'
    configTable['schema_db'] = 'accommodation_service_production'
    configTable['schema_s3'] = 'accommodation_service_production'
    configTable['column'] = 'created_at'
    
    pilot = execute_etl.etl_execute()
    pilot.main(ds,config,configTable,conn_aws, conn_mysql)
    
    return 'execute_etl task has done!'

# policies_data table
def execute_policies_data(ds,**kwargs):
    config,conn_aws,conn_mysql = get_connection()
    
    configTable = {}
    configTable['table'] = 'policies_data'
    configTable['schema_db'] = 'accommodation_service_production'
    configTable['schema_s3'] = 'accommodation_service_production'
    configTable['column'] = 0
    
    pilot = execute_etl.etl_execute()
    pilot.main(ds,config,configTable,conn_aws, conn_mysql)
    
    return 'execute_etl task has done!'

# policies_insurance table
def execute_policies_insurance(ds,**kwargs):
    config,conn_aws,conn_mysql = get_connection()
    
    configTable = {}
    configTable['table'] = 'policies_insurance'
    configTable['schema_db'] = 'accommodation_service_production'
    configTable['schema_s3'] = 'accommodation_service_production'
    configTable['column'] = 'created_at'
    
    pilot = execute_etl.etl_execute()
    pilot.main(ds,config,configTable,conn_aws, conn_mysql)
    
    return 'execute_etl task has done!'

# products table
def execute_products(ds,**kwargs):
    config,conn_aws,conn_mysql = get_connection()
    
    configTable = {}
    configTable['table'] = 'products'
    configTable['schema_db'] = 'accommodation_service_production'
    configTable['schema_s3'] = 'accommodation_service_production'
    configTable['column'] = 0
    
    pilot = execute_etl.etl_execute()
    pilot.main(ds,config,configTable,conn_aws, conn_mysql)
    
    return 'execute_etl task has done!'

# products_policies table
def execute_products_policies(ds,**kwargs):
    config,conn_aws,conn_mysql = get_connection()
    
    configTable = {}
    configTable['table'] = 'products_policies'
    configTable['schema_db'] = 'accommodation_service_production'
    configTable['schema_s3'] = 'accommodation_service_production'
    configTable['column'] = 0
    
    pilot = execute_etl.etl_execute()
    pilot.main(ds,config,configTable,conn_aws, conn_mysql)
    
    return 'execute_etl task has done!'

# user_policies table
def execute_user_policies(ds,**kwargs):
    config,conn_aws,conn_mysql = get_connection()
    
    configTable = {}
    configTable['table'] = 'user_policies'
    configTable['schema_db'] = 'accommodation_service_production'
    configTable['schema_s3'] = 'accommodation_service_production'
    configTable['column'] = 0
    
    pilot = execute_etl.etl_execute()
    pilot.main(ds,config,configTable,conn_aws, conn_mysql)
    
    return 'execute_etl task has done!'


def create_dag(dag_id,
                schedule,
                default_args,
                CONFIG):
    dag = DAG(dag_id, default_args=default_args, schedule_interval=schedule,max_active_runs=1)
        
    with dag:
        
        execute_benefits_etl = PythonOperator(task_id = 'execute_benefits', provide_context=True, python_callable = execute_benefits)
        
        execute_bookings_etl = PythonOperator(task_id = 'execute_bookings', provide_context=True, python_callable = execute_bookings)
        
        execute_claim_histories_etl = PythonOperator(task_id = 'execute_claim_histories', provide_context=True, python_callable = execute_claim_histories)
        
        execute_claim_payment_amounts_etl = PythonOperator(task_id = 'execute_claim_payment_amounts', provide_context=True, python_callable = execute_claim_payment_amounts)
        
        execute_claim_payment_histories_etl = PythonOperator(task_id = 'execute_claim_payment_histories', provide_context=True, python_callable = execute_claim_payment_histories)
        
        execute_claims_etl = PythonOperator(task_id = 'execute_claims', provide_context=True, python_callable = execute_claims)
        
        execute_claims_covered_users_etl = PythonOperator(task_id = 'execute_claims_covered_users', provide_context=True, python_callable = execute_claims_covered_users)
        
        execute_covered_users_etl = PythonOperator(task_id = 'execute_covered_users', provide_context=True, python_callable = execute_covered_users)
        
        execute_document_types_etl = PythonOperator(task_id = 'execute_document_types', provide_context=True, python_callable = execute_document_types)
        
        execute_documents_etl = PythonOperator(task_id = 'execute_documents', provide_context=True, python_callable = execute_documents)
        
        execute_locations_etl = PythonOperator(task_id = 'execute_locations', provide_context=True, python_callable = execute_locations)
        
        execute_policies_etl = PythonOperator(task_id = 'execute_policies', provide_context=True, python_callable = execute_policies)
        
        execute_policies_data_etl = PythonOperator(task_id = 'execute_policies_data', provide_context=True, python_callable = execute_policies_data)
        
        execute_policies_insurance_etl = PythonOperator(task_id = 'execute_policies_insurance', provide_context=True, python_callable = execute_policies_insurance)
        
        execute_products_etl = PythonOperator(task_id = 'execute_products', provide_context=True, python_callable = execute_products)
        
        execute_products_policies_etl = PythonOperator(task_id = 'execute_products_policies', provide_context=True, python_callable = execute_products_policies)
        
        execute_user_policies_etl = PythonOperator(task_id = 'execute_user_policies', provide_context=True, python_callable = execute_user_policies)
        
        start = DummyOperator(task_id = 'start_task')
        
        triggerDatamart = "airflow trigger_dag -e {{ ts }}  etl_datamart_acommodation"
        trigger_command = BashOperator(
            task_id='trigger_command',
            bash_command=triggerDatamart,
            dag=dag
        )
        
        start >> [execute_benefits_etl, execute_bookings_etl, execute_user_policies_etl,
                  execute_claim_histories_etl, execute_claim_payment_amounts_etl, execute_claim_payment_histories_etl,
                  execute_claims_etl, execute_claims_covered_users_etl, execute_covered_users_etl,
                  execute_document_types_etl, execute_documents_etl, execute_locations_etl, execute_policies_etl, execute_policies_data_etl,
                  execute_policies_insurance_etl, execute_products_etl, execute_products_policies_etl] >> trigger_command
        
        return dag


schedule = '35 */6 * * *'
dag_id = 'etl_accommodation_service_production'
args = {
    'owner': CONFIG['username'],
    'depends_on_past': False,
    'start_date': datetime.strptime("2020-07-08", '%Y-%m-%d'),
    'email': CONFIG['email'],
    'email_on_failure': False,
    'email_on_retry': False,
    'conccurency':1,
    'retries': 2,
    'retry_delay': timedelta(minutes=5)
}
globals()[dag_id] = create_dag(dag_id, schedule, args, CONFIG)