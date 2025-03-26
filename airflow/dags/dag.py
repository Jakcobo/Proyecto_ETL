from airflow import DAG
from datetime import timedelta, datetime
from airflow.operators.dummy_operator import DummyOperator
from airflow.decorators import dag, task

from task_etl import *

#from database.db_operations import creating_engine, load_clean_data
#from src.extract.extract_data import exe_extract_data
#from extract.extract_api import extracting_api_data
#from transform.transform_DWH import transforming_into_DWH
#from transform.transform_api import transforming_api_data
#from transform.merge import merging_data
#from load.load_DWH import loading_data

# Step 2: Initiating the default_args
default_args = {
    'owner' : 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2025, 3, 25),
    'retries': 1,
    'retry_delay': timedelta(minutes=120)
}

@dag(
    default_args=default_args,
    description='Creating an ETL pipeline task.',
    schedule=timedelta(days=1),
    max_active_runs=1,
    catchup=False
)

def dag():
    @task
    def extract_data_task():
        return extract_data()


etl_tasks = dag()

"""dag = DAG(dag_id='DAG-1',
        default_args=default_args,
        schedule_interval='@once', 
        catchup=False
    )

start = DummyOperator(task_id = 'start', dag = dag)
end = DummyOperator(task_id = 'end', dag = dag)

 # Step 5: Setting up dependencies 
start >> end"""