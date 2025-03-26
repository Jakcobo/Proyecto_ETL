#from airflow import DAG
from datetime import timedelta, datetime
#from airflow.operators.dummy_operator import DummyOperator
from airflow.decorators import dag, task

from task_etl import *

default_args = {
    'owner' : 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2025, 3, 25),
    'retries': 1,
    'retry_delay': timedelta(minutes=120)
}

@dag(
    dag_id="ETL",
    default_args=default_args,
    description='ETL pipeline task in Airflow.',
    schedule=timedelta(days=1),
    max_active_runs=1,
    catchup=False
)

def etl_dag():
    @task
    def extract_data_task():
        return extract_data()
    
    #data = extract_data_task()
    extract_data_task()

etl_instance = etl_dag()

"""dag = DAG(dag_id='DAG-1',
        default_args=default_args,
        schedule_interval='@once', 
        catchup=False
    )

start = DummyOperator(task_id = 'start', dag = dag)
end = DummyOperator(task_id = 'end', dag = dag)

 # Step 5: Setting up dependencies 
start >> end"""