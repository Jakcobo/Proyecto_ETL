#from airflow import DAG
from datetime import timedelta, datetime
#from airflow.operators.dummy_operator import DummyOperator
from airflow.decorators import dag, task

from task_etl import *

default_args = {
    'owner' : 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 4, 8), 
    'retries': 1,
    'retry_delay': timedelta(minutes=15)
}

@dag(
    dag_id="ETL_Airbnb",
    default_args=default_args,
    description='ETL pipeline task for Airbnb data in Airflow.',
    schedule=timedelta(days=1),
    max_active_runs=1,
    catchup=False,
    tags=['etl', 'airbnb', 'postgres']
)

def etl_dag():
    @task
    def extract_data_task():
        """Extracts data from the source CSV."""
        return extract_data()
    
    @task
    def load_data_task(df_csv):
        """Loads the extrated DataFrame into the staging table, replacing it if exists"""
        return load_data(df_csv)
    
    @task
    def clean_data_task():
        """Cleans the data in the staging table"""
        return clean_data()
    
    # @task
    # def migrate_to_dimensional_model_task():
    #     return migrate_to_dimensional_model()


    extrated_data = extract_data_task()
    load_result = load_data_task(extrated_data)
    clean_data_task_instance = clean_data_task()
    load_result >> clean_data_task_instance
    
    # migrate_to_dimensional_model_task()

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
