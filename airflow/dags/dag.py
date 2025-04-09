from datetime import timedelta, datetime
from airflow.decorators import dag, task
import pandas as pd
from task_etl import (
    extract_data,
    clean_data,
    load_cleaned_data,
    create_dimensional_model,
    migrate_to_dimensional_model
)

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 4, 8),
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

@dag(
    dag_id="ETL_Airbnb",
    default_args=default_args,
    description='Refactored ETL pipeline with DataFrame cleaning for Airbnb data.',
    schedule=timedelta(days=1),
    max_active_runs=1,
    catchup=False,
    tags=['etl', 'airbnb', 'postgres', 'refactored']
)
def etl_dag_refactored():

    @task
    def extract_data_task() -> pd.DataFrame:
        """Task: Extracts data from the source and returns a DataFrame."""
        df_extracted = extract_data()
        return df_extracted

    @task
    def clean_data_task(df_input: pd.DataFrame) -> pd.DataFrame:
        """Task: Cleans the DataFrame received from the previous task."""
        df_cleaned = clean_data(df_input)
        return df_cleaned

    @task
    def load_cleaned_data_task(df_final: pd.DataFrame):
        """Task: Loads the cleaned DataFrame into the final database table."""
        load_success = load_cleaned_data(df_final)
        if not load_success:
            raise ValueError("Loading cleaned data failed.") # Hacer que la tarea falle si la carga no fue exitosa

    @task
    def create_model_task():
        """Task: Creates the dimensional model tables if they don't exist."""
        model_created = create_dimensional_model() # Llama a la función lógica
        if not model_created:
            raise ValueError("Dimensional model creation failed.")
    
    
    @task
    def load_dimensional_model_task(df_final: pd.DataFrame): 
        """Task: Migrates data from cleaned table to dimensional model."""
        model_success = migrate_to_dimensional_model(df_final)
        if not model_success:
            raise ValueError("Loading dimensional model failed")

    extracted_data = extract_data_task()
    cleaned_data = clean_data_task(extracted_data)
    load_result = load_cleaned_data_task(cleaned_data)

    model_creation_result = create_model_task()
    dimensional_model_result = load_dimensional_model_task(cleaned_data)
    
    extracted_data >> model_creation_result
    model_creation_result >> dimensional_model_result

etl_instance_refactored = etl_dag_refactored()