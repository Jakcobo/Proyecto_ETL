#/home/nicolas/Escritorio/proyecto ETL/develop/airflow/dags/dag.py

import os
import sys
from datetime import timedelta, datetime
from airflow.decorators import dag, task
import pandas as pd

# Agregar path raíz del proyecto
BASE_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
if BASE_DIR not in sys.path:
    sys.path.append(BASE_DIR)

# Importar las funciones de tarea refactorizadas
from task_etl import (
    extract_data,
    clean_data,
    load_cleaned_data,
    create_dimensional_model,
    insert_data_to_model,
    api_extract_task_logic,
    api_transform_task_logic,
    api_load_task_logic
)

# Argumentos por defecto
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
    description='ETL pipeline: Clean, Create Dim Model, Load Dim Model for Airbnb data.',
    schedule_interval=timedelta(days=1),
    max_active_runs=1,
    catchup=False,
    tags=['etl', 'airbnb', 'postgres', 'refactored', 'dimensional']
)
def etl_pipeline():

    @task()
    def extract_data_task() -> pd.DataFrame:
        return extract_data()

    @task()
    def clean_data_task(df_input: pd.DataFrame) -> pd.DataFrame:
        return clean_data(df_input)

    @task()
    def load_cleaned_data_task(df_final: pd.DataFrame):
        load_cleaned_data(df_final)

    @task()
    def create_dimensional_model_task():
        create_dimensional_model()

    @task()
    def insert_data_to_model_task():
        insert_data_to_model()

    @task()
    def extract_api_task() -> pd.DataFrame:
        return api_extract_task_logic()

    @task()
    def transform_api_task(df_input: pd.DataFrame) -> pd.DataFrame:
        return api_transform_task_logic(df_input)

    @task()
    def load_api_task(df_to_load: pd.DataFrame):
        api_load_task_logic(df_to_load)

    # --- Definir flujo de tareas principal ---
    df_extracted = extract_data_task()
    df_cleaned = clean_data_task(df_extracted)
    load_result = load_cleaned_data_task(df_cleaned)

    create_model = create_dimensional_model_task()
    insert_model = insert_data_to_model_task()

    load_result >> create_model >> insert_model

    # --- Flujo de tareas API ---
    df_api_extracted = extract_api_task()
    df_api_transformed = transform_api_task(df_api_extracted)
    load_api_task(df_api_transformed)

# Instanciar DAG
etl_instance = etl_pipeline()
