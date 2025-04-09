# /home/nicolas/Escritorio/proyecto/otra_prueba/airflow/dags/dag.py
from datetime import timedelta, datetime
from airflow.decorators import dag, task
import pandas as pd

# Importar las funciones de tarea refactorizadas
from task_etl import (
    extract_data,
    clean_data,
    load_cleaned_data,
    create_dimensional_model,
    insert_data_to_model # --- NUEVA IMPORTACIÓN ---
)

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 4, 8),
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

@dag(
    dag_id="ETL_Airbnb_Refactored",
    default_args=default_args,
    description='ETL pipeline: Clean, Create Dim Model, Load Dim Model for Airbnb data.', # Descripción actualizada
    schedule=timedelta(days=1),
    max_active_runs=1,
    catchup=False,
    tags=['etl', 'airbnb', 'postgres', 'refactored', 'dimensional']
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
        """Task: Loads the cleaned DataFrame into the final staging/cleaned table."""
        load_success = load_cleaned_data(df_final)
        if not load_success:
             raise ValueError("Loading cleaned data failed.")

    @task
    def create_model_task():
        """Task: Creates the dimensional model tables if they don't exist."""
        model_created = create_dimensional_model()
        if not model_created:
            raise ValueError("Dimensional model creation failed.")

    # --- NUEVA TAREA ---
    @task
    def insert_data_to_model_task():
        """Task: Inserts data from cleaned table into the dimensional model tables."""
        insert_success = insert_data_to_model()
        if not insert_success:
            raise ValueError("Insertion into dimensional model failed.")
    # --- FIN NUEVA TAREA ---

    # --- Definir el flujo del DAG ---
    extracted_data = extract_data_task()
    cleaned_data = clean_data_task(extracted_data)
    load_result = load_cleaned_data_task(cleaned_data)
    model_creation_result = create_model_task()
    # --- NUEVA DEPENDENCIA ---
    # La inserción de datos en el modelo depende de que las tablas del modelo se hayan creado
    insert_result = insert_data_to_model_task()

    load_result >> model_creation_result # Crear modelo después de cargar datos limpios
    model_creation_result >> insert_result # Insertar en modelo después de crearlo
    # --- FIN NUEVA DEPENDENCIA ---


# Instanciar el DAG
etl_instance_refactored = etl_dag_refactored()