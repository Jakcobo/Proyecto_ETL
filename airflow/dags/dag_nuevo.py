# /home/nicolas/Escritorio/proyecto/otra_prueba/airflow/dags/dag.py
from datetime import timedelta, datetime
from airflow.decorators import dag, task
import pandas as pd # Importar pandas si se usa para type hinting

# Importar las funciones de tarea refactorizadas
from task_etl import extract_data, clean_data, load_cleaned_data
# from task_etl import migrate_to_dimensional_model # Si se usa

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 4, 8), # Ajusta la fecha de inicio si es necesario
    'retries': 1,
    'retry_delay': timedelta(minutes=5) # Reducir el delay para pruebas si quieres
}

@dag(
    dag_id="ETL_Airbnb_Refactored", # Nuevo ID para no colisionar con el antiguo
    default_args=default_args,
    description='Refactored ETL pipeline with DataFrame cleaning for Airbnb data.',
    schedule=timedelta(days=1), # O None si es manual, o '@daily'
    max_active_runs=1,
    catchup=False,
    tags=['etl', 'airbnb', 'postgres', 'refactored']
)
def etl_dag_refactored():

    @task
    def extract_data_task() -> pd.DataFrame: # Añadir type hint ayuda a la claridad
        """Task: Extracts data from the source and returns a DataFrame."""
        # Llama a la función importada que hace el trabajo real
        df_extracted = extract_data()
        return df_extracted

    @task
    def clean_data_task(df_input: pd.DataFrame) -> pd.DataFrame:
        """Task: Cleans the DataFrame received from the previous task."""
        # Llama a la función importada que hace el trabajo real
        df_cleaned = clean_data(df_input)
        return df_cleaned

    @task
    def load_cleaned_data_task(df_final: pd.DataFrame):
        """Task: Loads the cleaned DataFrame into the final database table."""
        # Llama a la función importada que hace el trabajo real
        load_success = load_cleaned_data(df_final)
        # Podrías añadir lógica aquí basada en load_success si es necesario
        if not load_success:
             raise ValueError("Loading cleaned data failed.") # Hacer que la tarea falle si la carga no fue exitosa

    # @task
    # def migrate_to_dimensional_model_task(): # Mantener si se usa después
    #     """Task: Migrates data from cleaned table to dimensional model."""
    #     # Esta tarea dependería de load_cleaned_data_task
    #     return migrate_to_dimensional_model()

    # --- Definir el flujo del DAG usando TaskFlow API ---
    extracted_data = extract_data_task()
    cleaned_data = clean_data_task(extracted_data)
    load_result = load_cleaned_data_task(cleaned_data)

    # Si tuvieras la migración dimensional, la encadenarías así:
    # migration_result = migrate_to_dimensional_model_task()
    # load_result >> migration_result

# Instanciar el DAG para que Airflow lo reconozca
etl_instance_refactored = etl_dag_refactored()