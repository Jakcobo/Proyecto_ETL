# proyecto_etl/airflow/dags/dag.py
from datetime import timedelta, datetime
from airflow.decorators import dag, task
import pandas as pd

# Importar las funciones de tarea refactorizadas
from task_etl import (
    extract_data
    ,clean_data
    ,load_cleaned_data
    ,create_dimensional_model
    ,insert_data_to_model # --- NUEVA IMPORTACIÓN ---
    ,api_extract_task_logic
    ,api_transform_task_logic
    ,api_load_task_logic
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
    description='ETL pipeline: Clean, Create Dim Model, Load Dim Model for Airbnb data.', # Descripción actualizada
    schedule=timedelta(days=1),
    max_active_runs=1,
    catchup=False,
    tags=['etl', 'airbnb', 'postgres', 'refactored', 'dimensional']
)
def etl_dag():

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

    @task(task_id="extract_api_csv_data")
    def extract_api_data() -> pd.DataFrame: # Type hint para XComs
        """Task: Extracts api data from CSV and returns a DataFrame."""
        df_extracted = api_extract_task_logic() # Llama a la función de task_etl.py
        # Si df_extracted es None o vacío, las tareas siguientes deberían manejarlo.
        if df_extracted.empty:
            raise ValueError("La extracción de api no produjo datos.")
        return df_extracted

    @task(task_id="transform_api_data")
    def transform_api_data(df_input: pd.DataFrame) -> pd.DataFrame:
        """Task: Transforms the extracted api data."""
        if not isinstance(df_input, pd.DataFrame) or df_input.empty:
            raise ValueError("No hay datos para transformar o el input no es un DataFrame. Saltando transformación.")
            return pd.DataFrame() # Devuelve un DataFrame vacío para que la carga no falle si no hay datos
        
        df_transformed = api_transform_task_logic(df_input)
        if df_transformed.empty and not df_input.empty:
            raise ValueError("La transformación de api resultó en un DataFrame vacío desde datos no vacíos.")
        return df_transformed

    @task(task_id="load_api_data_to_db")
    def load_api_data(df_to_load: pd.DataFrame):
        """Task: Loads the transformed api data into the database."""
        if not isinstance(df_to_load, pd.DataFrame) or df_to_load.empty:
            raise ValueError("No hay datos transformados para cargar en la base de datos. Tarea de carga completada sin acción.")
            return # Termina sin error si no hay nada que cargar
        
        api_load_task_logic(df_to_load)

    # Definir el flujo de tareas
    extracted_df = extract_api_data()
    transformed_df = transform_api_data(extracted_df)
    # init_db_task = init_database() # Si la usas
    #init_db_task >> extracted_df # Ejemplo de dependencia
    
    load_api_data(transformed_df)
    # Si usaras la tarea init_db y quisieras que se ejecute antes de todo:
    extracted_df >> transformed_df >> load_api_data(transformed_df)

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
etl_instance = etl_dag()