# Proyecto_ETL/airflow/dags/dag.py
import os
import sys
from datetime import timedelta, datetime
from airflow.decorators import dag, task
from airflow.exceptions import AirflowFailException
from airflow.models import Variable
import pandas as pd

# --- Configuración de Rutas ---
PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
SRC_PATH = os.path.join(PROJECT_ROOT, "src")

if SRC_PATH not in sys.path:
    sys.path.append(SRC_PATH)
if PROJECT_ROOT not in sys.path:
    sys.path.append(PROJECT_ROOT)

# --- Importaciones de Módulos del Proyecto ---
from extract.api_extract import extract_api_data
from extract.airbnb_extract import extract_airbnb_data
from transform.airbnb_transform import clean_airbnb_data
from transform.api_transform import clean_api_data
from load.merge_operacional import merge_operacional
from database.create_dimensional import create_and_prepare_dimensional_model_data
from load.load_dimensional import load_dimensional_data
from load.data_merge import data_merge
from stream.kafka_producer import send_dataframe_to_kafka # Descomentar cuando kafka_producer.py esté listo

# --- Variables Globales / de Configuración ---
TARGET_DB_NAME = Variable.get("postgres_db_name", default_var="airbnb_etl_db") # Asegúrate que el default sea el correcto

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 5, 14),
    'retries': 1,
    'retry_delay': timedelta(minutes=2)
}

@dag(
    dag_id="ETL_Airbnb_Foursquare_Kafka", # ID Actualizado
    default_args=default_args,
    description='Pipeline ETL para datos de AirBnB y FourSquare, cargando a Postgres y enviando a Kafka.', # Descripción Actualizada
    schedule_interval=timedelta(days=1),
    max_active_runs=1,
    catchup=False,
    tags=['etl', 'airbnb', 'foursquare', 'postgres', 'kafka', 'v3'] # v3 para la nueva versión
)
def etl_pipeline_with_kafka():

    # --- Tareas de Extracción ---
    @task(task_id="1.1_extraccion_airbnb")
    def extraccion_airbnb_task() -> pd.DataFrame:
        raw_airbnb_df = extract_airbnb_data()
        if not isinstance(raw_airbnb_df, pd.DataFrame) or raw_airbnb_df.empty:
            raise AirflowFailException("extract_airbnb_data debe devolver un DataFrame no vacío.")
        return raw_airbnb_df

    @task(task_id="1.2_extraccion_api")
    def extraccion_api_task() -> pd.DataFrame:
        raw_api_df = extract_api_data()
        if not isinstance(raw_api_df, pd.DataFrame) or raw_api_df.empty:
            raise AirflowFailException("extract_api_data debe devolver un DataFrame no vacío.")
        return raw_api_df

    # --- Tareas de Transformación ---
    @task(task_id="2.1_transformacion_airbnb")
    def transformacion_airbnb_task(df_airbnb_raw: pd.DataFrame) -> pd.DataFrame:
        cleaned_airbnb_df = clean_airbnb_data(df_airbnb_raw)
        if not isinstance(cleaned_airbnb_df, pd.DataFrame) or cleaned_airbnb_df.empty:
            raise AirflowFailException("clean_airbnb_data debe devolver un DataFrame no vacío.")
        return cleaned_airbnb_df

    @task(task_id="2.2_transformacion_api")
    def transformacion_api_task(df_api_raw: pd.DataFrame) -> pd.DataFrame:
        cleaned_api_df = clean_api_data(df_api_raw)
        if not isinstance(cleaned_api_df, pd.DataFrame) or cleaned_api_df.empty:
            raise AirflowFailException("clean_api_data debe devolver un DataFrame no vacío.")
        return cleaned_api_df

    # --- Tareas de Merge ---
    @task(task_id="3.1_merge_operacional")
    def merge_operacional_task(df_airbnb: pd.DataFrame, df_tiendas: pd.DataFrame) -> pd.DataFrame:
        merged_df = merge_operacional(df_airbnb, df_tiendas)
        if not isinstance(merged_df, pd.DataFrame) or merged_df.empty: # Comprobar si está vacío también
            raise AirflowFailException("merge_operacional debe devolver un DataFrame no vacío.")
        return merged_df

    @task(task_id="3.2_data_merge")
    def data_merge_task(df_airbnb: pd.DataFrame, df_operations: pd.DataFrame) -> pd.DataFrame:
        df_data_merge = data_merge(df_airbnb, df_operations)
        if not isinstance(df_data_merge, pd.DataFrame) or df_data_merge.empty:
            raise AirflowFailException("data_merge debe devolver un DataFrame no vacío.")
        return df_data_merge

    # --- Tareas de Carga Dimensional (PostgreSQL) ---
    @task(task_id="4.1_preparar_modelo_dimensional")
    def preparar_modelo_dimensional_task(df_merged_final: pd.DataFrame) -> dict:
        if df_merged_final.empty:
            raise AirflowFailException("El DataFrame df_merged_final está vacío. No se puede preparar el modelo dimensional.")
        prepared_data = create_and_prepare_dimensional_model_data(df_merged_final, db_name=TARGET_DB_NAME)
        return prepared_data

    @task(task_id="4.2_cargar_modelo_dimensional")
    def cargar_modelo_dimensional_task(prepared_data_dictionary: dict):
        if not prepared_data_dictionary or not any(
            isinstance(df, pd.DataFrame) and not df.empty for df in prepared_data_dictionary.values()
        ):
            raise AirflowFailException("El diccionario de datos preparados está vacío o todos los DataFrames están vacíos. No se carga nada.")
        
        # El argumento load_order en tu función load_dimensional_data es ignorado,
        # pero se mantiene aquí por si la función interna cambia en el futuro.
        custom_load_order = [
            "dim_host", "dim_property_location", "dim_property", "dim_date", "fact_publication"
        ]
        success = load_dimensional_data(prepared_data_dictionary, db_name=TARGET_DB_NAME, load_order=custom_load_order)
        if not success:
            raise AirflowFailException("La carga del modelo dimensional falló.")
        return "Carga dimensional a PostgreSQL completada."

    # --- Tarea de Productor Kafka ---
    @task(task_id="5_kafka_producer")
    def kafka_producer_task(df_to_produce: pd.DataFrame):
        if df_to_produce.empty:
            # logger.info("DataFrame para Kafka está vacío. No se envían datos.") # Usar logger de Airflow si prefieres
            print("DataFrame para Kafka está vacío. No se envían datos.") # O print para logs de Airflow
            return "DataFrame para Kafka vacío. No se enviaron datos."

        # --- Conversión de tipos de Pandas para serialización JSON ---
        df_copy = df_to_produce.copy()
        for col in df_copy.columns:
            if pd.api.types.is_datetime64_any_dtype(df_copy[col].dtype):
                df_copy[col] = df_copy[col].apply(lambda x: x.isoformat() if pd.notna(x) else None)
            elif hasattr(df_copy[col].dtype, 'na_value') and df_copy[col].hasnans:
                df_copy[col] = df_copy[col].astype(object).where(pd.notna(df_copy[col]), None)
        
        # --- Configuración de Kafka ---
        try:
            # Es buena práctica obtener estas variables de Airflow Variables
            topic_name = Variable.get("kafka_topic_name", default_var="airbnb_publications_enriched")
            bootstrap_servers = Variable.get("kafka_bootstrap_servers", default_var="localhost:29092")
            # Opcional: definir una columna clave para los mensajes
            key_column_name = "publication_key" # O None si no quieres clave
        except KeyError as e:
            # logger.error(f"Variable de Airflow no encontrada: {e}") # Usar logger de Airflow
            print(f"Variable de Airflow no encontrada: {e}")
            raise AirflowFailException(f"Variable de Airflow no encontrada: {e}. Asegúrate de definir 'kafka_topic_name' y 'kafka_bootstrap_servers'.")

        print(f"Intentando enviar {len(df_copy)} filas al topic '{topic_name}' en '{bootstrap_servers}'...")

        success, sent_count, failed_count = send_dataframe_to_kafka(
            df=df_copy, 
            topic_name=topic_name,
            bootstrap_servers=bootstrap_servers,
            key_column=key_column_name # Pasa la clave si la quieres
        )
        
        if not success or failed_count > 0 : # Fallar si no todo fue exitoso o si hubo fallos
            # logger.error(f"Falló el envío de {failed_count} de {len(df_copy)} mensajes a Kafka.") # Usar logger de Airflow
            print(f"Falló el envío de {failed_count} de {len(df_copy)} mensajes a Kafka.")
            raise AirflowFailException(f"Falló el envío de {failed_count} de {len(df_copy)} mensajes a Kafka.")
        
        # logger.info(f"{sent_count} de {len(df_copy)} mensajes enviados a Kafka topic '{topic_name}'. Fallidos: {failed_count}.") # Usar logger
        print(f"{sent_count} de {len(df_copy)} mensajes enviados a Kafka topic '{topic_name}'. Fallidos: {failed_count}.")
        return f"{sent_count} de {len(df_copy)} mensajes enviados a Kafka topic '{topic_name}'. Fallidos: {failed_count}."

    # --- Definición del Flujo del DAG ---
    df_airbnb_raw_output = extraccion_airbnb_task()
    df_api_raw_output = extraccion_api_task()

    df_airbnb_clean_output = transformacion_airbnb_task(df_airbnb_raw_output)
    df_api_clean_output = transformacion_api_task(df_api_raw_output)

    df_merged_operacional_output = merge_operacional_task(
        df_airbnb=df_airbnb_clean_output,
        df_tiendas=df_api_clean_output
    )

    df_final_merged_output = data_merge_task(
        df_airbnb=df_airbnb_clean_output, # Usar el df_airbnb limpio, no el raw
        df_operations=df_merged_operacional_output
    )

    # Rama para la carga dimensional
    prepared_dimensional_data_output = preparar_modelo_dimensional_task(
        df_merged_final=df_final_merged_output
    )
    carga_dimensional_db_result = cargar_modelo_dimensional_task( # Renombrado para claridad
        prepared_data_dictionary=prepared_dimensional_data_output
    )

    # Rama para el productor Kafka (se ejecuta después del merge final, en paralelo con la preparación dimensional)
    kafka_producer_result = kafka_producer_task(
        df_to_produce=df_final_merged_output
    )
    
    # Definir dependencias explícitas si es necesario, aunque el TaskFlow API suele inferirlas.
    # En este caso, kafka_producer_task y preparar_modelo_dimensional_task dependen de df_final_merged_output.
    # cargar_modelo_dimensional_task depende de prepared_dimensional_data_output.
    # El flujo definido arriba debería ser suficiente.

etl_dag_instance = etl_pipeline_with_kafka()