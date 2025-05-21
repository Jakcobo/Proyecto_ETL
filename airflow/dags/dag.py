# Proyecto_ETL/airflow/dags/dag.py
import os
import sys
from datetime import timedelta, datetime
from airflow.decorators import dag, task
from airflow.exceptions import AirflowFailException
from airflow.models import Variable
import pandas as pd

PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
SRC_PATH = os.path.join(PROJECT_ROOT, "src")

if SRC_PATH not in sys.path:
    sys.path.append(SRC_PATH)
if PROJECT_ROOT not in sys.path:
    sys.path.append(PROJECT_ROOT)

from utils.ge_utils import validate_dataframe_with_ge
from extract.api_extract import extract_api_data
from extract.airbnb_extract import extract_airbnb_data
from transform.airbnb_transform import clean_airbnb_data
from transform.api_transform import clean_api_data
from load.merge_operacional import merge_operacional
from database.create_dimensional import create_and_prepare_dimensional_model_data
from load.load_dimensional import load_dimensional_data
from load.data_merge import data_merge

GE_CONTEXT_ROOT_DIR = os.path.join(PROJECT_ROOT, "great_expectations")
TARGET_DB_NAME = Variable.get("postgres_db_name", default_var="airbnb")

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 5, 14), 
    'retries': 1,
    'retry_delay': timedelta(minutes=2) 
}

@dag(
    dag_id="ETL_Airbnb",
    default_args=default_args,
    description='Pipeline on ETL from AirBnB and FourSquare Data.',
    schedule_interval=timedelta(days=1),
    max_active_runs=1,
    catchup=False,
    tags=['etl', 'airbnb', 'foursquare', 'postgres', 'kafka', 'v2']
)
def etl_pipeline():
    @task(task_id="1.1_extraccion_airbnb")
    def extraccion_airbnb_task() -> pd.DataFrame:
        raw_airbnb_df = extract_airbnb_data()
        if not isinstance(raw_airbnb_df, pd.DataFrame):
            raise TypeError("extract_airbnb_data debe devolver un DataFrame")
        return raw_airbnb_df
    
    @task(task_id="1.2_extraccion_api")
    def extraccion_api_task() -> pd.DataFrame:
        raw_api_df = extract_api_data()
        if not isinstance(raw_api_df, pd.DataFrame):
            raise TypeError("extract_api_data debe devolver un DataFrame")
        return raw_api_df
    
    @task(task_id="2.1_transformacion_airbnb")
    def transformacion_airbnb_task(df_airbnb_raw: pd.DataFrame) -> pd.DataFrame:
        cleaned_airbnb_df = clean_airbnb_data(df_airbnb_raw)
        if not isinstance(cleaned_airbnb_df, pd.DataFrame):
            raise TypeError("clean_airbnb_data debe devolver un DataFrame")

        validation_result = validate_dataframe_with_ge(
            df=cleaned_airbnb_df,
            expectation_suite_name="cleaned_airbnb_suite",
            data_asset_name="cleaned_airbnb_from_pipeline",
            ge_context_root_dir=GE_CONTEXT_ROOT_DIR
        )
        if not validation_result.success:
            raise AirflowFailException("Validación GE fallida para cleaned_airbnb_suite")
        
        return cleaned_airbnb_df
    
    @task(task_id="2.2_transformacion_api")
    def transformacion_api_task(df_api_raw: pd.DataFrame) -> pd.DataFrame:
        cleaned_api_df = clean_api_data(df_api_raw)
        if not isinstance(cleaned_api_df, pd.DataFrame):
            raise TypeError("clean_api_data debe devolver un DataFrame")

        validation_result = validate_dataframe_with_ge(
            df=cleaned_api_df,
            expectation_suite_name="cleaned_api_suite",
            data_asset_name="cleaned_api_from_pipeline",
            ge_context_root_dir=GE_CONTEXT_ROOT_DIR
        )
        if not validation_result.success:
            raise AirflowFailException("Validación GE fallida para cleaned_api_suite")
        
        return cleaned_api_df
    
    @task(task_id="3.1_merge_operacional")
    def merge_operacional_task(df_airbnb: pd.DataFrame, df_tiendas: pd.DataFrame) -> pd.DataFrame:
        merged_df = merge_operacional(df_airbnb, df_tiendas)
        if not isinstance(merged_df, pd.DataFrame):
            raise TypeError("merge_operacional debe devolver un DataFrame")
        return merged_df
    
    @task(task_id="3.2_data_merge")
    def data_merge_task(df_airbnb: pd.DataFrame, df_operations: pd.DataFrame) -> pd.DataFrame:
        df_data_merge = data_merge(df_airbnb, df_operations)
        if not isinstance(df_data_merge, pd.DataFrame):
            raise TypeError("data_merge debe devolver un DataFrame")
        return df_data_merge
    
    @task(task_id="4.1_preparar_modelo_dimensional")
    def preparar_modelo_dimensional_task(df_merged_final: pd.DataFrame) -> dict:
        """
        Prepara los DataFrames para el modelo dimensional y crea las tablas si no existen.
        """
        if df_merged_final.empty:
            raise AirflowFailException("El DataFrame df_merged_final está vacío. No se puede preparar el modelo dimensional.")
        
        prepared_data = create_and_prepare_dimensional_model_data(df_merged_final, db_name=TARGET_DB_NAME)
        
        # validation_result = validate_dataframe_with_ge(
        #     df=prepared_data["dim_host"],
        #     expectation_suite_name="dim_host_suite",
        #     data_asset_name="dim_host_from_pipeline",
        #     ge_context_root_dir=GE_CONTEXT_ROOT_DIR
        # )
        # if not validation_result.success:
        #     raise AirflowFailException("Validación GE fallida para dim_host_suite")
            
        return prepared_data
    
    @task(task_id="4.2_cargar_modelo_dimensional")
    def cargar_modelo_dimensional_task(prepared_data_dictionary: dict):
        """
        Carga los DataFrames del modelo dimensional en la base de datos.
        """
        if not prepared_data_dictionary or not any(not df.empty for df in prepared_data_dictionary.values() if isinstance(df, pd.DataFrame)):
            raise AirflowFailException("El diccionario de datos preparados está vacío o todos los DataFrames están vacíos. No se carga nada.")

        custom_load_order = [
            "dim_host",
            "dim_property_location",
            "dim_property",
            "dim_date",
            "fact_publication"
        ]
        success = load_dimensional_data(prepared_data_dictionary, db_name=TARGET_DB_NAME, load_order=custom_load_order)
        if not success:
            raise AirflowFailException("La carga del modelo dimensional falló.")
        return "Carga dimensional completada."

    # Flujo del DAG
    df_airbnb_raw_output = extraccion_airbnb_task()
    df_api_raw_output = extraccion_api_task()

    df_airbnb_clean_output = transformacion_airbnb_task(df_airbnb_raw_output)
    df_api_clean_output = transformacion_api_task(df_api_raw_output)
    
    df_merged_operacional_output = merge_operacional_task(
        df_airbnb=df_airbnb_clean_output,
        df_tiendas=df_api_clean_output
    )

    df_final_merged_output = data_merge_task(
        df_airbnb=df_airbnb_clean_output,
        df_operations=df_merged_operacional_output
    )

    prepared_dimensional_data_output = preparar_modelo_dimensional_task(
        df_merged_final=df_final_merged_output
    )

    cargar_modelo_dimensional_task(
        prepared_data_dictionary=prepared_dimensional_data_output
    )

etl_dag_instance = etl_pipeline()
