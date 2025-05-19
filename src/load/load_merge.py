import os
import sys
from datetime import timedelta, datetime
from airflow.decorators import dag, task
from airflow.exceptions import AirflowFailException
import pandas as pd

PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(_file_), "..", ".."))
SRC_PATH = os.path.join(PROJECT_ROOT, "src")

if SRC_PATH not in sys.path:
    sys.path.append(SRC_PATH)
# Añadir el directorio del proyecto para que great_expectations/ pueda ser encontrado si es necesario
if PROJECT_ROOT not in sys.path:
    sys.path.append(PROJECT_ROOT)

from extract.api_extract import extract_api_data
from extract.airbnb_extract import extract_airbnb_data
from transform.airbnb_transform import clean_airbnb_data
from transform.api_transform import clean_api_data
from load.merge_operacional import merge_operacional
from load.data_merge import data_merge
from utils.ge_utils import validate_dataframe_with_ge # <--- IMPORTAR HELPER

# Define la ruta al contexto de GE. Podrías usar Variables de Airflow para esto.
# Asume que la carpeta great_expectations está en PROJECT_ROOT
GE_CONTEXT_ROOT_DIR = os.path.join(PROJECT_ROOT, "great_expectations")

default_args = { # ... (sin cambios)
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 5, 14),
    'retries': 1,
    'retry_delay': timedelta(minutes=2)
}

@dag( # ... (sin cambios)
    dag_id="ETL_Airbnb_with_GE",
    default_args=default_args,
    description='Pipeline on ETL from AirBnB and FourSquare Data with Great Expectations.',
    schedule_interval=timedelta(days=1),
    max_active_runs=1,
    catchup=False,
    tags=['etl', 'airbnb', 'foursquare', 'postgres', 'kafka', 'v2', 'ge']
)
def etl_pipeline_with_ge():

    @task(task_id="1.1_extraccion_airbnb")
    def extraccion_airbnb_task() -> pd.DataFrame:
        raw_airbnb_df = extract_airbnb_data()
        if not isinstance(raw_airbnb_df, pd.DataFrame):
            raise TypeError("extract_airbnb_main_data debe devolver un Pandas DataFrame")

        validation_result = validate_dataframe_with_ge(
            df=raw_airbnb_df,
            expectation_suite_name="raw_airbnb_suite", # El nombre que le diste a tu suite
            data_asset_name="raw_airbnb_from_pipeline",
            ge_context_root_dir=GE_CONTEXT_ROOT_DIR
        )
        if not validation_result.success:
            # Aquí puedes loggear validation_result para más detalles
            # print(str(validation_result))
            raise AirflowFailException("Validación de GE fallida para raw_airbnb_df")
        return raw_airbnb_df

    @task(task_id="1.2_extraccion_api")
    def extraccion_api_task() -> pd.DataFrame:
        raw_api_df = extract_api_data()
        if not isinstance(raw_api_df, pd.DataFrame):
            raise TypeError("extract_foursquare_data debe devolver un Pandas DataFrame")

        validation_result = validate_dataframe_with_ge(
            df=raw_api_df,
            expectation_suite_name="raw_api_suite",
            data_asset_name="raw_api_from_pipeline",
            ge_context_root_dir=GE_CONTEXT_ROOT_DIR
        )
        if not validation_result.success:
            raise AirflowFailException("Validación de GE fallida para raw_api_df")
        return raw_api_df

    @task(task_id="2.1_transformacion_airbnb")
    def transformacion_airbnb_task(df_airbnb_raw: pd.DataFrame) -> pd.DataFrame:
        cleaned_airbnb_df = clean_airbnb_data(df_airbnb_raw)
        if not isinstance(cleaned_airbnb_df, pd.DataFrame):
            raise TypeError("clean_airbnb_data debe devolver un Pandas DataFrame")

        validation_result = validate_dataframe_with_ge(
            df=cleaned_airbnb_df,
            expectation_suite_name="cleaned_airbnb_suite",
            data_asset_name="cleaned_airbnb_from_pipeline",
            ge_context_root_dir=GE_CONTEXT_ROOT_DIR
        )
        if not validation_result.success:
            raise AirflowFailException("Validación de GE fallida para cleaned_airbnb_df")
        return cleaned_airbnb_df

    @task(task_id="2.2_transformacion_api")
    def transformacion_api_task(df_api_raw: pd.DataFrame) -> pd.DataFrame:
        cleaned_api_df = clean_api_data(df_api_raw)
        if not isinstance(cleaned_api_df, pd.DataFrame):
            raise TypeError("clean_api_data debe devolver un Pandas DataFrame")

        validation_result = validate_dataframe_with_ge(
            df=cleaned_api_df,
            expectation_suite_name="cleaned_api_suite",
            data_asset_name="cleaned_api_from_pipeline",
            ge_context_root_dir=GE_CONTEXT_ROOT_DIR
        )
        if not validation_result.success:
            raise AirflowFailException("Validación de GE fallida para cleaned_api_df")
        return cleaned_api_df

    @task(task_id="3.1_merge_operacional")
    def merge_operacional_task(df_airbnb: pd.DataFrame, df_tiendas: pd.DataFrame) -> pd.DataFrame:
        merged_df = merge_operacional(df_airbnb, df_tiendas)
        if not isinstance(merged_df, pd.DataFrame):
            raise TypeError("merge_operacional_airbnb_api debe devolver un DataFrame")

        validation_result = validate_dataframe_with_ge(
            df=merged_df,
            expectation_suite_name="merged_operacional_suite",
            data_asset_name="merged_operacional_from_pipeline",
            ge_context_root_dir=GE_CONTEXT_ROOT_DIR
        )
        if not validation_result.success:
            raise AirflowFailException("Validación de GE fallida para merged_operacional_df")
        return merged_df

    @task(task_id="3.2_data_merge")
    def data_merge_task(df_airbnb: pd.DataFrame, df_operations: pd.DataFrame) -> pd.DataFrame:
        df_data_merge_result = data_merge(df_airbnb, df_operations) # Renombré para evitar confusión
        if not isinstance(df_data_merge_result, pd.DataFrame):
            raise TypeError("data_merge debe devolver un DataFrame")

        validation_result = validate_dataframe_with_ge(
            df=df_data_merge_result,
            expectation_suite_name="final_data_merge_suite",
            data_asset_name="final_data_merge_from_pipeline",
            ge_context_root_dir=GE_CONTEXT_ROOT_DIR
        )
        if not validation_result.success:
            raise AirflowFailException("Validación de GE fallida para final_data_merge_df")
        return df_data_merge_result

    # Definición del flujo del DAG
    df_airbnb_raw_output = extraccion_airbnb_task()
    df_api_raw_output = extraccion_api_task()

    df_airbnb_cleaned_output = transformacion_airbnb_task(df_airbnb_raw_output)
    df_api_cleaned_output = transformacion_api_task(df_api_raw_output)

    df_merge_output = merge_operacional_task(
        df_airbnb=df_airbnb_cleaned_output,
        df_tiendas=df_api_cleaned_output
    )

    final_merged_data_output = data_merge_task(
        df_airbnb=df_airbnb_cleaned_output, # Asumo que es df_airbnb_cleaned_output aquí
        df_operations=df_merge_output
    )
    # Continúa con tus tareas de carga si final_merged_data_output es el que se carga.
    # load_final_data_task(final_merged_data_output) # Ejemplo

etl_dag_instance_ge = etl_pipeline_with_ge()