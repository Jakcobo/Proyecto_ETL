import os
import sys
from datetime import timedelta, datetime
from airflow.decorators import dag, task
from airflow.exceptions import AirflowFailException
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
from load.data_merge import data_merge

GE_CONTEXT_ROOT_DIR = os.path.join(PROJECT_ROOT, "great_expectations")

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

    # Flujo del DAG
    df_airbnb_raw = extraccion_airbnb_task()
    df_api_raw = extraccion_api_task()
    df_airbnb_clean = transformacion_airbnb_task(df_airbnb_raw)
    df_api_clean = transformacion_api_task(df_api_raw)
    df_merged = merge_operacional_task(df_airbnb_clean, df_api_clean)
    data_merge_task(df_airbnb_clean, df_merged)

etl_dag_instance = etl_pipeline()
