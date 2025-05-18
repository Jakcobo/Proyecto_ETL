import os
import sys
from datetime import timedelta, datetime
from airflow.decorators import dag, task
import pandas as pd

PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
SRC_PATH = os.path.join(PROJECT_ROOT, "src")

if SRC_PATH not in sys.path:
    sys.path.append(SRC_PATH)

from extract.api_extract import extract_api_data
from extract.airbnb_extract import extract_airbnb_data
from transform.airbnb_transform import clean_airbnb_data
from transform.api_transform import clean_api_data
#from load.load_airbnb_api_clean import load_cleaned_api_to_db 
#from database.merge import merge_airbnb_api_data
#from load.load_merge import load_merged_data_to_db
#from database.model_dimensional import create_and_prepare_dimensional_model_data
#from load.load_dimensional import populate_dimensional_model
#from kafka_producer import stream_data_to_kafka

if SRC_PATH not in sys.path:
    sys.path.append(SRC_PATH)
    
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)
if SRC_PATH not in sys.path:
    sys.path.insert(0, SRC_PATH)

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
            raise TypeError("extract_airbnb_main_data debe devolver un Pandas DataFrame")
        return raw_airbnb_df
    
    @task(task_id="1.2_extraccion_api")
    def extraccion_api_task() -> pd.DataFrame:
        raw_api_df = extract_api_data()
        if not isinstance(raw_api_df, pd.DataFrame):
            raise TypeError("extract_foursquare_data debe devolver un Pandas DataFrame")
        return raw_api_df
    
    @task(task_id="2.1_transformacion_airbnb")
    def transformacion_airbnb_task(df_airbnb_raw: pd.DataFrame) -> pd.DataFrame:
        cleaned_airbnb_df = clean_airbnb_data(df_airbnb_raw)
        if not isinstance(cleaned_airbnb_df, pd.DataFrame):
            raise TypeError("clean_airbnb_data debe devolver un Pandas DataFrame")
        return cleaned_airbnb_df
    
    @task(task_id="2.2_transformacion_api")
    def transformacion_api_task(df_api_raw: pd.DataFrame) -> pd.DataFrame:
        cleaned_api_df = clean_api_data(df_api_raw)
        if not isinstance(cleaned_api_df, pd.DataFrame):
            raise TypeError("clean_api_data debe devolver un Pandas DataFrame")
        return cleaned_api_df
    
    df_airbnb_raw_output = extraccion_airbnb_task()
    df_airbnb_cleaned_output = transformacion_airbnb_task(df_airbnb_raw_output)
    df_api_raw_output = extraccion_api_task()
    df_api_cleaned_output = transformacion_api_task(df_api_raw_output)

    df_airbnb_cleaned_output
    df_api_cleaned_output
    
    #carga_limpia_resultado = carga_airbnb_y_api_limpia_task(
    #    df_api_cleaned=df_api_cleaned_output,
    #    df_airbnb_cleaned=df_airbnb_cleaned_output
    #)

    #df_merged_output = merge_datos_task(
    #    df_api_cleaned=df_api_cleaned_output,
    #    df_airbnb_cleaned=df_airbnb_cleaned_output
    #)

    #carga_merge_resultado = carga_merge_task(df_merged_output)

    #prepared_data_dictionary_output = preparar_modelo_dimensional_task(df_merged=df_merged_output)

    #carga_dim_resultado = carga_dimensional_task(prepared_dataframes_dict=prepared_data_dictionary_output)
    
    # df_merged_output -> carga_merge_resultado
    # df_merged_output -> prepared_data_dictionary_output
    # prepared_data_dictionary_output -> carga_dim_resultado
    
    #kafka_producer_task(dimensional_load_status=carga_dim_resultado)

etl_dag_instance = etl_pipeline()