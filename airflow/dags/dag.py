import os
import sys
from datetime import timedelta, datetime
from airflow.decorators import dag, task
import pandas as pd

PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
SRC_PATH = os.path.join(PROJECT_ROOT, "src")

if SRC_PATH not in sys.path:
    sys.path.append(SRC_PATH)

from extract.api_extract import extract_foursquare_data 
from extract.airbnb_extract import extract_airbnb_main_data
from transform.api_transform import clean_api_data_updated 
from transform.airbnb_transform import clean_airbnb_data_updated 
from load.load_airbnb_api_clean import load_cleaned_api_airbnb_to_db 
from database.merge import merge_airbnb_api_data 
from load.load_merge import load_merged_data_to_db
from database.model_dimensional import create_and_prepare_dimensional_model_data
from load.load_dimensional import populate_dimensional_model
from kafka_producer import stream_data_to_kafka


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
    dag_id="ETL_Airbnb_Foursquare_V2_No_Bruta",
    default_args=default_args,
    description='Nuevo flujo ETL con carga bruta, merge y Kafka.',
    schedule_interval=timedelta(days=1),
    max_active_runs=1,
    catchup=False,
    tags=['etl', 'airbnb', 'foursquare', 'postgres', 'kafka', 'v2']
)
def etl_pipeline_v2_no_bruta():

    # --- Tareas de Extracción (Paso 1.x) ---
    @task(task_id="1.1_extraccion_api")
    def extraccion_api_task() -> pd.DataFrame:
        # La ruta está definida en tu descripción, pero es mejor si la función
        # extract_foursquare_data la construye internamente de forma robusta
        # o la toma de una variable de Airflow / config.
        # Por ahora, asumimos que la función extract_foursquare_data sabe dónde está el archivo.
        # O puedes pasar la ruta: extract_foursquare_data(csv_path="/home/nicolas/Escritorio/proyecto ETL/develop/data/raw/ny_places_foursquare_v3.csv")
        raw_api_df = extract_foursquare_data()
        if not isinstance(raw_api_df, pd.DataFrame):
             raise TypeError("extract_foursquare_data debe devolver un Pandas DataFrame")
        return raw_api_df

    @task(task_id="1.2_extraccion_airbnb")
    def extraccion_airbnb_task() -> pd.DataFrame:
        # Similarmente, para Airbnb. Tu descripción dice ny_places_foursquare_v3.csv,
        # pero debería ser el archivo de Airbnb. Corrijo asumiendo que era un typo.
        # extract_airbnb_main_data(csv_path="/home/nicolas/Escritorio/proyecto ETL/develop/data/raw/Airbnb_Open_Data.csv")
        raw_airbnb_df = extract_airbnb_main_data()
        if not isinstance(raw_airbnb_df, pd.DataFrame):
             raise TypeError("extract_airbnb_main_data debe devolver un Pandas DataFrame")
        return raw_airbnb_df

    # --- Tareas de Transformación (Paso 2.x) ---
    @task(task_id="2.1_transformacion_api")
    def transformacion_api_task(df_api_raw: pd.DataFrame) -> pd.DataFrame:
        cleaned_api_df = clean_api_data_updated(df_api_raw)
        if not isinstance(cleaned_api_df, pd.DataFrame):
             raise TypeError("clean_api_data_updated debe devolver un Pandas DataFrame")
        return cleaned_api_df

    @task(task_id="2.2_transformacion_airbnb")
    def transformacion_airbnb_task(df_airbnb_raw: pd.DataFrame) -> pd.DataFrame:
        cleaned_airbnb_df = clean_airbnb_data_updated(df_airbnb_raw)
        if not isinstance(cleaned_airbnb_df, pd.DataFrame):
             raise TypeError("clean_airbnb_data_updated debe devolver un Pandas DataFrame")
        return cleaned_airbnb_df

    # --- Tarea de Carga Limpia (Paso 2.3) ---
    @task(task_id="2.3_carga_airbnb_y_api_limpia")
    def carga_airbnb_y_api_limpia_task(df_api_cleaned: pd.DataFrame, df_airbnb_cleaned: pd.DataFrame):
        # Esta función en load_airbnb_api_clean.py necesitará dos argumentos DataFrame
        # y los nombres de las tablas. Ej: "cleaned_api_staging", "cleaned_airbnb_staging"
        load_cleaned_api_airbnb_to_db(df_api_cleaned, "cleaned_api_staging", df_airbnb_cleaned, "cleaned_airbnb_staging")

    # --- Tarea de Merge (Paso 3.1) ---
    @task(task_id="3.1_merge_datos")
    def merge_datos_task(df_api_cleaned: pd.DataFrame, df_airbnb_cleaned: pd.DataFrame) -> pd.DataFrame:
        # La función de merge tomará los dos DataFrames limpios.
        # NOTA: Si el volumen de datos es muy grande, considera que esta tarea lea
        # desde las tablas creadas en 2.3_carga_airbnb_y_api_limpia_task en lugar de pasar DFs por XCom.
        # Por ahora, sigo tu descripción de "entrada los dataframes de 2.1 y 2.2".
        merged_df = merge_airbnb_api_data(df_airbnb_cleaned, df_api_cleaned)
        if not isinstance(merged_df, pd.DataFrame):
             raise TypeError("merge_airbnb_api_data debe devolver un Pandas DataFrame")
        return merged_df

    # --- Tarea de Carga Merge (Paso 3.2) ---
    @task(task_id="3.2_carga_merge")
    def carga_merge_task(df_merged: pd.DataFrame):
        # Carga el DataFrame mergeado a una tabla final de staging/integración. Ej: "final_merged_data"
        load_merged_data_to_db(df_merged, "final_merged_data")

    # --- Tarea de Creación Modelo Dimensional (Paso 4.1) ---
    # Según tu descripción, esta tarea tiene como entrada el DF de 3.1 merge.
    # Esto es un poco inusual si la tarea SOLO crea tablas (como en tu DAG original).
    # Si esta tarea *transforma* el df_merged para ajustarlo al modelo Y LUEGO 4.2 lo carga, tiene sentido.
    # Si solo crea tablas, no necesita entrada y su salida no es un DF.
    # Voy a asumir que esta tarea *prepara* el DF para el modelo dimensional.
    # Si solo crea tablas, quita la entrada y salida de DataFrame.
    @task(task_id="4.1_preparar_modelo_dimensional")
    def preparar_modelo_dimensional_task(df_merged: pd.DataFrame) -> dict: # <--- TIPO DE RETORNO CORREGIDO
        """
        Crea el esquema del modelo dimensional si no existe y prepara los DataFrames
        para cada tabla del modelo a partir del DataFrame mergeado.
        Retorna un diccionario de DataFrames.
        """
        # La función create_and_prepare_dimensional_model_data ya maneja la creación de tablas
        # y la preparación de los datos.
        prepared_data_dict = create_and_prepare_dimensional_model_data(
            df_merged=df_merged,
            db_name="airbnb" # Asegúrate que el nombre de la BD sea el correcto
        )
        if not isinstance(prepared_data_dict, dict):
            raise TypeError("create_and_prepare_dimensional_model_data debe devolver un diccionario de DataFrames.")
        return prepared_data_dict # <--- RETORNO CORREGIDO


    # --- Tarea de Carga Dimensional (Paso 4.2) ---
    @task(task_id="4.2_carga_dimensional")
    def carga_dimensional_task(prepared_dataframes_dict: dict): # <--- ARGUMENTO DE ENTRADA CORREGIDO
        """
        Carga los DataFrames preparados (dimensiones y hechos) en la base de datos.
        """
        # La función populate_dimensional_model toma el diccionario de DataFrames.
        populate_dimensional_model(
            prepared_dataframes=prepared_dataframes_dict,
            db_name="airbnb" # Asegúrate que el nombre de la BD sea el correcto
        )
        # Esta tarea no necesita retornar nada para la siguiente (Kafka),
        # la dependencia se establece por el flujo.

    # --- Tarea Productor Kafka (Paso 5) ---
    @task(task_id="5_kafka_producer")
    def kafka_producer_task(dimensional_load_status: None): # dimensional_load_status es solo para dependencia
        # El argumento 'dimensional_load_status' es solo para asegurar que esta tarea
        # se ejecute DESPUÉS de carga_dimensional_task. No se usa su valor.
        stream_data_to_kafka(
            db_name="airbnb", # Nombre de la BD donde está el modelo dimensional
            source_table_for_kafka="fact_listing_pois", # <--- LÍNEA ACTUALIZADA
            max_messages=100, # Ajusta según necesidad para pruebas/producción
            time_limit_seconds=600 # 10 minutos
        )


    # --- Definición del Flujo de Tareas ---
    # ... (flujo para tareas 1.1 a 3.2 sin cambios) ...
    df_api_raw_output = extraccion_api_task()
    df_api_cleaned_output = transformacion_api_task(df_api_raw_output)

    df_airbnb_raw_output = extraccion_airbnb_task()
    df_airbnb_cleaned_output = transformacion_airbnb_task(df_airbnb_raw_output)

    carga_limpia_resultado = carga_airbnb_y_api_limpia_task(
        df_api_cleaned=df_api_cleaned_output,
        df_airbnb_cleaned=df_airbnb_cleaned_output
    )

    df_merged_output = merge_datos_task(
        df_api_cleaned=df_api_cleaned_output,
        df_airbnb_cleaned=df_airbnb_cleaned_output
    )

    carga_merge_resultado = carga_merge_task(df_merged_output)

    # Flujo para Modelo Dimensional y Kafka
    # La tarea 4.1 toma el df_merged_output y produce un diccionario de DFs.
    prepared_data_dictionary_output = preparar_modelo_dimensional_task(df_merged=df_merged_output)

    # La tarea 4.2 toma el diccionario de DFs y carga el modelo.
    # La dependencia de carga_merge_resultado es implícita porque df_merged_output
    # (que se usa para generar prepared_data_dictionary_output) depende de carga_merge_task
    # si el merge leyera de la tabla "final_merged_data".
    # PERO, si merge_datos_task produce df_merged_output directamente, entonces
    # preparar_modelo_dimensional_task puede correr en paralelo a carga_merge_task.
    # Sin embargo, para que populate_dimensional_model funcione correctamente, los datos
    # de "final_merged_data" (si se usan como referencia indirecta o si las dimensiones se leen de la BD)
    # deben estar cargados.
    #
    # Para asegurar el orden correcto:
    # 1. df_merged_output (de 3.1) va a carga_merge_task (3.2) Y a preparar_modelo_dimensional_task (4.1)
    # 2. carga_dimensional_task (4.2) debe esperar a que preparar_modelo_dimensional_task (4.1) termine
    #    (para tener el diccionario de DFs) Y a que carga_merge_task (3.2) termine (para que la tabla
    #    "final_merged_data" esté disponible si `populate_dimensional_model` la consulta, aunque
    #    la lógica actual de `populate_dimensional_model` usa el diccionario de DFs y relee dimensiones).

    # Para simplificar y asegurar el orden, hacemos que 4.1 dependa de 3.2 si es necesario,
    # o simplemente pasamos los datos. El flujo actual es:
    # 3.1 (merge_datos_task) -> df_merged_output
    # df_merged_output -> 3.2 (carga_merge_task) -> carga_merge_resultado
    # df_merged_output -> 4.1 (preparar_modelo_dimensional_task) -> prepared_data_dictionary_output

    # carga_dimensional_task (4.2) necesita prepared_data_dictionary_output.
    # Y lógicamente, es bueno que la tabla "final_merged_data" esté cargada antes de poblar el modelo,
    # aunque no se use directamente si todo viene del diccionario.
    # Para ser explícitos, podemos hacer que 4.1 espere a 3.2 si hay alguna dependencia indirecta.
    # Pero si 4.1 solo necesita df_merged_output, puede correr en paralelo a 3.2.

    # Flujo actual con paso de datos:
    carga_dim_resultado = carga_dimensional_task(prepared_dataframes_dict=prepared_data_dictionary_output)
    
    # Asegurar que carga_dimensional_task (4.2) también espere a carga_merge_task (3.2)
    # Esto se puede hacer si carga_dimensional_task toma una "señal" de carga_merge_resultado,
    # o si hacemos que preparar_modelo_dimensional_task dependa de carga_merge_resultado.
    #
    # Opción más simple y robusta:
    # Hacer que la preparación del modelo (4.1) dependa de que los datos mergeados estén cargados (3.2).
    # Esto asegura que la tabla "final_merged_data" existe y está poblada antes de que
    # `create_and_prepare_dimensional_model_data` (que crea el schema) se ejecute,
    # y antes de que `populate_dimensional_model` (que podría leer dimensiones de la BD) se ejecute.

    # REVISIÓN DEL FLUJO PARA 4.1 y 4.2:
    # 1. df_merged_output = merge_datos_task(...)
    # 2. carga_merge_resultado = carga_merge_task(df_merged_output)
    # 3. prepared_data_dictionary_output = preparar_modelo_dimensional_task(df_merged=df_merged_output, upstream_task_signal=carga_merge_resultado)
    #    (donde upstream_task_signal es solo para forzar dependencia si no hay paso de datos directo)
    #    O, si `create_and_prepare_dimensional_model_data` NO necesita que `final_merged_data` esté en la BD
    #    (porque todo viene de `df_merged`), entonces `preparar_modelo_dimensional_task` solo depende de `df_merged_output`.

    # El flujo actual donde `preparar_modelo_dimensional_task` solo toma `df_merged_output` es correcto
    # si `create_and_prepare_dimensional_model_data` solo usa ese DataFrame para preparar los datos
    # y crea el schema independientemente de la tabla `final_merged_data`.
    # Y `populate_dimensional_model` usa el diccionario.

    # La dependencia de `carga_dimensional_task` con `carga_merge_task` es implícita si
    # `populate_dimensional_model` lee las dimensiones de la BD, y esas dimensiones
    # se poblaron a partir de `final_merged_data` (lo cual no es el caso aquí, se pueblan
    # a partir del diccionario).

    # Por lo tanto, el flujo:
    # df_merged_output -> carga_merge_resultado
    # df_merged_output -> prepared_data_dictionary_output
    # prepared_data_dictionary_output -> carga_dim_resultado
    # Es correcto. `carga_dim_resultado` no necesita esperar explícitamente a `carga_merge_resultado`
    # si `populate_dimensional_model` solo usa `prepared_data_dictionary_output` y el engine.

    kafka_producer_task(dimensional_load_status=carga_dim_resultado)


# Instanciar el DAG
etl_dag_v2_no_bruta_instance = etl_pipeline_v2_no_bruta()