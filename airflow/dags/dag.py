#/home/nicolas/Escritorio/proyecto ETL/develop/airflow/dags/dag.py

import os
import sys
from datetime import timedelta, datetime
from airflow.decorators import dag, task
import pandas as pd

# --- Configuración de Paths ---
PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
SRC_PATH = os.path.join(PROJECT_ROOT, "src")

# Añadir PROJECT_ROOT y SRC_PATH a sys.path para permitir importaciones directas
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)
if SRC_PATH not in sys.path:
    sys.path.insert(0, SRC_PATH)

# --- Importar lógica de las tareas desde src ---

# Extract
from extract.api_extract import extract_foursquare_data # Renombrado para claridad, o usa tu nombre
from extract.airbnb_extract import extract_airbnb_main_data # Renombrado para claridad, o usa tu nombre

# Load (Bruta)
from load.bruta_load import load_raw_data_to_db # Necesitará manejar dos DataFrames

# Transform
from transform.api_transform import clean_api_data_updated # O el nombre de tu función de limpieza API
from transform.airbnb_transform import clean_airbnb_data_updated # O el nombre de tu función de limpieza Airbnb

# Load (Cleaned Airbnb y API)
from load.load_airbnb_api_clean import load_cleaned_api_airbnb_to_db # Necesitará manejar dos DataFrames limpios

# Merge
from database.merge import merge_airbnb_api_data # O el nombre de tu función de merge

# Load (Merge)
from load.load_merge import load_merged_data_to_db

# Dimensional Model
# Asumo que model_dimensional.py tendrá una función para crear/asegurar las tablas, similar a modeldb.py
from database.model_dimensional import create_and_prepare_dimensional_model_data # o el nombre de tu función

# Load (Dimensional)
# Asumo que load_dimensional.py (nuevo) leerá de la tabla mergeada y poblará el modelo
from load.load_dimensional import populate_dimensional_model

# Kafka
from kafka_producer import stream_data_to_kafka

# Argumentos por defecto para el DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 5, 14), # Actualiza según necesidad
    'retries': 1,
    'retry_delay': timedelta(minutes=2) # Reducido para pruebas más rápidas
}

@dag(
    dag_id="ETL_Airbnb_Foursquare_V2",
    default_args=default_args,
    description='Nuevo flujo ETL con carga bruta, merge y Kafka.',
    schedule_interval=timedelta(days=1),
    max_active_runs=1,
    catchup=False,
    tags=['etl', 'airbnb', 'foursquare', 'postgres', 'kafka', 'v2']
)
def etl_pipeline_v2():

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

    # --- Tarea de Carga Bruta (Paso 1.3) ---
    @task(task_id="1.3_carga_bruta")
    def carga_bruta_task(df_api_raw: pd.DataFrame, df_airbnb_raw: pd.DataFrame):
        # Esta función en bruta_load.py necesitará dos argumentos DataFrame
        # y los nombres de las tablas donde se cargarán. Ej: "raw_api_data", "raw_airbnb_data"
        load_raw_data_to_db(df_api_raw, "raw_api_data", df_airbnb_raw, "raw_airbnb_data")

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
        merged_df = merge_airbnb_api_data(df_api_cleaned, df_airbnb_cleaned)
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
    def preparar_modelo_dimensional_task(df_merged: pd.DataFrame) -> pd.DataFrame: # o (df_merged: pd.DataFrame) -> dict[str, pd.DataFrame] si preparas DFs para cada tabla dim/fact
        # Esta función en model_dimensional.py podría:
        # 1. Crear las tablas dimensionales si no existen (podría ser una tarea separada sin entrada de DF).
        # 2. Transformar/seleccionar columnas del df_merged para preparar los datos para las tablas dimensionales y de hechos.
        #    Si es así, la salida sería el/los DataFrame(s) listos para cargar.
        # Por ahora, asumo que crea las tablas Y retorna el mismo DF o uno preparado.
        # Opción más simple: esta tarea SOLO crea/valida las tablas, no necesita input ni output de DF.
        # create_and_prepare_dimensional_model_data() # Si solo crea tablas
        # Por ahora, mantengo la idea de que puede transformar el DF.
        # data_for_dimensional_model = transform_for_dimensional_model(df_merged) # Nombre de función hipotético
        # return data_for_dimensional_model

        # SIMPLIFICACIÓN: Asumamos que esta tarea crea las tablas como antes y no maneja DFs.
        # La tarea 4.2 leerá de la tabla cargada por 3.2_carga_merge
        create_and_prepare_dimensional_model_data() # Esta función no debería tomar ni retornar DF
        # Para que el flujo sea correcto, la carga dimensional (4.2) necesitará leer de la tabla
        # creada por carga_merge_task (3.2). Así que esta tarea 4.1 es solo para creación de schema.
        # No hay output de DF para la siguiente tarea si es solo creación de schema.
        return "Schema Created" # O algún indicador de éxito, no un DF.


    # --- Tarea de Carga Dimensional (Paso 4.2) ---
    @task(task_id="4.2_carga_dimensional")
    def carga_dimensional_task(schema_creation_status: str, merged_data_load_status: None): # Depende de que el schema esté y los datos mergeados estén cargados
        # Esta función en load_dimensional.py leerá de la tabla "final_merged_data"
        # (creada por carga_merge_task) y poblará las tablas dimensionales.
        # No toma un DataFrame directamente como argumento de la tarea anterior si 4.1 solo crea schema.
        populate_dimensional_model(source_table_name="final_merged_data")

    # --- Tarea Productor Kafka (Paso 5) ---
    @task(task_id="5_kafka_producer")
    def kafka_producer_task(dimensional_load_status: None): # Depende de que el modelo dimensional esté cargado
        # Esta función leerá datos (ej. de la tabla de hechos) y los enviará a Kafka.
        stream_data_to_kafka(source_table_for_kafka="fact_publication", count=1000, time_limit_minutes=10)


    # --- Definición del Flujo de Tareas ---

    # Flujo API
    df_api_raw_output = extraccion_api_task()
    df_api_cleaned_output = transformacion_api_task(df_api_raw_output)

    # Flujo Airbnb
    df_airbnb_raw_output = extraccion_airbnb_task()
    df_airbnb_cleaned_output = transformacion_airbnb_task(df_airbnb_raw_output)

    # Carga Bruta (paralela a transformaciones, pero depende de extracciones)
    carga_bruta_resultado = carga_bruta_task(df_api_raw=df_api_raw_output, df_airbnb_raw=df_airbnb_raw_output)

    # Carga de datos limpios (después de transformaciones)
    carga_limpia_resultado = carga_airbnb_y_api_limpia_task(df_api_cleaned=df_api_cleaned_output, df_airbnb_cleaned=df_airbnb_cleaned_output)

    # Merge (después de transformaciones)
    # El merge toma los DataFrames limpios directamente de las tareas de transformación
    df_merged_output = merge_datos_task(df_api_cleaned=df_api_cleaned_output, df_airbnb_cleaned=df_airbnb_cleaned_output)

    # Carga del Merge (después del merge)
    carga_merge_resultado = carga_merge_task(df_merged_output)

    # Modelo y Carga Dimensional
    # La creación del schema (4.1) debe completarse.
    # La carga de datos mergeados (3.2) también debe completarse antes de poblar el modelo dimensional (4.2).
    schema_status = preparar_modelo_dimensional_task(df_merged_output) # Revisar esta dependencia si 4.1 no necesita el DF
    # Si 4.1 no necesita df_merged_output, entonces sería: schema_status = preparar_modelo_dimensional_task()
    # y la dependencia sería implícita o se podría agrupar.

    # La carga dimensional (4.2) depende de que las tablas del modelo existan (hecho por 4.1)
    # y de que los datos mergeados estén en la BD (hecho por 3.2 carga_merge_task).
    # carga_dimensional_task necesita saber que carga_merge_task ha terminado.
    carga_dim_resultado = carga_dimensional_task(schema_creation_status=schema_status, merged_data_load_status=carga_merge_resultado)

    # Kafka (después de la carga dimensional)
    kafka_resultado = kafka_producer_task(dimensional_load_status=carga_dim_resultado)

    # Establecer dependencias explícitas donde el paso de datos no es directo:
    # La carga bruta puede correr después de las extracciones.
    # [df_api_raw_output, df_airbnb_raw_output] >> carga_bruta_resultado # Ya está implícito por los args

    # La carga de datos limpios puede correr después de las transformaciones
    # [df_api_cleaned_output, df_airbnb_cleaned_output] >> carga_limpia_resultado # Ya está implícito

    # La tarea de merge depende de que ambas transformaciones terminen
    # [df_api_cleaned_output, df_airbnb_cleaned_output] >> df_merged_output # Ya está implícito


    # La carga dimensional (4.2) depende de que el modelo (schema en 4.1) esté listo
    # Y de que los datos mergeados (3.2) estén cargados.
    # La tarea `preparar_modelo_dimensional_task` (4.1) podría no depender del `df_merged_output` si solo crea tablas.
    # Si es así, `preparar_modelo_dimensional_task` puede correr en paralelo a `carga_merge_task` después de `df_merged_output`.
    # Y `carga_dimensional_task` dependería de ambas.

    # Asumiendo que 4.1 SÍ toma df_merged_output (según tu descripción inicial):
    # df_merged_output >> carga_merge_resultado
    # df_merged_output >> schema_status
    # [carga_merge_resultado, schema_status] >> carga_dim_resultado
    # carga_dim_resultado >> kafka_resultado

    # Flujo más lineal si 4.1 no depende de df_merged_output (solo crea schema):
    # (df_api_raw_output, df_airbnb_raw_output) -> se usan en transformaciones Y en carga_bruta
    # (df_api_cleaned_output, df_airbnb_cleaned_output) -> se usan en carga_limpia Y en merge
    # df_merged_output -> carga_merge_task
    # carga_merge_task \
    #                   -> carga_dimensional_task -> kafka_producer_task
    # preparar_modelo_dimensional_task (corre después de merge o en paralelo a carga_merge) /
    #
    # Para que sea más claro y robusto, hagamos explícitas las dependencias clave si no hay paso de datos:
    # carga_merge_task() y preparar_modelo_dimensional_task() deben terminar ANTES de carga_dimensional_task()
    # Si preparar_modelo_dimensional_task NO toma df_merged_output:
    # df_merged_output >> carga_merge_resultado
    # modelo_schema_creado = preparar_modelo_dimensional_task() # No depende de df_merged_output
    # [carga_merge_resultado, modelo_schema_creado] >> carga_dim_resultado >> kafka_resultado

    # Siguiendo tu descripción de que 4.1 toma el DF de 3.1:
    # No es necesario definir las dependencias con `>>` si ya se pasan por argumentos.
    # Airflow infiere estas dependencias.
    # Solo necesitamos asegurar el orden correcto para las tareas que no tienen paso de XCom directo
    # pero sí dependencia lógica (ej. carga_dimensional depende de que carga_merge haya terminado).

    # Dependencias Lógicas Clave (Airflow las infiere si hay paso de datos, pero es bueno pensarlas):
    # 1. Extracciones primero.
    # 2. Carga Bruta y Transformaciones después de sus respectivas extracciones.
    # 3. Carga Limpia y Merge después de AMBAS transformaciones.
    # 4. Carga Merge después de Merge.
    # 5. Preparar Modelo Dimensional después de Merge (si toma el DF).
    # 6. Carga Dimensional después de Carga Merge Y Preparar Modelo Dimensional.
    # 7. Kafka Producer después de Carga Dimensional.

    # El flujo definido por los argumentos de las tareas ya establece la mayoría de estas.
    # La dependencia clave es que `carga_dimensional_task` necesita que `carga_merge_task`
    # termine (para que los datos estén en la BD) y que `preparar_modelo_dimensional_task`
    # termine (para que el schema exista). Esto ya está cubierto por cómo se pasan los "status".


# Instanciar el DAG
etl_dag_v2 = etl_pipeline_v2()