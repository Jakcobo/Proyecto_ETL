# /home/nicolas/Escritorio/proyecto/otra_prueba/airflow/dags/dag.py
from datetime import timedelta, datetime
from airflow.decorators import dag, task
from airflow.models.param import Param # <--- Importar Param
import os

# Importar las funciones de tarea refactorizadas
from task_etl import (
    extract_data_to_staging,
    clean_data_from_staging,
    load_cleaned_data_from_staging,
    create_dimensional_model,
    insert_data_to_model
)

# Define una ruta base para el staging area (puede ser configurable también)
STAGING_AREA_BASE_PATH = "/tmp/airflow_staging"

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 4, 8), # Ajusta según necesidad
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

@dag(
    dag_id="ETL_Airbnb_Parametrized", # ID más genérico
    default_args=default_args,
    description='ETL pipeline parametrizable para Airbnb data usando staging y variables.',
    schedule=timedelta(days=1), # O tu schedule deseado
    max_active_runs=1,
    catchup=False,
    tags=['etl', 'airbnb', 'postgres', 'refactored', 'dimensional', 'staging', 'parametrized'],
    # --- Definición de Parámetros ---
    params={
        "output_clean_table": Param(
            "airbnb_cleaned", # Valor por defecto
            type="string",
            description="Nombre de la tabla destino para los datos limpios (staging)."
        ),
        "source_csv_var": Param(
            "airbnb_source_csv_path", # Nombre de la Variable Airflow a usar por defecto
             type="string",
             description="Nombre de la Variable Airflow que contiene la ruta relativa del CSV fuente."
        ),
         "project_root_var": Param(
             "AIRFLOW_VAR_AIRBNB_PROJECT_ROOT", # Nombre de la variable para la raíz del proyecto
             type="string",
             description="Nombre de la Variable Airflow para la ruta raíz del proyecto (usada si la ruta del CSV es relativa)."
         ),
        "target_db_conn_id": Param(
            "postgres_airbnb", # Nombre de la Conexión Airflow por defecto
            type="string",
            description="ID de la Conexión Airflow de Postgres a usar."
        )
        # Puedes añadir más: db_name (si no está en la conn), staging_path_base, etc.
    }
)
def etl_dag_parametrized():
    """
    DAG parametrizado para procesar datos de Airbnb.
    Extrae datos de un CSV (ruta definida en Variable Airflow),
    limpia los datos usando un staging area local, carga los datos limpios
    a una tabla (nombre parametrizable) y finalmente puebla un modelo dimensional.
    """

    @task
    def extract_data_task(**context) -> str:
        """
        Task: Extrae datos de la ruta CSV (definida por variables en params),
        guarda los datos en el staging area y devuelve la ruta del archivo Parquet.
        """
        params = context['params']
        run_id = context['run_id']
        staging_path = os.path.join(STAGING_AREA_BASE_PATH, run_id)
        os.makedirs(staging_path, exist_ok=True)

        # Pasa los nombres de las variables a usar como parámetros
        extracted_file_path = extract_data_to_staging(
            staging_dir=staging_path,
            csv_path_var=params['source_csv_var'],
            project_root_var_name=params['project_root_var']
        )
        return extracted_file_path

    @task
    def clean_data_task(input_file_path: str, **context) -> str:
        """
        Task: Lee datos Parquet del staging, los limpia, guarda el resultado
        en un nuevo archivo Parquet en staging y devuelve su ruta.
        """
        run_id = context['run_id']
        staging_path = os.path.join(STAGING_AREA_BASE_PATH, run_id)
        cleaned_file_path = clean_data_from_staging(input_file_path, staging_path)
        return cleaned_file_path

    @task
    def load_cleaned_data_task(cleaned_file_path: str, table_name: str, db_conn_id: str):
        """
        Task: Lee datos limpios Parquet del staging y los carga en la tabla especificada
        usando la conexión de base de datos definida.
        """
        load_success = load_cleaned_data_from_staging(cleaned_file_path, table_name, db_conn_id)
        if not load_success:
             raise ValueError(f"Fallo al cargar datos limpios en la tabla '{table_name}'.")

    @task
    def create_model_task(db_conn_id: str):
        """
        Task: Crea las tablas del modelo dimensional (si no existen)
        usando la conexión de base de datos definida.
        """
        model_created = create_dimensional_model(db_conn_id)
        if not model_created:
            raise ValueError("Fallo al crear el modelo dimensional.")

    @task
    def insert_data_to_model_task(source_table_name: str, db_conn_id: str):
        """
        Task: Inserta datos desde la tabla limpia especificada hacia las tablas
        del modelo dimensional, usando la conexión definida.
        """
        insert_success = insert_data_to_model(source_table_name, db_conn_id)
        if not insert_success:
            raise ValueError(f"Fallo al insertar datos en el modelo dimensional desde '{source_table_name}'.")

    # --- Definir el flujo del DAG ---
    # Las tareas acceden a los params directamente o se les pasan vía Jinja

    extracted_data_path = extract_data_task() # Accede a params vía context internamente
    cleaned_data_path = clean_data_task(extracted_data_path) # No necesita params directamente

    # Pasar los parámetros relevantes usando Jinja templating
    load_result = load_cleaned_data_task(
        cleaned_file_path=cleaned_data_path,
        table_name="{{ params.output_clean_table }}",
        db_conn_id="{{ params.target_db_conn_id }}"
    )

    model_creation_result = create_model_task(
        db_conn_id="{{ params.target_db_conn_id }}"
    )

    insert_result = insert_data_to_model_task(
        source_table_name="{{ params.output_clean_table }}",
        db_conn_id="{{ params.target_db_conn_id }}"
    )

    # Definir dependencias
    load_result >> model_creation_result
    model_creation_result >> insert_result

# Instanciar el DAG para que Airflow lo reconozca
etl_instance_parametrized = etl_dag_parametrized()
