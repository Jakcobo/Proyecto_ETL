# /home/nicolas/Escritorio/proyecto/otra_prueba/airflow/dags/task_etl.py
import pandas as pd
import logging
import sys
import os
from dotenv import load_dotenv # Todavía útil para fallbacks o config local
from airflow.models import Variable # Para leer variables

# Asegúrate que la ruta a 'src' sea correcta (esto puede necesitar ajuste según dónde corra Airflow)
try:
    # Intenta una ruta relativa al DAG file
    src_path = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..", "src"))
    if src_path not in sys.path:
        sys.path.append(src_path)

    # Alternativamente, podrías definir PYTHONPATH o añadir a sys.path en el Dockerfile si usas Docker
    # print("SYSPATH:", sys.path) # Para debuggear

    from extract.extract_data import exe_extract_data
    from load.load_data import exe_load_data
    from transform.dataset_clean import clean_airbnb_data
    from database.modeldb import create_dimensional_model_tables
    from load.dimensional_load import load_dimensional_data
    # Asegúrate que db.py ahora soporta obtener engine desde conn_id
    from database.db import get_db_engine
except ImportError as e:
     logging.error(f"Error importing project modules: {e}. Check sys.path and project structure relative to Airflow execution.")
     logging.error(f"Current sys.path: {sys.path}")
     raise

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s', datefmt='%m/%d/%Y %I:%M:%S %p')
logger = logging.getLogger(__name__)

# Ya no necesitamos FINAL_TABLE_NAME como constante global aquí
STAGING_FILE_FORMAT = "parquet" # Formato para archivos intermedios

# --- Funciones de Tarea Modificadas para Parámetros ---

def extract_data_to_staging(staging_dir: str, csv_path_var: str, project_root_var_name: str) -> str:
    """
    Extrae datos del CSV cuya ruta relativa está en la Variable 'csv_path_var'.
    La ruta base del proyecto se obtiene de 'project_root_var_name'.
    Guarda los datos en formato Parquet en el 'staging_dir'.
    Devuelve la ruta completa al archivo Parquet creado.
    """
    try:
        logger.info(f"Executing data extraction task (to staging). Source var: '{csv_path_var}', Root var: '{project_root_var_name}'")

        # Obtener rutas desde Variables Airflow
        try:
             # Ruta base del proyecto (puede ser /opt/airflow/dags/mi_proyecto si usas Docker)
             project_root = Variable.get(project_root_var_name, default_var=os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..")))
             relative_csv_path = Variable.get(csv_path_var)
             source_csv_path = os.path.join(project_root, relative_csv_path)
             logger.info(f"Resolved source CSV path: {source_csv_path}")
        except KeyError as e:
             logger.error(f"Airflow Variable '{e}' not found! Asegúrate de que las variables '{csv_path_var}' y '{project_root_var_name}' estén definidas en Airflow.")
             raise ValueError(f"Airflow Variable requerida '{e}' no encontrada.") from e

        # Llama a la función de extracción de src pasándole la ruta resuelta
        df = exe_extract_data(source_csv_path)

        if not isinstance(df, pd.DataFrame):
             logger.error(f"Extraction function did not return a pandas DataFrame. Got type: {type(df)}")
             raise TypeError("Extraction must return a pandas DataFrame")

        output_filename = f"extracted_data.{STAGING_FILE_FORMAT}"
        output_path = os.path.join(staging_dir, output_filename)

        logger.info(f"Saving extracted data to: {output_path}")
        df.to_parquet(output_path, index=False)
        logger.info(f"Extraction successful. DataFrame shape: {df.shape}. Saved to staging.")
        return output_path # Devuelve la ruta del archivo

    except FileNotFoundError as fnf:
        logger.error(f"CSV file not found at resolved path: {source_csv_path}. Error: {fnf}", exc_info=True)
        raise # Relanza para que Airflow marque la tarea como fallida
    except Exception as e:
        logger.error(f"Error during data extraction to staging: {e}", exc_info=True)
        raise

def clean_data_from_staging(input_path: str, staging_dir: str) -> str:
    """
    Lee datos Parquet desde 'input_path', los limpia usando la lógica de
    'src.transform.dataset_clean.clean_airbnb_data', y guarda el resultado
    en un nuevo archivo Parquet en 'staging_dir'.
    Devuelve la ruta al archivo limpio.
    """
    try:
        logger.info(f"Executing data cleaning task (from staging: {input_path}).")
        if not os.path.exists(input_path):
            logger.error(f"Input staging file not found: {input_path}")
            raise FileNotFoundError(f"Input staging file not found: {input_path}")

        logger.info(f"Reading data from {input_path}")
        df_raw = pd.read_parquet(input_path)

        logger.info(f"DataFrame received for cleaning. Shape: {df_raw.shape}")
        df_cleaned = clean_airbnb_data(df_raw) # Llama a la lógica de limpieza

        output_filename = f"cleaned_data.{STAGING_FILE_FORMAT}"
        output_path = os.path.join(staging_dir, output_filename)

        logger.info(f"Saving cleaned data to: {output_path}")
        df_cleaned.to_parquet(output_path, index=False)
        logger.info(f"Data cleaning completed. Cleaned DataFrame shape: {df_cleaned.shape}. Saved to staging.")
        return output_path # Devuelve la ruta del archivo limpio

    except Exception as e:
        logger.error(f"Error during data cleaning from staging: {e}", exc_info=True)
        raise

def load_cleaned_data_from_staging(cleaned_file_path: str, table_name: str, db_conn_id: str):
    """
    Lee datos limpios desde 'cleaned_file_path' (Parquet) y los carga
    en la base de datos especificada por 'db_conn_id', en la tabla 'table_name'.
    Utiliza la estrategia 'replace' (borra y recrea la tabla).
    """
    engine = None
    try:
        logger.info(f"Executing loading task into table '{table_name}' using conn_id '{db_conn_id}' (from staging: {cleaned_file_path}).")
        if not os.path.exists(cleaned_file_path):
            logger.error(f"Cleaned data staging file not found: {cleaned_file_path}")
            raise FileNotFoundError(f"Cleaned data staging file not found: {cleaned_file_path}")

        logger.info(f"Reading cleaned data from {cleaned_file_path}")
        df_cleaned = pd.read_parquet(cleaned_file_path)

        if not isinstance(df_cleaned, pd.DataFrame):
             logger.error(f"Staging file did not contain a pandas DataFrame. Got type: {type(df_cleaned)}.")
             raise TypeError("load_cleaned_data requires a pandas DataFrame input.")

        logger.info(f"Cleaned DataFrame received for loading. Shape: {df_cleaned.shape}")

        # Obtiene el engine usando el connection ID proporcionado
        # Asume que get_db_engine puede manejar conn_id y extraer el db_name si es necesario
        # O modifica get_db_engine para aceptar conn_id directamente
        engine = get_db_engine(airflow_conn_id=db_conn_id, use_airflow_conn=True) # Forzar uso de conexión Airflow

        if not engine:
             logger.error(f"Failed to get database engine using connection ID: {db_conn_id}")
             raise ConnectionError(f"Could not establish DB connection using {db_conn_id}")

        # Llama a la función de carga de src pasándole el DataFrame, engine y table_name
        # exe_load_data debe modificarse para aceptar un engine en lugar de db_name, o get_db_engine llamado dentro de ella
        # Simplificamos aquí asumiendo que exe_load_data puede tomar un engine existente
        # O, mejor, pasamos el engine directamente a exe_load_data si lo modificamos para aceptarlo.
        # Alternativa: Modificar exe_load_data para tomar conn_id y table_name
        success = exe_load_data(df=df_cleaned, table_name=table_name, engine_or_conn_id=engine) # Pasar engine

        if success:
            logger.info(f"Cleaned data loading into '{table_name}' completed successfully.")
        else:
            # exe_load_data debería lanzar una excepción en caso de fallo real.
            logger.warning(f"Cleaned data loading function reported failure for table '{table_name}'. Check logs.")
        return success

    except Exception as e:
        logger.error(f"Error during cleaned data loading into '{table_name}' from staging: {e}", exc_info=True)
        raise # Relanza para que Airflow falle la tarea
    finally:
        if engine:
            logger.info("Disposing database engine after loading cleaned data.")
            engine.dispose()


def create_dimensional_model(db_conn_id: str):
    """
    Función lógica para la tarea: Crea las tablas del modelo dimensional
    si no existen, usando la conexión de base de datos especificada.
    """
    engine = None
    try:
        logger.info(f"Executing dimensional model creation task using conn_id '{db_conn_id}'.")
        # Obtiene el engine usando el connection ID
        engine = get_db_engine(airflow_conn_id=db_conn_id, use_airflow_conn=True)

        if engine:
            create_dimensional_model_tables(engine) # Llama a la función de src/database/modeldb.py
            logger.info("Dimensional model tables creation process finished.")
            return True
        else:
            logger.error(f"Failed to get database engine using connection ID: {db_conn_id}. Cannot create dimensional model.")
            return False

    except Exception as e:
        logger.error(f"Error during dimensional model creation: {e}", exc_info=True)
        raise
    finally:
        if engine:
            logger.info("Disposing database engine after model creation.")
            engine.dispose()


def insert_data_to_model(source_table_name: str, db_conn_id: str):
    """
    Función lógica para la tarea: Inserta datos desde la tabla 'source_table_name'
    hacia las tablas del modelo dimensional, usando la conexión 'db_conn_id'.
    """
    engine = None
    try:
        logger.info(f"Executing task to insert data into dimensional model from source '{source_table_name}' using conn_id '{db_conn_id}'.")
        # Obtener el engine usando el connection ID
        engine = get_db_engine(airflow_conn_id=db_conn_id, use_airflow_conn=True)

        if engine:
            # Llamar a la función orquestadora de dimensional_load.py, pasándole el engine y el nombre de la tabla fuente
            success = load_dimensional_data(engine=engine, source_table_name=source_table_name)
            logger.info(f"Insertion into dimensional model finished. Success: {success}")
            return success
        else:
            logger.error(f"Failed to get database engine using connection ID: {db_conn_id}. Cannot insert data into dimensional model.")
            return False

    except Exception as e:
        logger.error(f"Error during insertion into dimensional model from '{source_table_name}': {e}", exc_info=True)
        raise # Relanzar para que la tarea falle en Airflow
    finally:
        if engine:
            logger.info("Disposing database engine after dimensional model insertion.")
            engine.dispose()