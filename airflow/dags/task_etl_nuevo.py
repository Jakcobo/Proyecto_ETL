# /home/nicolas/Escritorio/proyecto/otra_prueba/airflow/dags/task_etl.py
import pandas as pd
import logging
import sys
import os
from sqlalchemy import create_engine # Se necesitará para el Load final
from dotenv import load_dotenv

# Asegúrate que la ruta a 'src' sea correcta desde la perspectiva de Airflow
# Puede ser necesario ajustar esto dependiendo de cómo se ejecute Airflow (ej. Docker)
# Una mejor práctica es instalar tu código como un paquete Python.
try:
    # Intenta la ruta relativa desde task_etl.py
    src_path = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..", "src"))
    if src_path not in sys.path:
        sys.path.append(src_path)
    # Importar módulos después de asegurar la ruta
    from extract.extract_data import exe_extract_data
    from load.load_data import exe_load_data # Usaremos esto para cargar datos LIMPIOS
    # Importa la función orquestadora del pipeline de limpieza
    from transform.dataset_clean import clean_airbnb_data
    # from load.model_dimensional import ModelDimensional # Si se usa después
    from database.db import get_db_engine # Reutilizar conexión si es posible
except ImportError as e:
     logging.error(f"Error importing project modules: {e}. Check sys.path and project structure relative to Airflow execution.")
     raise

logging.basicConfig(level=logging.INFO, format='%(asctime)s %(message)s', datefmt='%m/%d/%Y %I:%M:%S %p')
logger = logging.getLogger(__name__)

# --- Variables de Configuración (Opcional, mejor con Airflow Variables/Connections) ---
# Puedes definir el nombre de la tabla destino aquí o pasarlo como parámetro
FINAL_TABLE_NAME = "airbnb_cleaned" # O 'airbnb_EDA' si quieres sobreescribir

# --- Funciones de Tareas de Airflow ---

def extract_data() -> pd.DataFrame:
    """Tarea de extracción: Extrae datos y devuelve un DataFrame."""
    try:
        logger.info("Executing data extraction task.")
        df = exe_extract_data() # Asume que esto devuelve un DataFrame
        if not isinstance(df, pd.DataFrame):
             logger.error(f"Extraction function did not return a pandas DataFrame. Got type: {type(df)}")
             raise TypeError("Extraction must return a pandas DataFrame")
        logger.info(f"Extraction successful. DataFrame shape: {df.shape}")
        # No convertir a JSON si las tareas siguientes esperan un DataFrame
        return df
    except Exception as e:
        logger.error(f"Error during data extraction: {e}", exc_info=True)
        raise

def clean_data(df_raw: pd.DataFrame) -> pd.DataFrame:
    """Tarea de limpieza: Recibe un DataFrame, lo limpia y devuelve el DataFrame limpio."""
    try:
        logger.info("Executing data cleaning task.")
        if not isinstance(df_raw, pd.DataFrame):
            # Esto puede ocurrir si XCom falla o la tarea anterior no devolvió un DF
            logger.error(f"Input to clean_data is not a DataFrame. Got type: {type(df_raw)}. Check XCom backend and previous task output.")
            # Intentar cargar desde JSON si se pasó como JSON (menos ideal con TaskFlow)
            # try:
            #     df_raw = pd.read_json(df_raw, orient='records')
            #     logger.info("Successfully loaded DataFrame from JSON fallback.")
            # except Exception as json_e:
            #     logger.error(f"Could not convert input to DataFrame: {json_e}")
            raise TypeError("clean_data requires a pandas DataFrame input.")

        logger.info(f"DataFrame received for cleaning. Shape: {df_raw.shape}")
        # Llama a la función de limpieza refactorizada
        df_cleaned = clean_airbnb_data(df_raw)
        logger.info(f"Data cleaning completed. Cleaned DataFrame shape: {df_cleaned.shape}")
        return df_cleaned
    except Exception as e:
        logger.error(f"Error during data cleaning: {e}", exc_info=True)
        raise

def load_cleaned_data(df_cleaned: pd.DataFrame):
    """Tarea de carga: Recibe el DataFrame limpio y lo carga en la tabla final."""
    try:
        logger.info(f"Executing loading task for cleaned data into table '{FINAL_TABLE_NAME}'.")
        if not isinstance(df_cleaned, pd.DataFrame):
            logger.error(f"Input to load_cleaned_data is not a DataFrame. Got type: {type(df_cleaned)}.")
            raise TypeError("load_cleaned_data requires a pandas DataFrame input.")

        logger.info(f"Cleaned DataFrame received for loading. Shape: {df_cleaned.shape}")

        # Reutilizar la lógica de exe_load_data, asegurándose que usa 'replace'
        # y apunta a la tabla correcta.
        # Podrías pasar db_name y table_name como argumentos si necesitas flexibilidad.
        success = exe_load_data(df=df_cleaned, db_name="airbnb", table_name=FINAL_TABLE_NAME)

        if success:
            logger.info(f"Cleaned data loading into '{FINAL_TABLE_NAME}' completed successfully.")
        else:
            logger.warning(f"Cleaned data loading function reported failure for table '{FINAL_TABLE_NAME}'.")
        return success

    except Exception as e:
        logger.error(f"Error during cleaned data loading into '{FINAL_TABLE_NAME}': {e}", exc_info=True)
        raise

# --- Funciones antiguas (ya no se usan directamente en el DAG principal o refactorizadas) ---
# def load_data(df_to_load): # Esta función ahora es load_cleaned_data
#     pass
# def old_clean_data(): # Lógica movida a clean_airbnb_data y llamada por clean_data
#     pass
# def migrate_to_dimensional_model(): # Mantener si se usa después
#     pass