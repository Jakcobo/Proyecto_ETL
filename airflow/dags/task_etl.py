# /home/nicolas/Escritorio/proyecto/otra_prueba/airflow/dags/task_etl.py
import pandas as pd
import logging
import sys
import os
from dotenv import load_dotenv

# Asegúrate que la ruta a 'src' sea correcta
try:
    src_path = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..", "src"))
    if src_path not in sys.path:
        sys.path.append(src_path)
    # Importar módulos
    from extract.extract_data import exe_extract_data
    from load.load_data import exe_load_data
    from transform.dataset_clean import clean_airbnb_data
    from database.modeldb import create_dimensional_model_tables
    # --- NUEVA IMPORTACIÓN ---
    from load.dimensional_load import load_dimensional_data
    # --- FIN NUEVA IMPORTACIÓN ---
    from database.db import get_db_engine
except ImportError as e:
     logging.error(f"Error importing project modules: {e}. Check sys.path and project structure relative to Airflow execution.")
     raise

logging.basicConfig(level=logging.INFO, format='%(asctime)s %(message)s', datefmt='%m/%d/%Y %I:%M:%S %p')
logger = logging.getLogger(__name__)

FINAL_TABLE_NAME = "airbnb_cleaned" # Tabla donde se cargan los datos limpios

# --- Funciones de Tareas Existentes (sin cambios) ---
def extract_data() -> pd.DataFrame:
    # ... (código existente) ...
    """Tarea de extracción: Extrae datos y devuelve un DataFrame."""
    try:
        logger.info("Executing data extraction task.")
        df = exe_extract_data() # Asume que esto devuelve un DataFrame
        if not isinstance(df, pd.DataFrame):
             logger.error(f"Extraction function did not return a pandas DataFrame. Got type: {type(df)}")
             raise TypeError("Extraction must return a pandas DataFrame")
        logger.info(f"Extraction successful. DataFrame shape: {df.shape}")
        return df
    except Exception as e:
        logger.error(f"Error during data extraction: {e}", exc_info=True)
        raise


def clean_data(df_raw: pd.DataFrame) -> pd.DataFrame:
    # ... (código existente) ...
    """Tarea de limpieza: Recibe un DataFrame, lo limpia y devuelve el DataFrame limpio."""
    try:
        logger.info("Executing data cleaning task.")
        if not isinstance(df_raw, pd.DataFrame):
            logger.error(f"Input to clean_data is not a DataFrame. Got type: {type(df_raw)}. Check XCom backend and previous task output.")
            raise TypeError("clean_data requires a pandas DataFrame input.")

        logger.info(f"DataFrame received for cleaning. Shape: {df_raw.shape}")
        df_cleaned = clean_airbnb_data(df_raw)
        logger.info(f"Data cleaning completed. Cleaned DataFrame shape: {df_cleaned.shape}")
        return df_cleaned
    except Exception as e:
        logger.error(f"Error during data cleaning: {e}", exc_info=True)
        raise


def load_cleaned_data(df_cleaned: pd.DataFrame):
    # ... (código existente) ...
    """Tarea de carga: Recibe el DataFrame limpio y lo carga en la tabla final."""
    try:
        logger.info(f"Executing loading task for cleaned data into table '{FINAL_TABLE_NAME}'.")
        if not isinstance(df_cleaned, pd.DataFrame):
            logger.error(f"Input to load_cleaned_data is not a DataFrame. Got type: {type(df_cleaned)}.")
            raise TypeError("load_cleaned_data requires a pandas DataFrame input.")

        logger.info(f"Cleaned DataFrame received for loading. Shape: {df_cleaned.shape}")
        success = exe_load_data(df=df_cleaned, db_name="airbnb", table_name=FINAL_TABLE_NAME)

        if success:
            logger.info(f"Cleaned data loading into '{FINAL_TABLE_NAME}' completed successfully.")
        else:
            logger.warning(f"Cleaned data loading function reported failure for table '{FINAL_TABLE_NAME}'.")
        return success

    except Exception as e:
        logger.error(f"Error during cleaned data loading into '{FINAL_TABLE_NAME}': {e}", exc_info=True)
        raise


def create_dimensional_model():
    # ... (código existente) ...
    """
    Función lógica para la tarea: Crea las tablas del modelo dimensional
    si no existen.
    """
    engine = None
    try:
        logger.info("Executing dimensional model creation task.")
        engine = get_db_engine(db_name="airbnb")

        if engine:
            create_dimensional_model_tables(engine)
            logger.info("Dimensional model tables creation process finished.")
            return True
        else:
            logger.error("Failed to get database engine. Cannot create dimensional model.")
            return False

    except Exception as e:
        logger.error(f"Error during dimensional model creation: {e}", exc_info=True)
        raise
    finally:
        if engine:
            engine.dispose()
            logger.info("Database engine disposed after model creation.")


# --- NUEVA FUNCIÓN PARA LA TAREA DE INSERCIÓN ---
def insert_data_to_model():
    """
    Función lógica para la tarea: Inserta datos desde la tabla limpia
    hacia las tablas del modelo dimensional.
    """
    engine = None
    try:
        logger.info("Executing task to insert data into dimensional model.")
        # Obtener el engine para la base de datos 'airbnb'
        engine = get_db_engine(db_name="airbnb")

        if engine:
            # Llamar a la función orquestadora de dimensional_load.py
            success = load_dimensional_data(engine)
            logger.info(f"Insertion into dimensional model finished. Success: {success}")
            return success
        else:
            logger.error("Failed to get database engine. Cannot insert data into dimensional model.")
            return False

    except Exception as e:
        logger.error(f"Error during insertion into dimensional model: {e}", exc_info=True)
        raise # Relanzar para que la tarea falle en Airflow
    finally:
        if engine:
            engine.dispose()
            logger.info("Database engine disposed after dimensional model insertion.")
# --- FIN NUEVA FUNCIÓN ---