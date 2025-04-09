import pandas as pd
import logging
import sys
import os
#from sqlalchemy import create_engine
#from dotenv import load_dotenv

logging.basicConfig(level=logging.INFO, format='%(asctime)s %(message)s', datefmt='%m/%d/%Y %I:%M:%S %p')
logger = logging.getLogger(__name__)

try:
    src_path = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..", "src"))
    if src_path not in sys.path:
        sys.path.append(src_path)
    from extract.extract_data import exe_extract_data
    from load.load_data import exe_load_data
    from transform.dataset_clean import clean_airbnb_data
    from database.modeldb import create_dimensional_model_tables
    #from database.db import get_db_engine
except ImportError as e:
    logger.error(f"Error importing project modules: {e}. Check sys.path and project structure relative to Airflow execution.")
    raise

FINAL_TABLE_NAME = "airbnb_cleaned"

def extract_data() -> pd.DataFrame:
    """Tarea de extracción: Extrae datos y devuelve un DataFrame."""
    try:
        logger.info("Executing data extraction task.")
        df = exe_extract_data()
        if not isinstance(df, pd.DataFrame):
            logger.error(f"Extraction function did not return a pandas DataFrame. Got type: {type(df)}")
            raise TypeError("Extraction must return a pandas DataFrame")
        logger.info(f"Extraction successful. DataFrame shape: {df.shape}")
        return df
    except Exception as e:
        logger.error(f"Error during data extraction: {e}", exc_info=True)
        raise

def clean_data(df_raw: pd.DataFrame) -> pd.DataFrame:
    """Tarea de limpieza: Recibe un DataFrame, lo limpia y devuelve el DataFrame limpio."""
    try:
        logger.info("Executing data cleaning task.")
        if not isinstance(df_raw, pd.DataFrame):
            logger.error(f"Input to clean_data is not a DataFrame. Got type: {type(df_raw)}. Check XCom backend and previous task output.")
            # Intentar cargar desde JSON si se pasó como JSON (menos ideal con TaskFlow)
            # try:
            #     df_raw = pd.read_json(df_raw, orient='records')
            #     logger.info("Successfully loaded DataFrame from JSON fallback.")
            # except Exception as json_e:
            #     logger.error(f"Could not convert input to DataFrame: {json_e}")
            raise TypeError("clean_data requires a pandas DataFrame input.")

        logger.info(f"DataFrame received for cleaning. Shape: {df_raw.shape}")
        df_cleaned = clean_airbnb_data(df_raw)
        logger.info(f"Data cleaning completed. Cleaned DataFrame shape: {df_cleaned.shape}")
        return df_cleaned
    except Exception as e:
        logger.error(f"Error during data cleaning: {e}", exc_info=True)
        raise

def create_dimensional_model():
    """
    Función lógica para la tarea: Crea las tablas del modelo dimensional si no existen.
    """
    try:
        logger.info("Executing dimensional model creation task.")
        create_dimensional_model_tables("airbnb")        
        logger.info("Dimensional model tables creation process finished.")

    except Exception as e:
        logger.error(f"Error during dimensional model creation: {e}", exc_info=True)
        raise # Relanzar para que la tarea falle en Airflow
    
def migrate_to_dimensional_model(df_cleaned: pd.DataFrame):
    success = True
    return success

def load_cleaned_data(df_cleaned: pd.DataFrame):
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

