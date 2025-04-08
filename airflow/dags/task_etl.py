import pandas as pd
import logging
#import json
import sys
import os
from sqlalchemy import create_engine
from dotenv import load_dotenv

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..", "src")))

from extract.extract_data import exe_extract_data
from load.load_data import exe_load_data
from load.model_dimensional import ModelDimensional
from transform.dataset_clean import DatasetCleaner
from database.db2 import get_db_engine

logging.basicConfig(level=logging.INFO, format='%(asctime)s %(message)s', datefmt='%m/%d/%Y %I:%M:%S %p')
logger = logging.getLogger(__name__)

def extract_data():
    """Extracts data and returns a pandas DataFrame"""
    try:
        logger.info("Executing data extraction task.")
        df = exe_extract_data()
        logger.info(f"Extraction successful. DataFrame shape: {df.shape}")
        return df
        #return df.to_json(orient="records")
    except Exception as e:
        logging.error(f"Error during data extraction {e}", exc_info=True)
        raise

def load_data(df_to_load):
    """Loads data into the database, expects a DataFrame."""
    try:
        logger.info("Executing data loading task.")
        if not isinstance(df_to_load, pd.DataFrame):
            logger.wargning("Input to load_data is not a DataFrame. Attemping conversion.")
            
            try:
                df_to_load = pd.DataFrame(df_to_load)
            except ValueError as ve:
                logger.error(f"Could not convert input to DataFrame: {ve}")
                raise TypeError("load_data requires a pandas DataFrame or compatible structure")
        
        logger.info(f"DataFrame receibed for loading. Shape: {df_to_load.shape}")
        success = exe_load_data(df_to_load)
        if success:
            logger.info("Data loading completed successfully.")
        else:
            logger.warning("Data loading function reported failure.")
        return success
        
    except Exception as e:
        logger.error(f"Error during data loading: {e}", exc_info=True)
        raise

# def load_data(df):
#     
#     try:
#         exe_load_data(df)
#         return True
#     except Exception as e:
#         logging.error(f"Error loading data {e}")
#         raise

def clean_data():
    """Cleans the data in the PostgresSQL table."""
    #TODO?
    try:
        load_dotenv(os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..", "env", ".env")))

        user = os.getenv("POSTGRES_USER")
        password = os.getenv("POSTGRES_PASSWORD")
        host = os.getenv("POSTGRES_HOST")
        port = os.getenv("POSTGRES_PORT")
        database = os.getenv("POSTGRES_DATABASE")

        if not all([user, password, host, port, database]):
            raise ValueError("Faltan variables de entorno necesarias para la conexión a la base de datos.")


        engine = create_engine(f'postgresql://{user}:{password}@{host}:{port}/{database}')
        cleaner = DatasetCleaner(engine)
        cleaner.execute_transformations()
        logging.info("Limpieza de datos completada exitosamente.")
        return True
    except Exception as e:
        logging.error(f"Error durante la limpieza de datos: {e}")
        raise



# def migrate_to_dimensional_model():
#     try:
#         with open(os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..", "credentials.json"))) as f:
#             creds = json.load(f)

#         engine = create_engine(
#             f'postgresql://{creds["user"]}:{creds["password"]}@{creds["host"]}:{creds["port"]}/{creds["database"]}'
#         )
#         model = ModelDimensional(engine)
#         model.execute_migration()
#         logging.info("Migración al modelo dimensional completada exitosamente.")
#         return True
#     except Exception as e:
#         logging.error(f"Error durante la migración al modelo dimensional: {e}")
#         raise