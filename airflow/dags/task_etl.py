import json
import pandas as pd
import logging
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

def extract_data():
    try:
        df = exe_extract_data()  
        return df
        #return df.to_json(orient="records")
    except Exception as e:
        logging.error(f"Error extracting data {e}")
        raise

def load_data(df):
    try:
        #engine = get_db_engine()
        #if not isinstance(df, pd.DataFrame):
        #    df = pd.DataFrame(df)
            
        #load_data(engine, df, "airbnb_data")
        exe_load_data(df)
        
        return True
    except Exception as e:
        logging.error(f"Error en carga de datos: {e}")
        raise

# def load_data(df):
#     try:
#         exe_load_data(df)
#         return True
#     except Exception as e:
#         logging.error(f"Error loading data {e}")
#         raise

def clean_data():
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