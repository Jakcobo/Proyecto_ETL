import pandas as pd
import logging
from sqlalchemy import create_engine, inspect, text
from database.db import get_db_engine#, disposing_engine, load_data, load_clean_data, creating_engine

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(message)s", datefmt="%d/%m/%Y %I:%M:%S %p")
logger = logging.getLogger(__name__)


# Ejemplo de modificación en src/load/load_data.py
# def exe_load_data(df: pd.DataFrame, table_name: str, engine_or_conn_id): # Aceptar engine o conn_id
#     engine = None
#     created_engine_here = False
#     try:
#         if isinstance(engine_or_conn_id, str): # Si es conn_id, obtener engine
#             logger.info(f"Obtaining engine using conn_id '{engine_or_conn_id}' inside exe_load_data.")
#             engine = get_db_engine(airflow_conn_id=engine_or_conn_id, use_airflow_conn=True)
#             created_engine_here = True
#         else: # Asumir que es un engine existente
#              logger.info("Using pre-existing engine passed to exe_load_data.")
#              engine = engine_or_conn_id

#         if not engine:
#              raise ConnectionError("Could not obtain database engine.")

#         logger.info(f"Loading data into table '{table_name}' with strategy 'replace'.")
#         # ... (df.to_sql usando el engine obtenido) ...
#         df.to_sql(name=table_name, con=engine, ...)
#         return True
#     # ... (except clauses) ...
#     finally:
#         if engine and created_engine_here: # Solo dispose si se creó aquí
#              logger.info("Disposing database engine created within exe_load_data.")
#              engine.dispose()

def exe_load_data(df: pd.DataFrame, db_name: str = "airbnb", table_name: str = "airbnb_data"):
    """
    Loads DataFrame into the specified database and table.
    Creates the database if it doesn't exist.
    Replaces the table if it exists (drop and recreates).
    """
    
    logger.info(f"Starting data load into database '{db_name}', table '{table_name}'.")
    engine = None
    
    if not isinstance(df, pd.DataFrame):
        logger.error("Invalid input: exe_load_data requires a pandas DataFrame.")
        raise TypeError("Invalid input: exe_load_data requires a pandas DataFrame.")
    
    if df.empty:
        logger.warning("Input DataFrame is empy. Skipping database load.")
        return False

    try:
        engine = get_db_engine(db_name)
        logger.info(f"Loading data into table '{table_name}' with strategy 'replace'.")
        df.to_sql(
            name=table_name,
            con=engine,
            if_exists="replace",
            index=False,
              method  ='multi',
            chunksize=40000
            )
        
        logger.info(f"Data loaded successfully into table '{table_name}'.")
        return True

    except Exception as e:
        logger.error(f"Error loading data into table '{table_name}': {e}", exc_info=True)
        raise

    finally:
        if engine:
            logger.info("Disposing databse engine.")
            engine.dispose()