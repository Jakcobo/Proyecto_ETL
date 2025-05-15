# proyecto_etl/src/load/load_data.py
import pandas as pd
import logging
from sqlalchemy import create_engine, inspect, text
from database.db import get_db_engine#, disposing_engine, load_data, load_clean_data, creating_engine
from database.modeldb import apiPlace

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(message)s", datefmt="%d/%m/%Y %I:%M:%S %p")
logger = logging.getLogger(__name__)


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

def load_api_places_to_db(df: pd.DataFrame, db_name: str = "airbnb", table_name: str = "api_data"):
    
    """
    Carga un DataFrame de lugares de Foursquare en la tabla 'api_places'.
    La tabla debe existir y el DataFrame debe tener las columnas correspondientes.
    Utiliza 'append' para añadir nuevos datos. Considerar estrategias para evitar duplicados.
    """
    engine = None
    if df.empty:
        logger.warning("DataFrame para cargar está vacío. No se realizará ninguna carga.")
        return

    table_name = apiPlace.__tablename__

    df_to_load = df 

    logger.info(f"Intentando cargar {len(df_to_load)} registros en la tabla '{table_name}'.")
    logger.debug(f"Columnas a cargar: {df_to_load.columns.tolist()}")
    # logger.debug(f"Primeras filas a cargar:\n{df_to_load.head()}")

    try:
        # Usar to_sql para carga masiva. if_exists='append' añade los datos.
        # if_exists='replace' borraría y recrearía la tabla (¡peligroso!).
        # Para evitar duplicados con 'append', necesitarías:
        # 1. Una constraint UNIQUE en la BD (ej. en 'fsq_id').
        # 2. Lógica de pre-limpieza o un upsert (más complejo con to_sql directamente).
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
        logger.info(f"{len(df_to_load)} registros cargados exitosamente en '{table_name}'.")
    except Exception as e:
        # Podría ser una violación de constraint UNIQUE si 'fsq_id' ya existe.
        logger.error(f"Error al cargar datos en la tabla '{table_name}': {e}", exc_info=True)
        # Aquí podrías implementar lógica de reintento o manejo específico del error
        raise