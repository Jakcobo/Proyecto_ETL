import pandas as pd
import logging
from sqlalchemy import create_engine, inspect, text
from database.db import get_db_engine#, disposing_engine, load_data, load_clean_data, creating_engine

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
            method='multi',
            #chunksize=100000 #no needed for the airbnb data
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