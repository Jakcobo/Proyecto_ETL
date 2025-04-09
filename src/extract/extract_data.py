#from database.db_operations import creating_engine, disposing_engine
import pandas as pd
import logging
import os # os ya no es necesario para construir la ruta aquí

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(message)s", datefmt="%d/%m/%Y %I:%M:%S %p")
logger = logging.getLogger(__name__)

# --- La función ahora recibe la ruta ---
def exe_extract_data(csv_path: str) -> pd.DataFrame:
    """Extracts data from the provided CSV path and returns a pandas DataFrame"""
    try:
        logger.info(f"Starting data extraction from CSV file: {csv_path}") # Usa la ruta pasada

        if not os.path.exists(csv_path): # Sigue validando la existencia
            logger.error(f"CSV file not found at: {csv_path}")
            raise FileNotFoundError(f"CSV file not found at: {csv_path}")

        df = pd.read_csv(csv_path, low_memory=False, encoding='ISO-8859-1')
        logger.info(f"Data extracted successfully from CSV. Shape: {df.shape}")
        return df
    except FileNotFoundError as fnf:
        logger.info(f"Extraction failed: {fnf}")
        raise
    except Exception as e:
        logger.error(f"Error extracting data from CSV '{csv_path}': {e}", exc_info=True)
        raise