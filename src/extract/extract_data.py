#from database.db_operations import creating_engine, disposing_engine

import pandas as pd
import logging
#import json
import os

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(message)s", datefmt="%d/%m/%Y %I:%M:%S %p")
logger = logging.getLogger(__name__)

def exe_extract_data():
    """Extracts data from CSV and returns a pandas DataFrame"""
    try:
        logger.info("Starting data extraction from CSV file.")
        #df = pd.read_csv("../../data/Airbnb_Open_Data.csv", low_memory=False, encoding='ISO-8859-1')
        project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..'))
        csv_path = os.path.join(project_root, 'data', 'Airbnb_Open_Data.csv')
        logger.info(f"Looking for CSV file at: {csv_path}")
        
        if not os.path.exists(csv_path):
            logger.error(f"CSV file not found at: {csv_path}")
            raise FileNotFoundError(f"CSV file not found at: {csv_path}")
        
        df = pd.read_csv(csv_path, low_memory=False, encoding='ISO-8859-1')
        logger.info(f"Data extracted successfully fromCSV. Shape: {df.shape}")
        #return json.loads(df.to_json(orient='records'))
        return df
    except FileNotFoundError as fnf:
        logger.info(f"Extraction failed: {fnf}")
        raise
    except Exception as e:
        logger.error(f"Error extracting data from CSV: {e}", exc_info=True)
        raise