#from database.db_operations import creating_engine, disposing_engine

import pandas as pd
import logging
import json
import os

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(message)s", datefmt="%d/%m/%Y %I:%M:%S %p")

def exe_extract_data():
    try:
        logging.info("Starting to extract the data from the csv file")
        #df = pd.read_csv("../../data/Airbnb_Open_Data.csv", low_memory=False, encoding='ISO-8859-1')
        project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..'))

        csv_path = os.path.join(project_root, 'data', 'Airbnb_Open_Data.csv')
        logging.info(f"Looking for CSV file at: {csv_path}")
        
        if not os.path.exists(csv_path):
            raise FileNotFoundError(f"CSV file not found at: {csv_path}")
        
        df = pd.read_csv(csv_path, low_memory=False, encoding='ISO-8859-1')
        logging.info("The data from the csv file were extracted")
        return json.loads(df.to_json(orient='records'))
        #return df
    except Exception as e:
        logging.error(f"Error extracting data from the csv file: {e}.")
        raise