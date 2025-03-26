import json
import pandas as pd
import logging

from src.extract.extract_data import exe_extract_data

logging.basicConfig(level=logging.INFO, format='%(asctime)s %(message)s', datefmt='%m/%d/%Y %I:%M:%S %p')

def extract_data():
    try:
        df = exe_extract_data()
        return df.to_json(orient="records")
    except Exception as e:
        logging.error(f"Error extracting data {e}")
