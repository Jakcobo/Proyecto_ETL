#from database.db_operations import creating_engine, disposing_engine

import pandas as pd
import logging

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(message)s", datefmt="%d/%m/%Y %I:%M:%S %p")

def exe_extract_data():
#    engine = creating_engine()
    try:
        logging.info("Starting to extract the data from the csv file")
        df = pd.read_csv("../data/Airbnb_Open_Data.csv", low_memory=False, encoding='ISO-8859-1')
        return df
    except Exception as e:
        logging.error(f"Error extracting data from the csv file: {e}.")
#    finally
#        disposing_engine(engine)
