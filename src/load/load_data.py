import pandas as pd
import logging

from database.db import creating_engine, disposing_engine, load_data, load_clean_data

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(message)s", datefmt="%d/%m/%Y %I:%M:%S %p")

def exe_load_data(df):
    
    logging.info("Starting to load the data.")
    engine = None

    try:
        #df = pd.DataFrame(df)
        engine = creating_engine(database="airbnbn")
        load_data(engine, df, "airbnb_data")
        logging.info("Data successfully loaded.")
        return True
    except Exception as e:
        logging.error(f"Error loading data: {e}")
        return None
    finally:
        if engine is not None:
            disposing_engine(engine)