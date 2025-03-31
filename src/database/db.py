#from env import env
from dotenv import load_dotenv
from sqlalchemy import create_engine as create_sqlalchemy_engine
from sqlalchemy import create_engine, Integer, Float, String, DateTime, inspect, MetaData, Table, Column, BIGINT
from sqlalchemy_utils import database_exists, create_database

import os
import logging
import warnings

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(message)s", datefmt="%m/%d/%Y %I:%M:%S %p")

def load_environment():
    try:
        env_path = os.path.join(os.path.dirname(__file__), '../env/.env')
        if os.path.exists(env_path):
            load_dotenv(env_path)
        else:
            load_dotenv()
    except Exception as e:
        logging.error(f"Error loading the credentials file: {e}")
        raise

load_environment()

def get_db_connection_params():
    params = {
        'user':     os.getenv("USER"),
        'password': os.getenv("PASSWORD"),
        'host':     os.getenv("HOST"),
        'port':     os.getenv("PORT"),
        'database': os.getenv("DATABASE")
    }
    
    if not all(params.values()):
        raise ValueError("Missing required database connection parameters")
    return params

def creating_engine(database=None):
    
    try:
        params = get_db_connection_params()
        if database:
            params['database']=database

        url = f"postgresql://{params['user']}:{params['password']}@{params['host']}:{params['port']}/{database}"
        engine = create_engine(url)
        
        if not database_exists(engine.url):
            create_database(engine.url)
            logging.info(f"Database {database} created")
        
        logging.info("Engine created. You can now connect to the database.")
        
        return engine
        
    except Exception as e:
        logging.error(f"Error creating database engine: {e}")
        raise

def disposing_engine(engine):
    if engine:
        engine.dispose()
        logging.info("Engine disposed.")


# Function for infer sql types
def infer_sqlalchemy_type(dtype, column_name):
    """ Map pandas dtype to SQLAlchemy's types """
    
    if column_name == "eventid":
        return BIGINT
    elif "int" in dtype.name:
        return Integer
    elif "float" in dtype.name:
        return Float
    elif "object" in dtype.name:
        return String(500)
    elif "datetime" in dtype.name:
        return DateTime
    else:
        return String(500)

def load_data(engine, df, table_name, primary_key="eventid"):
    
    try:
        logging.info(f"Creating table {table_name} from Pandas DataFrame")
        metadata = MetaData()

        if inspect(engine).has_table(table_name):
            logging.info(f"Table {table_name} exists - dropping it")
            with engine.begin() as conn:
                metadata.drop_all(conn, [Table(table_name, metadata)], checkfirst=True)
        
        columns = [Column(name,
            infer_sqlalchemy_type(dtype, name),
            primary_key=(name == primary_key)) for name, dtype in df.dtypes.items()]
        
        table = Table(table_name, metadata, *columns)
        table.create(engine)
        df.to_sql(table_name, con=engine, if_exists="append", index=False)
        logging.info(f"Table {table_name} succesfully created!")
        return True
    except Exception as e:
        logging.error(f"Error loading data into {table_name}: {e}")
        raise

# Function to create table
def load_clean_data(engine, df, table_name, primary_key="eventid"):
    
    try:
        logging.info(f"Creating table {table_name} DWH")
        
        if not inspect(engine).has_table(table_name):
            metadata = MetaData()
            columns = [Column(name,
                infer_sqlalchemy_type(dtype, name),
                primary_key=(name == primary_key)) for name, dtype in df.dtypes.items()]
            
            table = Table(table_name, metadata, *columns)
            table.create(engine)
            df.to_sql(table_name, con=engine, if_exists="append", index=False, chunksize=1000)
            logging.info(f"Table {table_name} succesfully created!")

            return True
        else:
            warnings.warn(f"Table {table_name} already exists.")
    
    except Exception as e:
        logging.error(f"Error loading data into {table_name}: {e}")
        raise