from dotenv import load_dotenv
from sqlalchemy import create_engine, Integer, Float, String, DateTime, inspect, MetaData, Table, Column, BIGINT #, create_sqlalchemy_engine
from sqlalchemy_utils import database_exists, create_database

import os
import logging
import warnings

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(message)s", datefmt="%m/%d/%Y %I:%M:%S %p")
logger = logging.getLogger(__name__)

def get_or_create_database(db_name="airbnb"):
    try:
        params = {
            'user':     os.getenv("USER"),
            'password': os.getenv("PASSWORD"),
            'host':     os.getenv("HOST"),
            'port':     os.getenv("PORT"),
            'database': os.getenv("DATABASE")
        }
        
        admin_url = f"postgresql://{params['user']}:{params['password']}@{params['host']}:{params['port']}/postgres"
        engine = create_engine(admin_url)
        
        if not database_exists(f"postgresql://{params['user']}:{params['password']}@{params['host']}:{params['port']}/{db_name}"):
            create_database(engine.url, template="template0", encoding="UTF8", owner=params['user'])
            logging.info(f"Base de datos '{db_name}' creada exitosamente")
        
        params['database'] = db_name
        return params
        
    except Exception as e:
        logging.error(f"Error al crear la base de datos: {e}")
        raise

def load_environment():
    try:
        env_path = os.path.join(os.path.dirname(__file__), '../env/.env')
        load_dotenv(env_path)
        
        config = {
            'user':     os.getenv("USER"),
            'password': os.getenv("PASSWORD"),
            'host':     os.getenv("HOST"),
            'port':     os.getenv("PORT"),
            'database': os.getenv("DATABASE")
        }
        logging.info(f"Configuration loaded: {{k:v from f,v in config.items() if k!= 'password'}}")
        return config
    except Exception as e:
        logging.error(f"Error loading the credentials file: {e}")
        raise

#load_environment()

#def get_db_connection_params():

#    missing = [k for k, v in params.items() if not v]
#    if missing:
#        logging.error(f"Faltan parámetros: {missing}")
#        raise ValueError(f"Missing required database parameters: {missing}")
    
#    return params

def get_db_engine(db_name="airbnb"):
    try:
        env_path = os.path.join(os.path.dirname(__file__), '../../env/.env')
        load_dotenv(env_path)
        
        config = {
            'user': os.getenv("POSTGRES_USER"),           #, "postgres"),
            'password': os.getenv("POSTGRES_PASSWORD"),   #, "postgres"),
            'host': os.getenv("POSTGRES_HOST"),           #, "localhost"),
            'port': os.getenv("POSTGRES_PORT"),           #, "5432"),
            'database': db_name
        }
        
        # First connect to default 'postgres' database to check/create our DB
        admin_url = f"postgresql://{config['user']}:{config['password']}@{config['host']}:{config['port']}/postgres"
        admin_engine = create_engine(admin_url)
        
        # Check if database exists, create if not
        with admin_engine.connect() as conn:
            conn.execution_options(isolation_level="AUTOCOMMIT")
            result = conn.execute(f"SELECT 1 FROM pg_database WHERE datname='{db_name}'")
            if not result.fetchone():
                conn.execute(f"CREATE DATABASE {db_name}")
                logger.info(f"Created database {db_name}")
        
        # Now create engine for our target database
        db_url = f"postgresql://{config['user']}:{config['password']}@{config['host']}:{config['port']}/{db_name}"
        return create_engine(db_url, pool_pre_ping=True)
        
    except Exception as e:
        logger.error(f"Error connecting to PostgreSQL: {e}")
        raise
    finally:
        if 'admin_engine' in locals():
            admin_engine.dispose()

def creating_engine(database='airbnb'):
    
    try:
        params = load_environment()
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
        #df.to_sql(table_name, con=engine, if_exists="append", index=False)
        df.to_sql(
        name=table_name,
        con=str(engine.url),      # <-- aquí pasas la URI, no el objeto Engine
        if_exists="append",
        index=False,
        method="multi"
        )
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