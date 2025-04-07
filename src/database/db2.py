from dotenv import load_dotenv
from sqlalchemy import create_engine, inspect, MetaData, Table, Column, Integer, Float, String, DateTime, BIGINT
from sqlalchemy_utils import database_exists, create_database
import os
import logging

# Configuración de logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(message)s", datefmt="%m/%d/%Y %I:%M:%S %p")
logger = logging.getLogger(__name__)

# Cargar variables de entorno
env_path = os.path.join(os.path.dirname(__file__), '../../env/.env')
load_dotenv(env_path)

def get_db_engine(db_name="airbnb"):
    """Obtiene o crea un motor de base de datos."""
    try:
        config = {
            'user':     os.getenv("POSTGRES_USER"),
            'password': os.getenv("POSTGRES_PASSWORD"),
            'host':     os.getenv("POSTGRES_HOST"),
            'port':     os.getenv("POSTGRES_PORT"),
            'database': db_name
        }
        
        port = int(config.get('port', 5432))
        db_url = f"postgresql://{config['user']}:{config['password']}@{config['host']}:{port}/{db_name}"
        admin_url = f"postgresql://{config['user']}:{config['password']}@{config['host']}:{port}/postgres"
        
        # Verificar si la base de datos ya existe
        if not database_exists(db_url):
            logger.info(f"Creating database {db_name}...")
            create_database(db_url)
        else:
            logger.info(f"Database {db_name} already exists.")
        
        return create_engine(db_url)
    
    except Exception as e:
        logger.error(f"Error to connect PostgreSQL: {e}")
        raise

def infer_sqlalchemy_type(dtype, column_name):
    """Mapea tipos de datos de Pandas a SQLAlchemy."""
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
    """Carga datos de un DataFrame a una tabla en la base de datos."""
    try:
        logging.info(f"Creating table {table_name} from Pandas DataFrame")
        metadata = MetaData()

        if inspect(engine).has_table(table_name):
            logging.info(f"Table {table_name} exists - dropping it")
            with engine.begin() as conn:
                metadata.drop_all(conn, [Table(table_name, metadata)], checkfirst=True)
        
        columns = [Column(name, infer_sqlalchemy_type(dtype, name), primary_key=(name == primary_key)) for name, dtype in df.dtypes.items()]
        table = Table(table_name, metadata, *columns)
        table.create(engine)
        df.to_sql(table_name, con=engine, if_exists="append", index=False)
        logging.info(f"Table {table_name} successfully created!")
        return True
    except Exception as e:
        logging.error(f"Error loading data into {table_name}: {e}")
        raise



    