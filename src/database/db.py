from dotenv import load_dotenv
from sqlalchemy import create_engine, inspect, Integer, Float, String, DateTime, MetaData, Table, Column, BIGINT, text #, create_sqlalchemy_engine
from sqlalchemy.exc import OperationalError, ProgrammingError
from sqlalchemy_utils import database_exists, create_database
from airflow.hooks.base import BaseHook # <--- Importar Hook
from airflow.exceptions import AirflowNotFoundException # <--- Para manejar conexión no encontrada

import os
import logging
import warnings

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(message)s", datefmt="%m/%d/%Y %I:%M:%S %p")
logger = logging.getLogger(__name__)

def load_enviroment_variables():
    """Loads enviroment variables from .env file"""
    env_path = os.path.abspath(os.path.join(os.path.dirname(__file__), '..','..','env','.env')) #testing path
    #env_path = os.path.join(os.path.dirname(__file__), '..','..','env','.env') #testing path
    #env_path = os.path.join(os.path.dirname(__file__), '../../env/.env') #testing path
    logger.debug(f"Attempinting to load enviroment variables from {env_path}")
    loaded = load_dotenv(dotenv_path=env_path, verbose=True)
    if not loaded:
        loaded = load_dotenv(verbose=True)
        logger.warning(f"Could not find .env file at expected path {env_path} or default locations.")

    return loaded

# --- Nueva función para obtener config desde Airflow Connection ---
def get_db_config_from_airflow(connection_id: str, db_name_override: str = None):
    """Gets database configuration from an Airflow Connection."""
    try:
        logger.debug(f"Attempting to retrieve Airflow connection: {connection_id}")
        conn = BaseHook.get_connection(connection_id)

        config = {
            'user': conn.login,
            'password': conn.password,
            'host': conn.host,
            'port': conn.port,
            # Usa el schema de la conexión como nombre de DB, permite override
            'database': db_name_override if db_name_override else conn.schema
        }
        missing_vars = [k for k, v in config.items() if k != 'password' and not v] # Password puede ser vacío
        if missing_vars:
             logger.error(f"Airflow Connection '{connection_id}' is missing required fields: {missing_vars}")
             raise ValueError(f"Airflow Connection '{connection_id}' incomplete.")

        log_config = config.copy()
        if log_config.get('password'): log_config['password'] = '********'
        logger.debug(f"Database configuration loaded from Airflow connection '{connection_id}': {log_config}")
        return config

    except AirflowNotFoundException:
        logger.error(f"Airflow Connection with ID '{connection_id}' not found.")
        raise
    except Exception as e:
        logger.error(f"Failed to parse Airflow Connection '{connection_id}': {e}", exc_info=True)
        raise

def check_and_create_db(admin_engine, db_name, owner):
    """Checks if a database exists and creates it if not."""
    logger.info(f"Checking if database '{db_name}' exists...")
    try:
        with admin_engine.connect() as conn:
            conn = conn.execution_options(isolation_level="AUTOCOMMIT")
            result = conn.execute(text(f"SELECT 1 FROM pg_database WHERE datname = :dbname"), {'dbname': db_name})
            exists = result.scalar() == 1
            
            if not exists:
                logger.info(f"Database '{db_name}' does not exist. Attemping to create...")
                conn.execute(text(f'CREATE DATABASE "{db_name}" OWNER "{owner}"'))
                logger.info(f"Database '{db_name}' created successfully with owner '{owner}'")
            else:
                logger.info(f"Database '{db_name}' already exists.")
            return True
    except ProgrammingError as pe:
        logger.error(f"Database error during check/create for '{db_name}': {pe}")
        raise
    except Exception as e:
        logger.error("Unexpected error during database check/create for '{db_name}': {e}", exc_info=True)
        raise
    
# --- Modificar get_db_engine para priorizar Airflow Connection ---
def get_db_engine(db_name="airbnb", airflow_conn_id="postgres_airbnb", use_airflow_conn=True):
    """
    Gets a SQLAlchemy engine.
    Prioritizes Airflow Connection if use_airflow_conn is True and connection exists.
    Falls back to .env configuration otherwise.
    """
    admin_engine = None
    config = None
    admin_config = None
    using_airflow = False

    if use_airflow_conn:
        try:
            # Intenta obtener config principal y de admin (asumiendo otra conexión o la misma)
            # Para simplificar, usaremos la misma conexión y cambiaremos 'database'
            admin_db_name = os.getenv("POSTGRES_ADMIN_DB", "postgres") # Aún podemos obtener esto de .env o Variable
            config = get_db_config_from_airflow(airflow_conn_id, db_name_override=db_name)
            admin_config = get_db_config_from_airflow(airflow_conn_id, db_name_override=admin_db_name)
            logger.info(f"Using Airflow connection '{airflow_conn_id}' for database '{db_name}' and admin db '{admin_db_name}'.")
            using_airflow = True
        except (AirflowNotFoundException, ValueError) as e:
            logger.warning(f"Could not use Airflow connection '{airflow_conn_id}': {e}. Falling back to .env file.")
            # Si falla, carga desde .env como antes
            config = get_db_config(db_name)
            admin_config_env = get_db_config(os.getenv("POSTGRES_ADMIN_DB", "postgres")) # Carga config admin desde env
            admin_config = admin_config_env


    else: # Si no se quiere usar Airflow Conn, cargar de .env
         logger.info("Using .env file configuration for database.")
         config = get_db_config(db_name)
         admin_config = get_db_config(os.getenv("POSTGRES_ADMIN_DB", "postgres"))

    # --- El resto de la lógica es similar, pero usa las config obtenidas ---
    try:
        # Construir URLs desde el diccionario de configuración
        admin_url = URL.create(
            drivername="postgresql",
            username=admin_config['user'],
            password=admin_config['password'],
            host=admin_config['host'],
            port=admin_config['port'],
            database=admin_config['database']
        )
        try:
            admin_engine = create_engine(admin_url, isolation_level="AUTOCOMMIT")
            with admin_engine.connect() as conn:
                logger.info(f"Admin connection to '{admin_config['database']}' successful.")
        except Exception as oe: # Captura OperationalError y otras
             # Log más detallado
             logger.error(f"Failed to connect to admin database '{admin_config['database']}' using {'Airflow Connection' if using_airflow else '.env'} credentials.")
             logger.error(f"Error details: {oe}", exc_info=True) # Loguear traceback
             raise ConnectionError("Failed to establish admin connection to PostgreSQL.") from oe


        check_and_create_db(admin_engine, db_name, config['user']) # Pasa el usuario correcto

        target_url = URL.create(
            drivername="postgresql",
            username=config['user'],
            password=config['password'],
            host=config['host'],
            port=config['port'],
            database=config['database']
        )
        logger.info(f"Creating engine for target database '{db_name}'.")
        target_engine = create_engine(target_url, pool_pre_ping=True)

        try:
            with target_engine.connect() as conn:
                logger.info(f"Connection to target database '{db_name}' successful.")
        except Exception as oe:
            logger.error(f"Failed to connect to target database '{db_name}' after creation/check: {oe}", exc_info=True)
            raise ConnectionError(f"Failed to connect to target database '{db_name}'.") from oe
        return target_engine
    except Exception as e:
        logger.error(f"Error in get_db_engine for database '{db_name}': {e}", exc_info=True)
        raise
    finally:
        if admin_engine:
            admin_engine.dispose()
            logger.debug("Admin engine disposed.")