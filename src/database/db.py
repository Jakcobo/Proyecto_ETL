# proyecto_etl/src/database/db.py
from dotenv import load_dotenv
from sqlalchemy import create_engine, inspect, Integer, Float, String, DateTime, MetaData, Table, Column, BIGINT, text #, create_sqlalchemy_engine
from sqlalchemy.exc import OperationalError, ProgrammingError
from sqlalchemy_utils import database_exists, create_database

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

def get_db_config(db_name=None):
    """Gets database configuration from enviroment variables."""
    load_enviroment_variables()
    
    config = {
        'user':     os.getenv("POSTGRES_USER"),
        'password': os.getenv("POSTGRES_PASSWORD"),
        'host':     os.getenv("POSTGRES_HOST"),
        'port':     os.getenv("POSTGRES_PORT"),
        'database': db_name if db_name else os.getenv("POSTGRES_DATABASE", "postgres")
    }
    
    missing_vars = [k for k, v in config.items() if k != 'database' and not v]
    if missing_vars:
        logger.error(f"Missing required PostgresSQL enviroment variables: {missing_vars}")
        raise ValueError(f"Missing enviroment variables: {missing_vars}")
    
    log_config = config.copy()
    if log_config.get('password'):
        log_config['password'] = '********'
    logger.debug(f"Database configuration loaded: {log_config}")
    
    return config

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
    
def get_db_engine(db_name="airbnb"):
    """
    Gets a SQLAlchemy engine for the specified database.
    Creates the database if it doesn't exists using admin credentials.
    """
    admin_engine = None
    try:
        config = get_db_config(db_name)
        admin_config = config.copy()
        admin_config['database'] = os.getenv("POSTGRES_ADMIN_DB", "postgres")
        admin_url = f"postgresql://{admin_config['user']}:{admin_config['password']}@{admin_config['host']}:{admin_config['port']}/{admin_config['database']}"
        try:
            admin_engine = create_engine(admin_url, isolation_level="AUTOCOMMIT")
            with admin_engine.connect() as conn:
                logger.info(f"Admin connection to '{admin_config['database']}' successful.")
        except OperationalError as oe:
            logger.error(f"Failed to connext to admin database '{admin_config['database']}' using provided credentials: {oe}")
            logger.error("Check the crendentials")
            raise ConnectionError("Failed to establish admin connection to PostgresSQL.") from oe
        
        check_and_create_db(admin_engine, db_name, config['user'])
        
        target_url = f"postgresql://{config['user']}:{config['password']}@{config['host']}:{config['port']}/{config['database']}"
        logger.info(f"Creating engine for target database '{db_name}'.")
        target_engine = create_engine(target_url, pool_pre_ping=True)
        
        try:
            with target_engine.connect() as conn:
                logger.info(f"Connection to target database '{db_name}' successful.")
        except OperationalError as oe:
            logger.error(f"Failed to connect to targer database '{db_name}' after creation/check: {oe}")
            raise ConnectionError(f"Failed to connect to target database '{db_name}'.") from oe
        return target_engine
    except Exception as e:
        logger.error(f"Error in get_db_engine for database '{db_name}': {e}", exc_info=True)
        raise
    finally:
        if admin_engine:
            admin_engine.dispose()
            logger.debug("Admin engine dispose.")
