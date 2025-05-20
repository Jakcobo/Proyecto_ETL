# Proyecto_ETL/src/database/db.py
from dotenv import load_dotenv
from sqlalchemy import create_engine, inspect, text
from sqlalchemy.exc import OperationalError, ProgrammingError
# from sqlalchemy_utils import database_exists, create_database # Podríamos no necesitar esto para Render

import os
import logging
# import warnings # No se usa

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s [%(name)s] %(message)s", datefmt="%Y-%m-%d %H:%M:%S")
logger = logging.getLogger(__name__)

def load_environment_variables():
    """Loads environment variables from .env file"""
    script_dir = os.path.dirname(__file__)
    env_path_dev = os.path.abspath(os.path.join(script_dir, '..', '..', 'env', '.env'))
    
    loaded = load_dotenv(dotenv_path=env_path_dev, verbose=True)
    logger.debug(f"Attempting to load environment variables from {env_path_dev}, Loaded: {loaded}")

    if not loaded:
        env_path_project_root = os.path.abspath(os.path.join(os.getcwd(), '.env'))
        print(env_path_project_root)
        loaded = load_dotenv(dotenv_path=env_path_project_root, verbose=True)
        logger.debug(f"Attempting to load environment variables from {env_path_project_root}, Loaded: {loaded}")

    if not loaded:
        loaded = load_dotenv(verbose=True)
        logger.debug(f"Attempting to load environment variables from default locations, Loaded: {loaded}")

    if not loaded:
        logger.warning(f"Could not find .env file at expected paths or default locations. Relying on system environment variables.")
    return loaded

def get_db_config(db_name=None):
    """Gets database configuration from environment variables."""
    load_environment_variables()

    config = {
        'user':     os.getenv("POSTGRES_USER"),
        'password': os.getenv("POSTGRES_PASSWORD"),
        'host':     os.getenv("POSTGRES_HOST"),
        'port':     os.getenv("POSTGRES_PORT", "5432"),
        'database': db_name if db_name else os.getenv("POSTGRES_DATABASE")
    }

    required_vars_for_connection = ['user', 'password', 'host', 'port']
    missing_vars = [k for k in required_vars_for_connection if not config.get(k)]
    
    if missing_vars:
        logger.error(f"Missing required PostgreSQL environment variables for connection: {missing_vars}")
        raise ValueError(f"Missing environment variables for connection: {missing_vars}")
    
    if not config['database']:
        logger.error("POSTGRES_DATABASE environment variable is not set and no db_name was provided.")
        raise ValueError("Database name is not configured.")

    log_config = config.copy()
    if log_config.get('password'):
        log_config['password'] = '********'
    logger.debug(f"Database configuration loaded: {log_config}")

    return config

def check_and_create_db_if_needed(admin_engine, db_name_to_check, owner_user, is_render_env=False):
    """
    Checks if a database exists. If running locally (not Render) and it doesn't exist,
    it attempts to create it. On Render, this creation step is skipped.
    """
    logger.info(f"Checking if database '{db_name_to_check}' exists...")
    try:
        with admin_engine.connect() as conn:
            # No necesitamos AUTOCOMMIT para un SELECT
            # conn = conn.execution_options(isolation_level="AUTOCOMMIT")
            result = conn.execute(text(f"SELECT 1 FROM pg_database WHERE datname = :dbname"), {'dbname': db_name_to_check})
            exists = result.scalar() == 1

            if not exists:
                if is_render_env:
                    logger.warning(f"Database '{db_name_to_check}' does not exist. On Render, database creation is handled by Render. Ensure it's created via Render dashboard.")
                    # Podrías lanzar un error si es crítico que exista y no lo hace
                    # raise ValueError(f"Database '{db_name_to_check}' not found on Render environment.")
                else:
                    logger.info(f"Database '{db_name_to_check}' does not exist. Attempting to create (local environment)...")
                    # Re-enable autocommit for CREATE DATABASE
                    conn_autocommit = admin_engine.connect().execution_options(isolation_level="AUTOCOMMIT")
                    try:
                        conn_autocommit.execute(text(f'CREATE DATABASE "{db_name_to_check}" OWNER "{owner_user}"'))
                        logger.info(f"Database '{db_name_to_check}' created successfully with owner '{owner_user}'")
                    finally:
                        conn_autocommit.close()
            else:
                logger.info(f"Database '{db_name_to_check}' already exists.")
            return True
    except ProgrammingError as pe:
        # Si el error es "permission denied" para acceder a pg_database, es un problema.
        # Si es "database ... does not exist" durante el CREATE DATABASE, es un error de creación.
        logger.error(f"Database error during check/create for '{db_name_to_check}': {pe}")
        if "permission denied for table pg_database" in str(pe).lower():
            logger.warning("Permission denied to query pg_database. The user might not have sufficient privileges for this check.")
            logger.warning("Assuming database exists or will be connected to directly if this is a managed environment like Render.")
            return True
        raise
    except Exception as e:
        logger.error(f"Unexpected error during database check/create for '{db_name_to_check}': {e}", exc_info=True)
        raise

def get_db_engine(db_name_target: str):
    """
    Gets a SQLAlchemy engine for the specified target database.
    If not on Render, it attempts to create the database if it doesn't exist using admin credentials.
    """
    admin_engine = None
    # Detectar si estamos en un entorno Render (puedes usar una variable de entorno como RENDER="true")
    IS_RENDER_ENVIRONMENT = os.getenv("RENDER", "false").lower() == "true"
    logger.info(f"Running in Render environment: {IS_RENDER_ENVIRONMENT}")

    try:
        # Configuración para la base de datos objetivo
        target_config = get_db_config(db_name=db_name_target)

        if not IS_RENDER_ENVIRONMENT:
            # Lógica de conexión administrativa y creación de DB (solo para entornos no-Render)
            admin_db_name = os.getenv("POSTGRES_ADMIN_DB", "postgres") # DB para la conexión de admin
            if not admin_db_name: # Asegurarse de que POSTGRES_ADMIN_DB esté seteado para entornos locales
                 raise ValueError("POSTGRES_ADMIN_DB environment variable is not set for local admin connection.")

            admin_connect_config = target_config.copy() # Usar mismo user/pass/host/port
            admin_connect_config['database'] = admin_db_name

            admin_url = f"postgresql://{admin_connect_config['user']}:{admin_connect_config['password']}@{admin_connect_config['host']}:{admin_connect_config['port']}/{admin_connect_config['database']}"
            try:
                # No es necesario isolation_level="AUTOCOMMIT" para el engine, sino para la conexión al ejecutar CREATE DATABASE
                admin_engine = create_engine(admin_url)
                with admin_engine.connect() as conn: # Probar conexión
                    logger.info(f"Admin connection to '{admin_connect_config['database']}' successful.")
            except OperationalError as oe:
                logger.error(f"Failed to connect to admin database '{admin_connect_config['database']}' using provided credentials: {oe}")
                logger.error("Check credentials and ensure POSTGRES_ADMIN_DB is accessible.")
                raise ConnectionError("Failed to establish admin connection to PostgreSQL.") from oe

            # Verificar y opcionalmente crear la base de datos objetivo
            check_and_create_db_if_needed(admin_engine, target_config['database'], target_config['user'], IS_RENDER_ENVIRONMENT)
        else:
            logger.info(f"Skipping admin connection and DB creation check for Render environment. Assuming '{target_config['database']}' exists.")


        # Crear y probar engine para la base de datos objetivo
        target_url = f"postgresql://{target_config['user']}:{target_config['password']}@{target_config['host']}:{target_config['port']}/{target_config['database']}"
        logger.info(f"Creating engine for target database '{target_config['database']}'.")
        # Opciones de pool son buenas prácticas
        target_engine = create_engine(target_url, pool_pre_ping=True, pool_recycle=1800, connect_args={'sslmode':'prefer'})


        try:
            with target_engine.connect() as conn:
                logger.info(f"Connection to target database '{target_config['database']}' successful.")
        except OperationalError as oe:
            logger.error(f"Failed to connect to target database '{target_config['database']}': {oe}")
            # Podría ser un problema de SSL si Render lo requiere y no está configurado
            if "SSL connection has been closed unexpectedly" in str(oe) or "server does not support SSL" in str(oe):
                 logger.warning("SSL issue detected. Ensure 'sslmode' is correctly set in connect_args if Render requires/disables SSL for external connections.")
            raise ConnectionError(f"Failed to connect to target database '{target_config['database']}'.") from oe
        return target_engine

    except Exception as e:
        logger.error(f"Error in get_db_engine for database '{db_name_target}': {e}", exc_info=True)
        raise
    finally:
        if admin_engine:
            admin_engine.dispose()
            logger.debug("Admin engine disposed.")

# if __name__ == '__main__':
#     logger.info("--- Iniciando prueba local de get_db_engine ---")
#     try:
#         os.environ["RENDER"] = "true"
#         TARGET_DB = os.getenv("POSTGRES_DATABASE")
#         if not TARGET_DB:
#             raise ValueError("POSTGRES_DATABASE no está seteado para la prueba.")
# 
#         engine = get_db_engine(db_name_target=TARGET_DB)
# 
#         if engine:
#             logger.info(f"Prueba de get_db_engine exitosa. Engine para '{TARGET_DB}' obtenido.")
#             # Podrías hacer una consulta simple
#             with engine.connect() as connection:
#                 result = connection.execute(text("SELECT version();"))
#                 db_version = result.scalar_one()
#                 logger.info(f"PostgreSQL Version (desde {TARGET_DB}): {db_version}")
#         else:
#             logger.error("La prueba de get_db_engine falló, no se obtuvo ningún engine.")
# 
#     except ValueError as ve:
#         logger.error(f"Error de configuración: {ve}")
#     except ConnectionError as ce:
#         logger.error(f"Error de conexión: {ce}")
#     except Exception as e:
#         logger.error(f"Error inesperado durante la prueba: {e}", exc_info=True)
#     finally:
#         logger.info("--- Prueba local de get_db_engine finalizada ---")