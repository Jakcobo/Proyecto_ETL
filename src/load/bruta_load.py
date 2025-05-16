# /home/nicolas/Escritorio/proyecto ETL/develop/src/load/bruta_load.py

import pandas as pd
import logging
import os
from sqlalchemy.exc import SQLAlchemyError

# Asegurarse de que src esté en el path para importar db
import sys
SRC_PATH = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
if SRC_PATH not in sys.path:
    sys.path.append(SRC_PATH)

try:
    from database.db import get_db_engine
except ImportError:
    # Este bloque es para ayudar si hay problemas con la ruta al ejecutar directamente
    # para pruebas, Airflow debería manejar esto correctamente.
    logger_fallback = logging.getLogger(__name__ + "_fallback")
    logger_fallback.error("Error importando get_db_engine. Asegúrate que 'src' esté en PYTHONPATH o la estructura sea correcta.")
    # Intenta una importación relativa si falla la absoluta (útil para pruebas directas a veces)
    # from ..database.db import get_db_engine # Esto puede no funcionar dependiendo de cómo se ejecute

logger = logging.getLogger(__name__)

def load_single_raw_dataframe_to_db(df: pd.DataFrame, table_name: str, engine, if_exists_strategy: str = "replace"):
    """
    Carga un único DataFrame a una tabla específica en la base de datos.

    Args:
        df (pd.DataFrame): El DataFrame a cargar.
        table_name (str): El nombre de la tabla destino.
        engine: El engine de SQLAlchemy para la conexión a la BD.
        if_exists_strategy (str): Estrategia si la tabla ya existe ('replace', 'append', 'fail').
    """
    if df.empty:
        logger.warning(f"El DataFrame para la tabla '{table_name}' está vacío. No se realizará la carga.")
        return False

    try:
        logger.info(f"Iniciando carga del DataFrame en la tabla '{table_name}' con estrategia '{if_exists_strategy}'. Shape: {df.shape}")
        # Envolver la operación to_sql en una transacción explícita
        with engine.begin() as connection: # <<--- AÑADIR ESTO
            df.to_sql(
                name=table_name,
                con=connection, # <<--- USAR ESTA CONEXIÓN
                if_exists=if_exists_strategy,
                index=False,
                method='multi',
                chunksize=10000
            )
        # El bloque 'with engine.begin()' hace commit automático si no hay error, o rollback si hay error.
        logger.info(f"DataFrame cargado exitosamente en la tabla '{table_name}'.")
        return True
    except SQLAlchemyError as e:
        logger.error(f"Error de SQLAlchemy al cargar datos en la tabla '{table_name}': {e}", exc_info=True)
        # El rollback ya debería haber ocurrido por el context manager `with engine.begin()`
        raise
    except Exception as e:
        logger.error(f"Error inesperado al cargar datos en la tabla '{table_name}': {e}", exc_info=True)
        raise

def load_raw_data_to_db(
    df_api_raw: pd.DataFrame,
    api_table_name: str,
    df_airbnb_raw: pd.DataFrame,
    airbnb_table_name: str,
    db_name: str = "airbnb",
    if_exists_strategy: str = "replace"
):
    """
    Carga los DataFrames crudos de API y Airbnb a sus respectivas tablas en la base de datos.

    Args:
        df_api_raw (pd.DataFrame): DataFrame crudo de datos de API.
        api_table_name (str): Nombre de la tabla para los datos crudos de API.
        df_airbnb_raw (pd.DataFrame): DataFrame crudo de datos de Airbnb.
        airbnb_table_name (str): Nombre de la tabla para los datos crudos de Airbnb.
        db_name (str, optional): Nombre de la base de datos. Por defecto "airbnb".
        if_exists_strategy (str, optional): Estrategia si la tabla ya existe. Por defecto "replace".
    """
    logger.info(f"Iniciando proceso de carga bruta para tablas '{api_table_name}' y '{airbnb_table_name}' en la BD '{db_name}'.")
    engine = None
    api_load_successful = False
    airbnb_load_successful = False

    try:
        engine = get_db_engine(db_name=db_name)
        logger.info(f"Engine para la base de datos '{db_name}' obtenido.")

        # Cargar datos de API
        logger.info(f"Procesando carga para {api_table_name}...")
        if df_api_raw is not None and not df_api_raw.empty:
            api_load_successful = load_single_raw_dataframe_to_db(df_api_raw, api_table_name, engine, if_exists_strategy)
        else:
            logger.warning(f"DataFrame para '{api_table_name}' es None o está vacío. Saltando carga.")
            api_load_successful = True # Considerar éxito si no había nada que cargar

        # Cargar datos de Airbnb
        logger.info(f"Procesando carga para {airbnb_table_name}...")
        if df_airbnb_raw is not None and not df_airbnb_raw.empty:
            # Pasamos el engine, la subfunción manejará la transacción
            airbnb_load_successful = load_single_raw_dataframe_to_db(df_airbnb_raw, airbnb_table_name, engine, if_exists_strategy)
        else:
            logger.warning(f"DataFrame para '{airbnb_table_name}' es None o está vacío. Saltando carga.")
            airbnb_load_successful = True

        if api_load_successful and airbnb_load_successful:
            logger.info("Carga bruta de ambos DataFrames completada exitosamente.")
        else:
            logger.error("Falló la carga bruta de uno o ambos DataFrames.")
            # La excepción ya se habrá lanzado desde load_single_raw_dataframe_to_db si hubo un error real.
            # Si solo uno estaba vacío, esto no debería ser un error fatal.

    except Exception as e:
        logger.error(f"Error general durante el proceso de carga bruta: {e}", exc_info=True)
        raise # Re-lanzar para que la tarea de Airflow falle
    finally:
        if engine:
            engine.dispose()
            logger.info("Engine de base de datos dispuesto.")

if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO,
                        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')

    logger.info("--- Iniciando prueba local de load_raw_data_to_db ---")

    # Crear DataFrames de ejemplo para la prueba
    sample_api_data = {
        'id_api': [1, 2, 3],
        'name_api': ['Place A', 'Place B', 'Place C'],
        'category_api': ['Restaurant', 'Bar', 'Shop']
    }
    test_df_api = pd.DataFrame(sample_api_data)

    sample_airbnb_data = {
        'id_airbnb': [101, 102, 103],
        'name_airbnb': ['Cozy Flat', 'Sunny Room', 'Spacious House'],
        'price_airbnb': [100, 50, 200]
    }
    test_df_airbnb = pd.DataFrame(sample_airbnb_data)

    # Nombres de las tablas para la prueba
    test_api_table = "test_raw_api_data"
    test_airbnb_table = "test_raw_airbnb_data"
    test_db = "airbnb" # Asegúrate que esta BD exista y las credenciales en .env sean correctas

    try:
        logger.info(f"Intentando cargar datos de prueba en BD '{test_db}', tablas '{test_api_table}', '{test_airbnb_table}'.")
        # Nota: Esta prueba MODIFICARÁ tu base de datos 'airbnb' creando/reemplazando estas tablas de prueba.
        load_raw_data_to_db(
            df_api_raw=test_df_api,
            api_table_name=test_api_table,
            df_airbnb_raw=test_df_airbnb,
            airbnb_table_name=test_airbnb_table,
            db_name=test_db
        )
        logger.info("Prueba de carga bruta completada. Verifica las tablas en tu base de datos.")

        # Prueba con un DataFrame vacío para ver el warning
        logger.info("\nIntentando cargar un DataFrame de API vacío y uno de Airbnb válido...")
        empty_df = pd.DataFrame()
        load_raw_data_to_db(
            df_api_raw=empty_df,
            api_table_name=test_api_table, # Reutilizamos tabla para ver que se borra o se salta
            df_airbnb_raw=test_df_airbnb,
            airbnb_table_name=test_airbnb_table,
            db_name=test_db
        )
        logger.info("Prueba con DataFrame vacío completada.")


    except Exception as main_exception:
        logger.error(f"Error durante la prueba local de carga bruta: {main_exception}", exc_info=True)
    finally:
        # Opcional: Limpiar tablas de prueba (requeriría ejecutar SQL)
        # engine = get_db_engine(test_db)
        # if engine:
        #     try:
        #         with engine.connect() as connection:
        #             connection.execute(text(f"DROP TABLE IF EXISTS {test_api_table};"))
        #             connection.execute(text(f"DROP TABLE IF EXISTS {test_airbnb_table};"))
        #             connection.commit() # Necesario si no usas begin() que es autocommit en muchos drivers
        #             logger.info(f"Tablas de prueba {test_api_table} y {test_airbnb_table} eliminadas.")
        #     except Exception as e:
        #         logger.error(f"Error limpiando tablas de prueba: {e}")
        #     finally:
        #         engine.dispose()
        logger.info("--- Prueba local de load_raw_data_to_db finalizada ---")