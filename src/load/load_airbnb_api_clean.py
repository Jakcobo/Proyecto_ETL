# /home/nicolas/Escritorio/proyecto ETL/develop/src/load/load_airbnb_api_clean.py

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
    logger_fallback = logging.getLogger(__name__ + "_fallback")
    logger_fallback.error("Error importando get_db_engine. Asegúrate que 'src' esté en PYTHONPATH.")
    # from ..database.db import get_db_engine # Alternativa para pruebas directas

logger = logging.getLogger(__name__)

def _load_single_cleaned_dataframe_to_db(df: pd.DataFrame, table_name: str, engine, if_exists_strategy: str = "replace"):
    """
    Carga un único DataFrame limpio a una tabla específica en la base de datos.
    (Esta función es idéntica a la de bruta_load.py, podría moverse a un utils.db_load)

    Args:
        df (pd.DataFrame): El DataFrame a cargar.
        table_name (str): El nombre de la tabla destino.
        engine: El engine de SQLAlchemy para la conexión a la BD.
        if_exists_strategy (str): Estrategia si la tabla ya existe ('replace', 'append', 'fail').
    """
    if not isinstance(df, pd.DataFrame):
        logger.error(f"La entrada para la tabla '{table_name}' no es un DataFrame. Tipo recibido: {type(df)}")
        raise TypeError(f"Se esperaba un DataFrame para la tabla '{table_name}', se obtuvo {type(df)}")

    if df.empty:
        logger.warning(f"El DataFrame para la tabla '{table_name}' está vacío. No se realizará la carga.")
        return False # Indicar que no se cargó nada

    try:
        logger.info(f"Iniciando carga del DataFrame limpio en la tabla '{table_name}' con estrategia '{if_exists_strategy}'. Shape: {df.shape}")
        df.to_sql(
            name=table_name,
            con=engine,
            if_exists=if_exists_strategy,
            index=False,
            method='multi',
            chunksize=10000
        )
        logger.info(f"DataFrame limpio cargado exitosamente en la tabla '{table_name}'.")
        return True
    except SQLAlchemyError as e:
        logger.error(f"Error de SQLAlchemy al cargar datos limpios en la tabla '{table_name}': {e}", exc_info=True)
        raise
    except Exception as e:
        logger.error(f"Error inesperado al cargar datos limpios en la tabla '{table_name}': {e}", exc_info=True)
        raise

def load_cleaned_api_airbnb_to_db(
    df_api_cleaned: pd.DataFrame,
    api_cleaned_table_name: str,
    df_airbnb_cleaned: pd.DataFrame,
    airbnb_cleaned_table_name: str,
    db_name: str = "airbnb",
    if_exists_strategy: str = "replace"
):
    """
    Carga los DataFrames limpios de API y Airbnb a sus respectivas tablas de staging en la base de datos.

    Args:
        df_api_cleaned (pd.DataFrame): DataFrame limpio de datos de API.
        api_cleaned_table_name (str): Nombre de la tabla para los datos limpios de API.
        df_airbnb_cleaned (pd.DataFrame): DataFrame limpio de datos de Airbnb.
        airbnb_cleaned_table_name (str): Nombre de la tabla para los datos limpios de Airbnb.
        db_name (str, optional): Nombre de la base de datos. Por defecto "airbnb".
        if_exists_strategy (str, optional): Estrategia si la tabla ya existe. Por defecto "replace".
    """
    logger.info(f"Iniciando proceso de carga de datos limpios para tablas '{api_cleaned_table_name}' y '{airbnb_cleaned_table_name}' en la BD '{db_name}'.")
    engine = None
    api_load_successful = False
    airbnb_load_successful = False

    try:
        engine = get_db_engine(db_name=db_name)
        logger.info(f"Engine para la base de datos '{db_name}' obtenido.")

        # Cargar datos de API limpios
        logger.info(f"Procesando carga para {api_cleaned_table_name}...")
        if df_api_cleaned is not None and not df_api_cleaned.empty:
            api_load_successful = _load_single_cleaned_dataframe_to_db(df_api_cleaned, api_cleaned_table_name, engine, if_exists_strategy)
        else:
            logger.warning(f"DataFrame para '{api_cleaned_table_name}' es None o está vacío. Saltando carga.")
            api_load_successful = True # Considerar éxito si no había nada que cargar

        # Cargar datos de Airbnb limpios
        logger.info(f"Procesando carga para {airbnb_cleaned_table_name}...")
        if df_airbnb_cleaned is not None and not df_airbnb_cleaned.empty:
            airbnb_load_successful = _load_single_cleaned_dataframe_to_db(df_airbnb_cleaned, airbnb_cleaned_table_name, engine, if_exists_strategy)
        else:
            logger.warning(f"DataFrame para '{airbnb_cleaned_table_name}' es None o está vacío. Saltando carga.")
            airbnb_load_successful = True # Considerar éxito si no había nada que cargar
        
        if api_load_successful and airbnb_load_successful:
            logger.info("Carga de ambos DataFrames limpios completada exitosamente.")
        else:
            # Esto solo se alcanzaría si una carga falló por una razón que no sea DataFrame vacío,
            # ya que _load_single_cleaned_dataframe_to_db lanzaría una excepción en caso de error.
            logger.error("Falló la carga de uno o ambos DataFrames limpios. Revise los logs anteriores.")
            # No es necesario lanzar una excepción aquí si las individuales ya lo hacen.

    except Exception as e:
        logger.error(f"Error general durante el proceso de carga de datos limpios: {e}", exc_info=True)
        raise
    finally:
        if engine:
            engine.dispose()
            logger.info("Engine de base de datos dispuesto.")

if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO,
                        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')

    logger.info("--- Iniciando prueba local de load_cleaned_api_airbnb_to_db ---")

    # Crear DataFrames de ejemplo limpios para la prueba
    sample_api_cleaned_data = {
        'fsq_id': ['1_clean', '2_clean', '3_clean'],
        'name_api_clean': ['Clean Place A', 'Clean Place B', 'Clean Place C'],
        'category_group_api': ['restaurants', 'cultural', 'retail_&_shopping']
    }
    test_df_api_cleaned = pd.DataFrame(sample_api_cleaned_data)

    sample_airbnb_cleaned_data = {
        'id_airbnb_clean': [101, 102, 103],
        'name_airbnb_clean': ['Clean Cozy Flat', 'Clean Sunny Room', 'Clean Spacious House'],
        'price_clean': [100.0, 50.0, 200.0],
        'last_review_clean': [20221231, 20230115, 19000101]
    }
    test_df_airbnb_cleaned = pd.DataFrame(sample_airbnb_cleaned_data)

    # Nombres de las tablas para la prueba
    test_api_cleaned_table = "test_cleaned_api_staging"
    test_airbnb_cleaned_table = "test_cleaned_airbnb_staging"
    test_db_name = "airbnb" # Asegúrate que esta BD exista y las credenciales en .env sean correctas

    try:
        logger.info(f"Intentando cargar datos limpios de prueba en BD '{test_db_name}', tablas '{test_api_cleaned_table}', '{test_airbnb_cleaned_table}'.")
        # Nota: Esta prueba MODIFICARÁ tu base de datos 'airbnb' creando/reemplazando estas tablas de prueba.
        load_cleaned_api_airbnb_to_db(
            df_api_cleaned=test_df_api_cleaned,
            api_cleaned_table_name=test_api_cleaned_table,
            df_airbnb_cleaned=test_df_airbnb_cleaned,
            airbnb_cleaned_table_name=test_airbnb_cleaned_table,
            db_name=test_db_name
        )
        logger.info("Prueba de carga de datos limpios completada. Verifica las tablas en tu base de datos.")

    except Exception as main_exception:
        logger.error(f"Error durante la prueba local de carga de datos limpios: {main_exception}", exc_info=True)
    finally:
        # Opcional: Limpiar tablas de prueba (similar a bruta_load.py)
        logger.info("--- Prueba local de load_cleaned_api_airbnb_to_db finalizada ---")