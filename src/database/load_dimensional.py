import pandas as pd
import logging
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy import text
import sys
import os

DB_MODULE_PATH = os.path.abspath(os.path.dirname(__file__))
if DB_MODULE_PATH not in sys.path:
    sys.path.append(DB_MODULE_PATH)

try:
    from .db import get_db_engine
except ImportError:
    from db import get_db_engine

logger = logging.getLogger(__name__)

def truncate_table(table_name: str, engine):
    """
    Elimina todos los registros de una tabla.
    ¡USAR CON PRECAUCIÓN!
    """
    try:
        with engine.connect() as connection:
            transaction = connection.begin()
            logger.info(f"Truncando (eliminando todos los datos de) la tabla '{table_name}'...")
            connection.execute(text(f'DELETE FROM public."{table_name}";'))
            transaction.commit()
            logger.info(f"Todos los datos eliminados de la tabla '{table_name}'.")
    except SQLAlchemyError as e:
        logger.error(f"Error al truncar la tabla '{table_name}': {e}", exc_info=True)
        if 'transaction' in locals() and transaction.is_active:
            transaction.rollback()
        raise
    except Exception as e:
        logger.error(f"Error inesperado durante el truncado de la tabla '{table_name}': {e}", exc_info=True)
        if 'transaction' in locals() and transaction.is_active:
            transaction.rollback()
        raise


def load_table(df: pd.DataFrame, table_name: str, engine, index: bool = False, chunksize: int = 1000):
    """
    Carga un DataFrame a una tabla específica en la base de datos usando if_exists='append'.
    Se asume que la tabla ya fue truncada si esa es la estrategia.
    """
    if df.empty:
        logger.info(f"DataFrame para la tabla '{table_name}' está vacío. No se cargan datos.")
        return

    try:
        logger.info(f"Iniciando carga de datos para la tabla '{table_name}'. Shape: {df.shape}")
        df.to_sql(name=table_name, schema='public', con=engine, if_exists='append', index=index, chunksize=chunksize)
        logger.info(f"Datos cargados exitosamente en la tabla '{table_name}'.")
    except SQLAlchemyError as e:
        logger.error(f"Error al cargar datos en la tabla '{table_name}': {e}", exc_info=True)
        raise
    except Exception as e:
        logger.error(f"Error inesperado durante la carga a la tabla '{table_name}': {e}", exc_info=True)
        raise

def load_dimensional_data(prepared_data_dictionary: dict, db_name: str, load_order: list = None):
    if not isinstance(prepared_data_dictionary, dict):
        logger.error("prepared_data_dictionary debe ser un diccionario.")
        raise TypeError("prepared_data_dictionary debe ser un diccionario.")

    if not prepared_data_dictionary:
        logger.warning("El diccionario de datos preparados está vacío. No se realizará ninguna carga.")
        return True

    logger.info(f"Iniciando carga (Truncate and Load) del modelo dimensional en la base de datos '{db_name}'.")
    engine = None
    try:
        engine = get_db_engine(db_name_target=db_name)

        if load_order is None:
            default_load_order = [
                "dim_host", "dim_property_location", "dim_property", "dim_date",
                "fact_publication"
            ]
            tables_in_dict = [table for table in default_load_order if table in prepared_data_dictionary]
        else:
            tables_in_dict = [table for table in load_order if table in prepared_data_dictionary]

        truncate_order = [table for table in reversed(tables_in_dict) if table in prepared_data_dictionary]

        logger.info(f"Orden de truncado de tablas: {truncate_order}")
        for table_name in truncate_order:
            truncate_table(table_name, engine)

        logger.info(f"Orden de carga de tablas: {tables_in_dict}")
        for table_name in tables_in_dict:
            df_to_load = prepared_data_dictionary.get(table_name)
            if df_to_load is not None:
                if not isinstance(df_to_load, pd.DataFrame):
                    logger.warning(f"El valor para '{table_name}' no es un DataFrame, se omite la carga.")
                    continue
                load_table(df_to_load, table_name, engine)
            else:
                logger.warning(f"No se encontraron datos para la tabla '{table_name}' en el diccionario.")

        logger.info("Carga (Truncate and Load) del modelo dimensional completada.")
        return True

    except Exception as e:
        logger.error(f"Fallo general durante la carga del modelo dimensional: {e}", exc_info=True)
        raise
    finally:
        if engine:
            engine.dispose()
            logger.info("Conexión a la base de datos cerrada después de la carga dimensional.")