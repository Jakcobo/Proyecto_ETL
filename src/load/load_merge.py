# /home/nicolas/Escritorio/proyecto ETL/develop/src/load/load_merge.py

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

def load_merged_data_to_db(
    df_merged: pd.DataFrame,
    table_name: str, # El nombre de la tabla será pasado desde el DAG
    db_name: str = "airbnb",
    if_exists_strategy: str = "replace"
):
    """
    Carga el DataFrame mergeado (Airbnb enriquecido con datos de API) a una tabla en la base de datos.

    Args:
        df_merged (pd.DataFrame): El DataFrame mergeado a cargar.
        table_name (str): El nombre de la tabla destino.
        db_name (str, optional): Nombre de la base de datos. Por defecto "airbnb".
        if_exists_strategy (str, optional): Estrategia si la tabla ya existe ('replace', 'append', 'fail').
                                         Por defecto "replace".
    """
    if not isinstance(df_merged, pd.DataFrame):
        logger.error(f"La entrada para la tabla '{table_name}' no es un DataFrame. Tipo recibido: {type(df_merged)}")
        raise TypeError(f"Se esperaba un DataFrame para la tabla '{table_name}', se obtuvo {type(df_merged)}")

    if df_merged.empty:
        logger.warning(f"El DataFrame mergeado para la tabla '{table_name}' está vacío. No se realizará la carga.")
        # Podrías decidir si esto es un error o simplemente no hacer nada.
        # Si no hacer nada es aceptable, puedes retornar False o simplemente pasar.
        return False # Indicar que no se cargó nada

    logger.info(f"Iniciando carga del DataFrame mergeado en la tabla '{table_name}' en la BD '{db_name}'. Estrategia: '{if_exists_strategy}'. Shape: {df_merged.shape}")
    engine = None

    try:
        engine = get_db_engine(db_name=db_name)
        logger.info(f"Engine para la base de datos '{db_name}' obtenido.")

        df_merged.to_sql(
            name=table_name,
            con=engine,
            if_exists=if_exists_strategy,
            index=False,
            method='multi',
            chunksize=10000 # Ajusta según el tamaño de tus datos y memoria
        )
        logger.info(f"DataFrame mergeado cargado exitosamente en la tabla '{table_name}'.")
        return True
    except SQLAlchemyError as e:
        logger.error(f"Error de SQLAlchemy al cargar datos mergeados en la tabla '{table_name}': {e}", exc_info=True)
        raise
    except Exception as e:
        logger.error(f"Error inesperado al cargar datos mergeados en la tabla '{table_name}': {e}", exc_info=True)
        raise
    finally:
        if engine:
            engine.dispose()
            logger.info("Engine de base de datos dispuesto.")

if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO,
                        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')

    logger.info("--- Iniciando prueba local de load_merged_data_to_db ---")

    # Crear un DataFrame de ejemplo que simule el resultado del merge
    sample_merged_data = {
        'id': [1, 2, 3], # ID de Airbnb
        'name_airbnb': ['Airbnb A', 'Airbnb B', 'Airbnb C'],
        'lat': [40.75, 40.76, 40.70],
        'long': [-73.98, -73.99, -73.90],
        'price': [100, 150, 200],
        'nearby_restaurants_count': [2, 0, 1],
        'nearby_parks_and_outdoor_count': [1, 1, 0], # Ejemplo de nombre de columna generado
        'total_nearby_pois': [3, 1, 1]
    }
    test_df_merged = pd.DataFrame(sample_merged_data)

    # Nombre de la tabla para la prueba
    # En el DAG, este nombre vendrá de la llamada a la tarea.
    test_merged_table_name = "test_final_merged_data"
    test_db_name_param = "airbnb" # Asegúrate que esta BD exista y las credenciales en .env sean correctas

    try:
        logger.info(f"Intentando cargar datos mergeados de prueba en BD '{test_db_name_param}', tabla '{test_merged_table_name}'.")
        logger.info(f"DataFrame de prueba:\n{test_df_merged.to_markdown(index=False)}")
        
        # Nota: Esta prueba MODIFICARÁ tu base de datos creando/reemplazando esta tabla de prueba.
        success = load_merged_data_to_db(
            df_merged=test_df_merged,
            table_name=test_merged_table_name, # Pasar el nombre de la tabla aquí
            db_name=test_db_name_param
        )
        if success:
            logger.info(f"Prueba de carga de datos mergeados completada. Verifica la tabla '{test_merged_table_name}' en tu base de datos.")
        else:
            logger.warning(f"La carga de datos mergeados no se realizó (posiblemente DataFrame vacío).")


        # Prueba con DataFrame vacío
        logger.info("\n--- Probando con DataFrame mergeado vacío ---")
        empty_df_merged = pd.DataFrame()
        success_empty = load_merged_data_to_db(
            df_merged=empty_df_merged,
            table_name=test_merged_table_name, # Reutilizar para ver si se borra o se salta
            db_name=test_db_name_param
        )
        if not success_empty:
             logger.info("Prueba con DataFrame mergeado vacío manejada correctamente (no se cargó).")


    except Exception as main_exception:
        logger.error(f"Error durante la prueba local de carga de datos mergeados: {main_exception}", exc_info=True)
    finally:
        # Opcional: Limpiar tabla de prueba
        # from sqlalchemy import text
        # engine_cleanup = get_db_engine(test_db_name_param)
        # if engine_cleanup:
        #     try:
        #         with engine_cleanup.connect() as connection:
        #             connection.execute(text(f"DROP TABLE IF EXISTS {test_merged_table_name};"))
        #             # SQLAlchemy 2.0+ no necesita commit explícito aquí con text() si el engine no está en modo autocommit legacy
        #             # connection.commit() 
        #             logger.info(f"Tabla de prueba {test_merged_table_name} eliminada.")
        #     except Exception as e_cleanup:
        #         logger.error(f"Error limpiando tabla de prueba: {e_cleanup}")
        #     finally:
        #         engine_cleanup.dispose()
        logger.info("--- Prueba local de load_merged_data_to_db finalizada ---")