# /home/nicolas/Escritorio/proyecto ETL/develop/src/load/load_merge.py

import pandas as pd
import logging
import os
from sqlalchemy.exc import SQLAlchemyError

# Asegurarse de que src esté en el path para importar db y definir PROCESSED_DATA_PATH
import sys
PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..')) # Raíz del proyecto (develop)
SRC_PATH = os.path.join(PROJECT_ROOT, "src")
if SRC_PATH not in sys.path:
    sys.path.insert(0, SRC_PATH)
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

try:
    from database.db import get_db_engine
except ImportError:
    logger_fallback = logging.getLogger(__name__ + "_fallback")
    logger_fallback.error("Error importando get_db_engine. Asegúrate que 'src' esté en PYTHONPATH.")

logger = logging.getLogger(__name__)

# Definir la ruta a la carpeta de datos procesados
PROCESSED_DATA_PATH = os.path.join(PROJECT_ROOT, "data", "processed") # Asegúrate que "processed" sea correcto

def _ensure_processed_dir_exists():
    """Asegura que el directorio data/processed exista."""
    if not os.path.exists(PROCESSED_DATA_PATH):
        try:
            os.makedirs(PROCESSED_DATA_PATH)
            logger.info(f"Directorio creado: {PROCESSED_DATA_PATH}")
        except OSError as e:
            logger.error(f"Error al crear el directorio {PROCESSED_DATA_PATH}: {e}")
            raise

def _save_df_to_processed_csv(df: pd.DataFrame, file_name: str):
    """Guarda un DataFrame en la carpeta data/processed como CSV."""
    if df is None or df.empty:
        logger.warning(f"DataFrame para '{file_name}' está vacío o es None. No se guardará el CSV.")
        return

    _ensure_processed_dir_exists()
    file_path = os.path.join(PROCESSED_DATA_PATH, file_name)
    try:
        df.to_csv(file_path, index=False, encoding='utf-8')
        logger.info(f"DataFrame guardado exitosamente como CSV en: {file_path}")
    except Exception as e:
        logger.error(f"Error al guardar DataFrame como CSV en '{file_path}': {e}", exc_info=True)
        # Considerar si este error debe ser fatal para la tarea
        # raise

def load_merged_data_to_db(
    df_merged: pd.DataFrame,
    table_name: str,
    db_name: str = "airbnb",
    if_exists_strategy: str = "replace",
    save_csv: bool = True, # Nuevo parámetro
    csv_file_name: str = "final_merged_data.csv" # Nombre del archivo CSV por defecto
):
    """
    Carga el DataFrame mergeado a una tabla en la base de datos y opcionalmente
    lo guarda como un archivo CSV en data/processed/.

    Args:
        df_merged (pd.DataFrame): El DataFrame mergeado a cargar.
        table_name (str): El nombre de la tabla destino en la BD.
        db_name (str, optional): Nombre de la base de datos. Por defecto "airbnb".
        if_exists_strategy (str, optional): Estrategia si la tabla ya existe. Por defecto "replace".
        save_csv (bool, optional): Si es True, guarda el DataFrame como CSV. Por defecto True.
        csv_file_name (str, optional): Nombre del archivo CSV a guardar.
                                       Por defecto "final_merged_data.csv".
    """
    if not isinstance(df_merged, pd.DataFrame):
        logger.error(f"La entrada para la tabla '{table_name}' no es un DataFrame. Tipo recibido: {type(df_merged)}")
        raise TypeError(f"Se esperaba un DataFrame para la tabla '{table_name}', se obtuvo {type(df_merged)}")

    # Guardar CSV
    if save_csv:
        _save_df_to_processed_csv(df_merged, csv_file_name)

    # Cargar a Base de Datos
    if df_merged.empty:
        logger.warning(f"El DataFrame mergeado para la tabla '{table_name}' está vacío. No se realizará la carga a la BD.")
        return False # Indicar que no se cargó nada a la BD

    logger.info(f"Iniciando carga del DataFrame mergeado en la tabla '{table_name}' en la BD '{db_name}'. Estrategia: '{if_exists_strategy}'. Shape: {df_merged.shape}")
    engine = None
    db_load_successful = False
    try:
        engine = get_db_engine(db_name=db_name)
        logger.info(f"Engine para la base de datos '{db_name}' obtenido.")

        df_merged.to_sql(
            name=table_name,
            con=engine,
            if_exists=if_exists_strategy,
            index=False,
            method='multi',
            chunksize=10000
        )
        logger.info(f"DataFrame mergeado cargado exitosamente en la tabla '{table_name}'.")
        db_load_successful = True
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
    
    return db_load_successful


if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO,
                        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')

    logger.info("--- Iniciando prueba local de load_merged_data_to_db ---")

    sample_merged_data = {
        'id': [101, 102, 103],
        'name_airbnb': ['Merged Airbnb A', 'Merged Airbnb B', 'Merged Airbnb C'],
        'lat': [40.751, 40.761, 40.701],
        'long': [-73.981, -73.991, -73.901],
        'price': [120, 160, 220],
        'nearby_restaurants_count': [3, 1, 2],
        'nearby_parks_and_outdoor_count': [0, 2, 1],
        'total_nearby_pois': [3, 3, 3]
    }
    test_df_merged = pd.DataFrame(sample_merged_data)

    test_merged_table_name_db = "test_final_merged_data_table" # Nombre para la tabla en BD
    test_merged_csv_filename = "test_final_merged_data_file.csv" # Nombre para el archivo CSV
    test_db_name_param = "airbnb_test_model"

    try:
        logger.info(f"Intentando cargar y guardar datos mergeados de prueba. BD: '{test_db_name_param}', Tabla: '{test_merged_table_name_db}', CSV: '{test_merged_csv_filename}'.")
        logger.info(f"DataFrame de prueba:\n{test_df_merged.to_markdown(index=False)}")
        
        success = load_merged_data_to_db(
            df_merged=test_df_merged,
            table_name=test_merged_table_name_db,
            db_name=test_db_name_param,
            save_csv=True,
            csv_file_name=test_merged_csv_filename
        )
        if success:
            logger.info(f"Prueba de carga y guardado de datos mergeados completada.")
            logger.info(f"Verifica la tabla '{test_merged_table_name_db}' en tu BD y el archivo CSV en '{os.path.join(PROCESSED_DATA_PATH, test_merged_csv_filename)}'.")
        else:
            logger.warning(f"La carga de datos mergeados a la BD no se realizó (posiblemente DataFrame vacío).")

    except Exception as main_exception:
        logger.error(f"Error durante la prueba local: {main_exception}", exc_info=True)
    finally:
        logger.info("--- Prueba local de load_merged_data_to_db finalizada ---")