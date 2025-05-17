# /home/nicolas/Escritorio/proyecto ETL/develop/src/load/load_airbnb_api_clean.py

import pandas as pd
import logging
import os
from sqlalchemy.exc import SQLAlchemyError

# Asegurarse de que src esté en el path para importar db y definir PROCESSED_DATA_PATH
import sys
PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..'))
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

PROCESSED_DATA_PATH = os.path.join(PROJECT_ROOT, "data", "processed") # Corregido a "processed"

def _ensure_processed_dir_exists():
    if not os.path.exists(PROCESSED_DATA_PATH):
        try:
            os.makedirs(PROCESSED_DATA_PATH)
            logger.info(f"Directorio creado: {PROCESSED_DATA_PATH}")
        except OSError as e:
            logger.error(f"Error al crear el directorio {PROCESSED_DATA_PATH}: {e}")
            raise

def _df_to_str_sample(df: pd.DataFrame, n: int = 3) -> str:
    """Retorna una representación en string de las primeras n filas y columnas de un DataFrame."""
    if df is None or df.empty:
        return "DataFrame vacío o None."
    return f"Shape: {df.shape}\nColumnas: {df.columns.tolist()}\nPrimeras {n} filas:\n{df.head(n).to_string()}"


def _save_df_to_processed_csv(df: pd.DataFrame, file_name: str):
    if df is None or df.empty:
        logger.warning(f"DataFrame para '{file_name}' está vacío o es None. No se guardará el CSV.")
        return

    _ensure_processed_dir_exists()
    file_path = os.path.join(PROCESSED_DATA_PATH, file_name)
    
    logger.info(f"Intentando guardar DataFrame en CSV: {file_path}")
    logger.debug(f"Muestra del DataFrame a guardar en '{file_name}':\n{_df_to_str_sample(df, n=3)}")
    
    try:
        df.to_csv(file_path, index=False, encoding='utf-8')
        logger.info(f"DataFrame guardado exitosamente como CSV en: {file_path}. Shape: {df.shape}")
    except Exception as e:
        logger.error(f"Error al guardar DataFrame como CSV en '{file_path}': {e}", exc_info=True)

def _load_single_cleaned_dataframe_to_db(df: pd.DataFrame, table_name: str, engine, if_exists_strategy: str = "replace"):
    if not isinstance(df, pd.DataFrame):
        logger.error(f"La entrada para la tabla '{table_name}' no es un DataFrame. Tipo recibido: {type(df)}")
        raise TypeError(f"Se esperaba un DataFrame para la tabla '{table_name}', se obtuvo {type(df)}")

    if df.empty:
        logger.warning(f"El DataFrame para la tabla '{table_name}' está vacío. No se realizará la carga a la BD.")
        return False

    logger.info(f"Iniciando carga a BD para tabla '{table_name}'. Estrategia: '{if_exists_strategy}'. Shape: {df.shape}.")
    logger.debug(f"Muestra del DataFrame a cargar en tabla '{table_name}':\n{_df_to_str_sample(df, n=3)}")

    logger.info(f"DataFrame dtypes para tabla '{table_name}':\n{df.dtypes.to_string()}")
    logger.info(f"Valores únicos para 'license' en tabla '{table_name}' (si existe):\n{df['license'].unique() if 'license' in df.columns else 'Columna no existe'}")
    logger.info(f"Valores únicos para 'house_rules' en tabla '{table_name}' (si existe):\n{df['house_rules'].unique() if 'house_rules' in df.columns else 'Columna no existe'}")

    try:
        df.to_sql(
            name=table_name,
            con=engine,
            if_exists=if_exists_strategy,
            index=False,
            method='multi',
        )
        logger.info(f"DataFrame cargado exitosamente en la tabla '{table_name}'.")
        return True
    except SQLAlchemyError as e:
        logger.error(f"Error de SQLAlchemy al cargar datos en la tabla '{table_name}': {e}", exc_info=True)
        raise
    except Exception as e:
        logger.error(f"Error inesperado al cargar datos en la tabla '{table_name}': {e}", exc_info=True)
        raise

def load_cleaned_api_airbnb_to_db(
    df_api_cleaned: pd.DataFrame,
    api_cleaned_table_name: str,
    df_airbnb_cleaned: pd.DataFrame,
    airbnb_cleaned_table_name: str,
    db_name: str = "airbnb",
    if_exists_strategy: str = "replace",
    save_csv: bool = True
):
    logger.info(f"Iniciando proceso de carga de datos limpios. BD: '{db_name}'.")
    logger.info(f"DataFrame API limpio - Tabla: '{api_cleaned_table_name}'. {_df_to_str_sample(df_api_cleaned, n=2)}")
    logger.info(f"DataFrame Airbnb limpio - Tabla: '{airbnb_cleaned_table_name}'. {_df_to_str_sample(df_airbnb_cleaned, n=2)}")

    engine = None
    api_load_successful_db = False
    airbnb_load_successful_db = False

    if save_csv:
        logger.info("Guardando DataFrames limpios como CSVs...")
        _save_df_to_processed_csv(df_api_cleaned, "cleaned_api_data.csv")
        _save_df_to_processed_csv(df_airbnb_cleaned, "cleaned_airbnb_data.csv")
    else:
        logger.info("Guardado de CSVs para datos limpios está desactivado.")

    try:
        engine = get_db_engine(db_name=db_name)
        logger.info(f"Engine para la base de datos '{db_name}' obtenido.")

        # Cargar datos de API limpios a la BD
        if df_api_cleaned is not None and not df_api_cleaned.empty:
            logger.info(f"Procesando carga a BD para API: {api_cleaned_table_name}...")
            api_load_successful_db = _load_single_cleaned_dataframe_to_db(df_api_cleaned, api_cleaned_table_name, engine, if_exists_strategy)
        else:
            logger.warning(f"DataFrame API limpio para '{api_cleaned_table_name}' es None o está vacío. Saltando carga a BD.")
            api_load_successful_db = True # Considerar éxito si no había nada que cargar

        # Cargar datos de Airbnb limpios a la BD
        if df_airbnb_cleaned is not None and not df_airbnb_cleaned.empty:
            logger.info(f"Procesando carga a BD para Airbnb: {airbnb_cleaned_table_name}...")
            airbnb_load_successful_db = _load_single_cleaned_dataframe_to_db(df_airbnb_cleaned, airbnb_cleaned_table_name, engine, if_exists_strategy)
        else:
            logger.warning(f"DataFrame Airbnb limpio para '{airbnb_cleaned_table_name}' es None o está vacío. Saltando carga a BD.")
            airbnb_load_successful_db = True
        
        if api_load_successful_db and airbnb_load_successful_db:
            logger.info("Carga a BD de ambos DataFrames limpios completada exitosamente.")
        else:
            # Si una carga falló (y no fue por DF vacío), la excepción ya se habría lanzado.
            # Este log es más para el caso donde uno o ambos DFs estaban vacíos y se saltó la carga.
            logger.info("Una o ambas cargas a BD no se realizaron (posiblemente DataFrames vacíos o error previo).")

    except Exception as e:
        logger.error(f"Error general durante el proceso de carga de datos limpios a la BD: {e}", exc_info=True)
        raise
    finally:
        if engine:
            engine.dispose()
            logger.info("Engine de base de datos dispuesto.")

if __name__ == '__main__':
    # Configurar logging para pruebas locales, puedes ponerlo en DEBUG para ver más
    logging.basicConfig(
        level=logging.DEBUG, # Nivel DEBUG para ver todos los logs
        format='%(asctime)s - %(name)s - [%(levelname)s] - %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )

    logger.info("--- Iniciando prueba local de load_cleaned_api_airbnb_to_db ---")

    sample_api_cleaned_data = {'fsq_id': ['1_clean_test', '2_clean_test', '3_clean_test', '4_clean_test', '5_clean_test'], 'name_api_clean': ['Clean Place A Test', 'Clean Place B Test', 'Clean Place C Test', 'Clean Place D Test', 'Clean Place E Test'], 'category_group_api': ['restaurants', 'cultural', 'retail_&_shopping', 'restaurants', 'parks_&_outdoor']}
    test_df_api_cleaned = pd.DataFrame(sample_api_cleaned_data)

    sample_airbnb_cleaned_data = {'id_airbnb_clean': [1010, 1020, 1030, 1040, 1050], 'name_airbnb_clean': ['Clean Cozy Flat Test', 'Clean Sunny Room Test', 'Clean Spacious House Test', 'Another Flat', 'The Best Room'], 'price_clean': [110.0, 60.0, 210.0, 90.0, 150.0], 'last_review_clean': [20231231, 20240115, 19010101, 20231010, 20240202]}
    test_df_airbnb_cleaned = pd.DataFrame(sample_airbnb_cleaned_data)

    test_api_cleaned_table = "test_cleaned_api_staging"
    test_airbnb_cleaned_table = "test_cleaned_airbnb_staging"
    test_db_name = "airbnb_test_model"

    try:
        logger.info(f"Intentando cargar y guardar datos limpios de prueba. BD: '{test_db_name}', Tablas: '{test_api_cleaned_table}', '{test_airbnb_cleaned_table}'.")
        
        load_cleaned_api_airbnb_to_db(
            df_api_cleaned=test_df_api_cleaned,
            api_cleaned_table_name=test_api_cleaned_table,
            df_airbnb_cleaned=test_df_airbnb_cleaned,
            airbnb_cleaned_table_name=test_airbnb_cleaned_table,
            db_name=test_db_name,
            save_csv=True
        )
        logger.info("Prueba de carga y guardado de datos limpios completada.")
        logger.info(f"Verifica las tablas en tu BD '{test_db_name}' y los archivos CSV en '{PROCESSED_DATA_PATH}'.")

    except Exception as main_exception:
        logger.error(f"Error durante la prueba local: {main_exception}", exc_info=True)
    finally:
        logger.info("--- Prueba local de load_cleaned_api_airbnb_to_db finalizada ---")