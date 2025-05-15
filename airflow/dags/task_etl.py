# proyecto_etl/airflow/dags/task_etl.py
import pandas as pd
import logging
import sys
import os
from dotenv import load_dotenv

# Asegúrate que la ruta a 'src' sea correcta
try:
    src_path = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..", "src"))
    if src_path not in sys.path:
        sys.path.append(src_path)
    # Importar módulos
    from extract.extract_data import exe_extract_data, extract_api_data
    from load.load_data import exe_load_data, load_api_places_to_db
    from transform.dataset_clean import clean_airbnb_data
    from transform.api_clean import clean_api_data
    from database.modeldb import create_dimensional_model_tables
    from load.dimensional_load import load_dimensional_data
    from database.db import get_db_engine
except ImportError as e:
     logging.error(f"Error importing project modules: {e}. Check sys.path and project structure relative to Airflow execution.")
     raise

logging.basicConfig(level=logging.INFO, format='%(asctime)s %(message)s', datefmt='%m/%d/%Y %I:%M:%S %p')
logger = logging.getLogger(__name__)

FINAL_TABLE_NAME = "airbnb_cleaned" # Tabla donde se cargan los datos limpios

# --- Funciones de Tareas Existentes (sin cambios) ---
def extract_data() -> pd.DataFrame:
    # ... (código existente) ...
    """Tarea de extracción: Extrae datos y devuelve un DataFrame."""
    try:
        logger.info("Executing data extraction task.")
        df = exe_extract_data() # Asume que esto devuelve un DataFrame
        if not isinstance(df, pd.DataFrame):
             logger.error(f"Extraction function did not return a pandas DataFrame. Got type: {type(df)}")
             raise TypeError("Extraction must return a pandas DataFrame")
        logger.info(f"Extraction successful. DataFrame shape: {df.shape}")
        return df
    except Exception as e:
        logger.error(f"Error during data extraction: {e}", exc_info=True)
        raise


def clean_data(df_raw: pd.DataFrame) -> pd.DataFrame:
    # ... (código existente) ...
    """Tarea de limpieza: Recibe un DataFrame, lo limpia y devuelve el DataFrame limpio."""
    try:
        logger.info("Executing data cleaning task.")
        if not isinstance(df_raw, pd.DataFrame):
            logger.error(f"Input to clean_data is not a DataFrame. Got type: {type(df_raw)}. Check XCom backend and previous task output.")
            raise TypeError("clean_data requires a pandas DataFrame input.")

        logger.info(f"DataFrame received for cleaning. Shape: {df_raw.shape}")
        df_cleaned = clean_airbnb_data(df_raw)
        logger.info(f"Data cleaning completed. Cleaned DataFrame shape: {df_cleaned.shape}")
        return df_cleaned
    except Exception as e:
        logger.error(f"Error during data cleaning: {e}", exc_info=True)
        raise


def load_cleaned_data(df_cleaned: pd.DataFrame):
    # ... (código existente) ...
    """Tarea de carga: Recibe el DataFrame limpio y lo carga en la tabla final."""
    try:
        logger.info(f"Executing loading task for cleaned data into table '{FINAL_TABLE_NAME}'.")
        if not isinstance(df_cleaned, pd.DataFrame):
            logger.error(f"Input to load_cleaned_data is not a DataFrame. Got type: {type(df_cleaned)}.")
            raise TypeError("load_cleaned_data requires a pandas DataFrame input.")

        logger.info(f"Cleaned DataFrame received for loading. Shape: {df_cleaned.shape}")
        success = exe_load_data(df=df_cleaned, db_name="airbnb", table_name=FINAL_TABLE_NAME)

        if success:
            logger.info(f"Cleaned data loading into '{FINAL_TABLE_NAME}' completed successfully.")
        else:
            logger.warning(f"Cleaned data loading function reported failure for table '{FINAL_TABLE_NAME}'.")
        return success

    except Exception as e:
        logger.error(f"Error during cleaned data loading into '{FINAL_TABLE_NAME}': {e}", exc_info=True)
        raise


def create_dimensional_model():
    # ... (código existente) ...
    """
    Función lógica para la tarea: Crea las tablas del modelo dimensional
    si no existen.
    """
    engine = None
    try:
        logger.info("Executing dimensional model creation task.")
        engine = get_db_engine(db_name="airbnb")

        if engine:
            create_dimensional_model_tables(engine)
            logger.info("Dimensional model tables creation process finished.")
            return True
        else:
            logger.error("Failed to get database engine. Cannot create dimensional model.")
            return False

    except Exception as e:
        logger.error(f"Error during dimensional model creation: {e}", exc_info=True)
        raise
    finally:
        if engine:
            engine.dispose()
            logger.info("Database engine disposed after model creation.")


# --- NUEVA FUNCIÓN PARA LA TAREA DE INSERCIÓN ---
def insert_data_to_model():
    """
    Función lógica para la tarea: Inserta datos desde la tabla limpia
    hacia las tablas del modelo dimensional.
    """
    engine = None
    try:
        logger.info("Executing task to insert data into dimensional model.")
        # Obtener el engine para la base de datos 'airbnb'
        engine = get_db_engine(db_name="airbnb")

        if engine:
            # Llamar a la función orquestadora de dimensional_load.py
            success = load_dimensional_data(engine)
            logger.info(f"Insertion into dimensional model finished. Success: {success}")
            return success
        else:
            logger.error("Failed to get database engine. Cannot insert data into dimensional model.")
            return False

    except Exception as e:
        logger.error(f"Error during insertion into dimensional model: {e}", exc_info=True)
        raise # Relanzar para que la tarea falle en Airflow
    finally:
        if engine:
            engine.dispose()
            logger.info("Database engine disposed after dimensional model insertion.")


def api_extract_task_logic(csv_file_path: str = "Notebooks/data/ny_places_api_v3.csv") -> pd.DataFrame:
    """Lógica para la tarea de extracción de datos de api."""
    logger.info(f"Iniciando tarea de extracción para api desde: {csv_file_path}")
    try:
        raw_df = extract_api_data(relative_file_path=csv_file_path)
        if raw_df.empty:
            logger.warning("Extracción resultó en un DataFrame vacío.")
            # Decidir si esto debe ser un error o no. Por ahora, se propaga.
        else:
            logger.info(f"Extracción completada. {len(raw_df)} filas obtenidas.")
        return raw_df
    except Exception as e:
        logger.error(f"Error durante la extracción de api: {e}", exc_info=True)
        raise

def api_transform_task_logic(df_input: pd.DataFrame) -> pd.DataFrame:
    """Lógica para la tarea de transformación de datos de api."""
    if not isinstance(df_input, pd.DataFrame) or df_input.empty:
        logger.warning("DataFrame de entrada para transformación está vacío o no es un DataFrame. Saltando.")
        # Devolver un DataFrame vacío para que el flujo continúe si es apropiado, o lanzar error
        return pd.DataFrame() # o df_input si se quiere propagar tal cual
        
    logger.info("Iniciando tarea de transformación para api.")
    try:
        transformed_df = clean_api_data(df_input)
        if transformed_df.empty and not df_input.empty: # Si entró algo pero salió vacío
            logger.warning("Transformación resultó en un DataFrame vacío a partir de datos no vacíos.")
        elif not transformed_df.empty:
            logger.info(f"Transformación completada. {len(transformed_df)} filas transformadas.")
        return transformed_df
    except Exception as e:
        logger.error(f"Error durante la transformación de api: {e}", exc_info=True)
        raise

def api_load_task_logic(df_to_load: pd.DataFrame):
    """Lógica para la tarea de carga de datos de api."""
    if not isinstance(df_to_load, pd.DataFrame) or df_to_load.empty:
        logger.warning("DataFrame para carga está vacío o no es un DataFrame. No se cargará nada.")
        return # Termina la tarea sin error si no hay nada que cargar

    logger.info("Iniciando tarea de carga para api.")
    try:
        # Opcional: Crear tablas si no existen (mejor con migraciones o una tarea separada de setup)
        # init_db() 
        # logger.info("Base de datos inicializada/verificada.")
        
        load_api_places_to_db(df_to_load)
        logger.info("Carga de datos de api completada.")
    except Exception as e:
        logger.error(f"Error durante la carga de api: {e}", exc_info=True)
        raise

# (Opcional) Una tarea para inicializar la BD si es necesario
# def database_init_task_logic():
#     logger.info("Iniciando tarea de inicialización de base de datos.")
#     try:
#         init_db()
#         logger.info("Inicialización de base de datos completada.")
#     except Exception as e:
#         logger.error(f"Error durante la inicialización de la base de datos: {e}", exc_info=True)
#         raise

if __name__ == "__main__":
    # Para pruebas locales de task_etl.py
    # Asegúrate de que el logging esté configurado y que los módulos src sean importables.
    logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(name)s: %(message)s", datefmt="%Y-%m-%d %H:%M:%S")
    
    # Añadir la raíz del proyecto a sys.path para que los imports de 'src' funcionen
    import sys
    current_dir = os.path.dirname(os.path.abspath(__file__))
    project_root_for_test = os.path.abspath(os.path.join(current_dir, '..', '..'))
    if project_root_for_test not in sys.path:
        sys.path.insert(0, project_root_for_test)
    logger.info(f"Añadido {project_root_for_test} a sys.path para pruebas locales.")

    # Re-importar con el path actualizado si es necesario (por si el try-except de arriba no lo hizo)
    from src.extract.extract_data import extract_api_data
    from src.transform.api_clean import clean_api_data
    from src.load.load_data import load_api_places_to_db
    #from src.database.db import init_db

    logger.info("--- Iniciando prueba local del flujo ETL de api ---")
    try:
        # 0. Inicializar BD (opcional, pero útil para prueba)
        logger.info("Paso 0: Inicializando base de datos...")
        #database_init_task_logic()

        # 1. Extract
        logger.info("Paso 1: Extrayendo datos...")
        # Ajusta la ruta si es necesario para tu prueba local
        # El extract_data ya calcula la ruta absoluta basada en su ubicación en src/
        extracted_data = api_extract_task_logic(csv_file_path="Notebooks/data/ny_places_api_v3.csv")
        if extracted_data.empty:
            logger.warning("Extracción no produjo datos. Finalizando prueba.")
        else:
            # 2. Transform
            logger.info("Paso 2: Transformando datos...")
            transformed_data = api_transform_task_logic(extracted_data)
            if transformed_data.empty:
                logger.warning("Transformación no produjo datos. Finalizando prueba.")
            else:
                # 3. Load
                logger.info("Paso 3: Cargando datos...")
                api_load_task_logic(transformed_data)
                logger.info("--- Flujo ETL de api (prueba local) completado exitosamente ---")

    except Exception as e:
        logger.error(f"Error en la prueba local del flujo ETL de api: {e}", exc_info=True)
