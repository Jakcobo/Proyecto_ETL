#/home/nicolas/Escritorio/proyecto ETL/develop/src/extract/extract_data.py
# from database.db_operations
# import creating_engine, disposing_engine

import pandas as pd
import logging
#import json
import os

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(message)s", datefmt="%d/%m/%Y %I:%M:%S %p")
logger = logging.getLogger(__name__)

def exe_extract_data():
    """Extracts data from CSV and returns a pandas DataFrame"""
    try:
        logger.info("Starting data extraction from CSV file.")
        #df = pd.read_csv("../../data/Airbnb_Open_Data.csv", low_memory=False, encoding='ISO-8859-1')
        project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..'))
        csv_path = os.path.join(project_root, 'data', 'Airbnb_Open_Data.csv')
        logger.info(f"Looking for CSV file at: {csv_path}")
        
        if not os.path.exists(csv_path):
            logger.error(f"CSV file not found at: {csv_path}")
            raise FileNotFoundError(f"CSV file not found at: {csv_path}")
        
        df = pd.read_csv(csv_path, low_memory=False, encoding='ISO-8859-1')
        logger.info(f"Data extracted successfully fromCSV. Shape: {df.shape}")
        #return json.loads(df.to_json(orient='records'))
        return df
    except FileNotFoundError as fnf:
        logger.info(f"Extraction failed: {fnf}")
        raise
    except Exception as e:
        logger.error(f"Error extracting data from CSV: {e}", exc_info=True)
        raise

def extract_api_data(relative_file_path: str = "Notebooks/data/ny_places_foursquare_v3.csv") -> pd.DataFrame:
    """
    Extrae datos de Foursquare desde un archivo CSV.
    La ruta es relativa al directorio raíz del proyecto (asumido un nivel arriba de 'src').
    """
    # Determinar la ruta base del proyecto. Asumimos que este script está en src/extract/
    # y el directorio 'data' está en la raíz del proyecto.
    try:
        # __file__ es la ruta al script actual (src/extract/extract_data.py)
        # os.path.dirname(__file__) es src/extract/
        # os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..')) es la raíz del proyecto
        project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..'))
        absolute_file_path = os.path.join(project_root, relative_file_path)

        logger.info(f"Intentando cargar CSV desde ruta absoluta: {absolute_file_path}")
        
        if not os.path.exists(absolute_file_path):
            logger.error(f"Archivo no encontrado en {absolute_file_path}")
            # Podríamos intentar una ruta relativa al CWD como fallback, pero es menos robusto para Airflow
            # logger.warning(f"Intentando cargar desde ruta relativa al CWD: {relative_file_path}")
            # absolute_file_path = relative_file_path # Probar ruta relativa directa
            # if not os.path.exists(absolute_file_path):
            #     logger.error(f"Archivo tampoco encontrado en CWD: {relative_file_path}")
            raise FileNotFoundError(f"El archivo {absolute_file_path} no fue encontrado.")

        df = pd.read_csv(absolute_file_path)
        logger.info(f"CSV '{absolute_file_path}' cargado exitosamente. Filas: {len(df)}, Columnas: {len(df.columns)}")
        return df
    except FileNotFoundError:
        logger.error(f"Error crítico: Archivo CSV no encontrado.")
        raise # Re-lanza la excepción para que Airflow la maneje
    except Exception as e:
        logger.error(f"Error al leer el CSV {relative_file_path}: {e}", exc_info=True)
        raise

if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(name)s: %(message)s", datefmt="%Y-%m-%d %H:%M:%S")
    logger.info("Probando extract_api_data...")
    try:
        # Para que esta prueba funcione, el archivo CSV debe estar en ../../data/ny_places_foursquare_v3.csv
        # relativo a este script.
        df_test = extract_api_data()
        logger.info("Prueba de extracción exitosa.")
        # logger.info(f"Primeras filas:\n{df_test.head()}")
    except Exception as e:
        logger.error(f"Error en la prueba de extracción: {e}")