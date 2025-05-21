# /Proyecto_ETL/src/extract/airbnb_extract.py
import pandas as pd
import logging
import os

logger = logging.getLogger(__name__)

def extract_airbnb_data(csv_path: str = None, encoding: str = 'ISO-8859-1', low_memory: bool = False) -> pd.DataFrame:
    """
    Extrae los datos principales de Airbnb desde un archivo CSV especificado o uno por defecto.

    Args:
        csv_path (str, optional): Ruta absoluta al archivo CSV.
            Si es None, se intentará construir la ruta basada en la raíz del proyecto
            y la constante RELATIVE_CSV_PATH_AIRBNB.
        encoding (str, optional): Codificación del archivo CSV. Por defecto 'ISO-8859-1'.
        low_memory (bool, optional): Opción low_memory para pd.read_csv. Por defecto False.


    Returns:
        pd.DataFrame: DataFrame con los datos extraídos.

    Raises:
        FileNotFoundError: Si el archivo CSV no se encuentra.
        Exception: Para otros errores durante la lectura del CSV.
    """
    RELATIVE_CSV_PATH_AIRBNB = "data/raw/Airbnb_Open_Data.csv"
    absolute_file_path = csv_path

    if absolute_file_path is None:
        project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..'))
        absolute_file_path = os.path.join(project_root, RELATIVE_CSV_PATH_AIRBNB)
        logger.info(f"Ruta CSV no proporcionada, usando ruta por defecto: {absolute_file_path}")
    else:
        logger.info(f"Intentando cargar CSV desde ruta proporcionada: {absolute_file_path}")

    try:
        if not os.path.exists(absolute_file_path):
            logger.error(f"Archivo CSV de Airbnb no encontrado en: {absolute_file_path}")
            raise FileNotFoundError(f"Archivo CSV de Airbnb no encontrado en: {absolute_file_path}")

        df = pd.read_csv(absolute_file_path, encoding=encoding, low_memory=low_memory)
        logger.info(f"Datos de Airbnb extraídos exitosamente desde '{os.path.basename(absolute_file_path)}'. "
                    f"Shape: {df.shape}")
        return df

    except FileNotFoundError:
        raise
    except Exception as e:
        logger.error(f"Error extrayendo datos de Airbnb desde CSV '{absolute_file_path}': {e}", exc_info=True)
        raise

if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO,
                        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')

    logger.info("--- Iniciando prueba local de extract_airbnb_main_data ---")
    try:
        # Prueba 1: Usando la ruta por defecto
        logger.info("Intentando extracción con ruta por defecto...")
        airbnb_df_default = extract_airbnb_data()
        logger.info(f"Extracción por defecto exitosa. Primeras 3 filas:\n{airbnb_df_default.head(3)}")
        logger.info(f"Columnas: {airbnb_df_default.columns.tolist()}")

        # Prueba 2: Proporcionando una ruta (opcional, para verificar)
        # test_airbnb_csv_file = os.path.join(get_project_root(), RELATIVE_CSV_PATH_AIRBNB)
        # logger.info(f"\nIntentando extracción con ruta explícita: {test_airbnb_csv_file}...")
        # airbnb_df_explicit = extract_airbnb_main_data(csv_path=test_airbnb_csv_file)
        # logger.info(f"Extracción explícita exitosa. Shape: {airbnb_df_explicit.shape}")

    except FileNotFoundError as fnf_error:
        logger.error(f"Prueba fallida (FileNotFoundError): {fnf_error}")
    except Exception as main_exception:
        logger.error(f"Error durante la prueba local: {main_exception}", exc_info=True)
    finally:
        logger.info("--- Prueba local de extract_airbnb_main_data finalizada ---")