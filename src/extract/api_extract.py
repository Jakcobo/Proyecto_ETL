# Proyecto_ETL/src/extract/api_extract.py
import pandas as pd
import logging
import os

logger = logging.getLogger(__name__)

def extract_api_data(csv_path: str = None) -> pd.DataFrame:
    """
    Extrae datos de Foursquare desde un archivo CSV especificado o uno por defecto.

    Args:
        csv_path (str, optional): Ruta absoluta al archivo CSV.
            Si es None, se intentará construir la ruta basada en la raíz del proyecto
            y la constante RELATIVE_CSV_PATH_API.

    Returns:
        pd.DataFrame: DataFrame con los datos extraídos.

    Raises:
        FileNotFoundError: Si el archivo CSV no se encuentra.
        Exception: Para otros errores durante la lectura del CSV.
    """
    RELATIVE_CSV_PATH_API = "data/raw/api_data.csv"
    absolute_file_path = csv_path

    if absolute_file_path is None:
        project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..'))
        absolute_file_path = os.path.join(project_root, RELATIVE_CSV_PATH_API)
        logger.info(f"Ruta CSV no proporcionada, usando ruta por defecto: {absolute_file_path}")
    else:
        logger.info(f"Intentando cargar CSV desde ruta proporcionada: {absolute_file_path}")

    try:
        if not os.path.exists(absolute_file_path):
            logger.error(f"Archivo CSV de API no encontrado en: {absolute_file_path}")
            raise FileNotFoundError(f"Archivo CSV de API no encontrado en: {absolute_file_path}")

        df = pd.read_csv(absolute_file_path)
        logger.info(f"Datos de API (Foursquare) extraídos exitosamente desde '{os.path.basename(absolute_file_path)}'. "
                    f"Shape: {df.shape}")
        return df

    except FileNotFoundError:
        raise
    except Exception as e:
        logger.error(f"Error extrayendo datos de API (Foursquare) desde CSV '{absolute_file_path}': {e}", exc_info=True)
        raise

if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO,
                        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')

    logger.info("--- Iniciando prueba local de extract_foursquare_data ---")
    try:
        # Prueba 1: Usando la ruta por defecto (asume que el archivo está en data/raw/)
        logger.info("Intentando extracción con ruta por defecto...")
        api_df_default = extract_api_data()
        logger.info(f"Extracción por defecto exitosa. Primeras 3 filas:\n{api_df_default.head(3)}")
        logger.info(f"Columnas: {api_df_default.columns.tolist()}")

        # Prueba 2: Proporcionando una ruta (puedes cambiarla a una ruta válida en tu sistema para probar). Si el archivo está donde se espera, esta prueba debería funcionar igual que la anterior.
        # test_csv_file = os.path.join(get_project_root(), RELATIVE_CSV_PATH_API)
        # logger.info(f"\nIntentando extracción con ruta explícita: {test_csv_file}...")
        # api_df_explicit = extract_foursquare_data(csv_path=test_csv_file)
        # logger.info(f"Extracción explícita exitosa. Shape: {api_df_explicit.shape}")

        # Prueba 3: Ruta incorrecta para probar FileNotFoundError
        # logger.info("\nIntentando extracción con ruta incorrecta...")
        # extract_foursquare_data(csv_path="/ruta/inexistente/api_data.csv")

    except FileNotFoundError as fnf_error:
        logger.error(f"Prueba fallida como se esperaba (FileNotFoundError): {fnf_error}")
    except Exception as main_exception:
        logger.error(f"Error durante la prueba local: {main_exception}", exc_info=True)
    finally:
        logger.info("--- Prueba local de extract_foursquare_data finalizada ---")