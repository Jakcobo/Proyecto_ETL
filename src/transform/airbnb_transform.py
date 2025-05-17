# /home/nicolas/Escritorio/proyecto ETL/develop/src/transform/airbnb_transform.py

import pandas as pd
import numpy as np # Necesario para np.nan y pd.NA
import logging
import os

# Configurar logging para este módulo
logger = logging.getLogger(__name__)

# --- Funciones de Limpieza Individuales (Adaptadas de dataset_clean.py) ---
# Estas funciones ahora son "privadas" al módulo o podrían moverse a un utils si son muy genéricas.

def _rename_columns_airbnb(df: pd.DataFrame) -> pd.DataFrame:
    """Renombra columnas: quita espacios, convierte a minúsculas, caracteres especiales."""
    logger.info("Renombrando columnas del DataFrame de Airbnb...")
    original_cols = df.columns.tolist()
    
    df.columns = df.columns.str.strip() # Eliminar espacios al inicio/final
    df.columns = df.columns.str.replace(' ', '_', regex=False)
    df.columns = df.columns.str.lower() # Esto convertiría 'ID' a 'id'
    # Opcional: Limpieza más agresiva (si la necesitas y sabes que no rompe nada)
    # df.columns = df.columns.str.replace(r'[^a-z0-9_]+', '', regex=True)

    new_cols = df.columns.tolist()
    renamed_cols_map = {oc: nc for oc, nc in zip(original_cols, new_cols) if oc != nc}
    if renamed_cols_map:
        logger.info(f"Se renombraron {len(renamed_cols_map)} columnas. Ejemplos: {dict(list(renamed_cols_map.items())[:3])}")
    else:
        logger.info("No se requirió renombrar columnas.")
    return df

def _transform_price_columns_airbnb(df: pd.DataFrame) -> pd.DataFrame:
    """Convierte columnas 'price' y 'service_fee' a tipo numérico."""
    logger.info("Transformando columnas de precio ('price', 'service_fee') a numérico...")
    for col in ['price', 'service_fee']:
        if col in df.columns:
            original_dtype = df[col].dtype
            if pd.api.types.is_object_dtype(original_dtype) or pd.api.types.is_string_dtype(original_dtype):
                logger.debug(f"Convirtiendo columna '{col}' (actual: {original_dtype})...")
                # Elimina '$', ',', convierte a numérico. Los errores se fuerzan a NaN.
                df[col] = df[col].astype(str).str.replace(r'[$\s,]', '', regex=True) # Añadido \s para espacios
                df[col] = pd.to_numeric(df[col], errors='coerce')
                
                # Imputación de NaN con 0.0 DESPUÉS de la conversión
                nan_count_after_conversion = df[col].isnull().sum()
                if nan_count_after_conversion > 0:
                    df[col] = df[col].fillna(0.0)
                    logger.info(f"Columna '{col}' convertida. {nan_count_after_conversion} NaNs imputados con 0.0. Nuevo tipo: {df[col].dtype}")
                else:
                    logger.info(f"Columna '{col}' convertida a numérico. Nuevo tipo: {df[col].dtype}")
            elif pd.api.types.is_numeric_dtype(original_dtype):
                logger.info(f"Columna '{col}' ya es numérica ({original_dtype}). Saltando conversión, pero asegurando float.")
                df[col] = df[col].astype(float) # Asegurar que sea float para consistencia
            else:
                logger.warning(f"Columna '{col}' tiene un tipo inesperado ({original_dtype}). Saltando conversión.")
        else:
            logger.warning(f"Columna '{col}' no encontrada para transformación de precio.")
    return df

def _transform_last_review_airbnb(df: pd.DataFrame) -> pd.DataFrame:
    """Convierte 'last_review' a formato INTEGER YYYYMMDD."""
    col_name = 'last_review'
    if col_name not in df.columns:
        logger.warning(f"Columna '{col_name}' no encontrada. Saltando transformación.")
        return df

    logger.info(f"Transformando columna '{col_name}' a INTEGER (YYYYMMDD)...")
    # Convertir a datetime, manejando errores y múltiples formatos.
    # Usar pd.NA para nulos consistentes si se usa Int64 luego.
    df[col_name + '_dt'] = pd.to_datetime(df[col_name], errors='coerce', infer_datetime_format=True)
    
    conversion_failures = df[col_name + '_dt'].isnull().sum() - df[col_name].isnull().sum()
    if conversion_failures > 0:
        logger.warning(f"{conversion_failures} entradas de '{col_name}' no pudieron ser parseadas a fechas.")

    # Imputar NaT (Not a Time) con una fecha por defecto ANTES de convertir a int
    # O considera dejarlo como pd.NA si tu modelo dimensional puede manejarlo.
    # Para YYYYMMDD como int, necesitamos una fecha.
    default_date_for_missing_reviews = pd.Timestamp('1900-01-01')
    df[col_name + '_dt'] = df[col_name + '_dt'].fillna(default_date_for_missing_reviews)
    logger.info(f"Fechas no parseables/nulas en '{col_name}_dt' imputadas con {default_date_for_missing_reviews.strftime('%Y-%m-%d')}.")

    # Convertir a formato YYYYMMDD como entero
    # Manejar pd.NA si se usa Int64. Si no, asegurarse de que no haya NaT.
    try:
        df[col_name] = df[col_name + '_dt'].dt.strftime('%Y%m%d')
        # df[col_name] = pd.to_numeric(df[col_name], errors='coerce').astype('Int64') # Usar Int64 para permitir nulos
        df[col_name] = pd.to_numeric(df[col_name], errors='raise').astype(int) # Si no se permiten nulos
    except Exception as e:
        logger.error(f"Error convirtiendo '{col_name}_dt' a formato YYYYMMDD int: {e}. Revise las fechas imputadas.", exc_info=True)
        # Podrías optar por dejar la columna como pd.NA o un valor centinela si la conversión falla
        # df[col_name] = pd.NA 

    df = df.drop(columns=[col_name + '_dt'])
    logger.info(f"Columna '{col_name}' transformada a INTEGER (YYYYMMDD). Tipo final: {df[col_name].dtype}")
    return df

def _correct_neighbourhood_group_airbnb(df: pd.DataFrame) -> pd.DataFrame:
    """Corrige typos conocidos en 'neighbourhood_group'."""
    col_name = 'neighbourhood_group'
    if col_name not in df.columns:
        logger.warning(f"Columna '{col_name}' no encontrada. Saltando corrección.")
        return df

    logger.info(f"Corrigiendo typos en '{col_name}'...")
    corrections = {
        'brookln': 'Brooklyn',
        'manhatan': 'Manhattan',
        # Añade más correcciones si son necesarias
    }
    # Aplicar correcciones de forma insensible a mayúsculas/minúsculas para los valores existentes
    # y luego mapear a la forma corregida.
    # Esto es más complejo. Una forma simple es asegurar que los valores estén en un formato esperado (ej. title case)
    # df[col_name] = df[col_name].str.title() # Ejemplo
    
    # Contar cuántos se van a cambiar (considerando el case actual de los datos)
    # Hacemos una copia temporal en minúsculas para la comparación
    # temp_series_lower = df[col_name].str.lower()
    # rows_to_change = df[temp_series_lower.isin(corrections.keys())].shape[0]

    # Aplicación directa (sensible a mayúsculas/minúsculas para las claves del diccionario)
    original_values = df[col_name].unique()
    df[col_name] = df[col_name].replace(corrections)
    changed_values = sum(1 for ov in original_values if ov in corrections and corrections[ov] != ov)

    if changed_values > 0:
        logger.info(f"Se corrigieron {changed_values} typos en '{col_name}'.")
    else:
        logger.info(f"No se encontraron typos conocidos (según el diccionario de correcciones) en '{col_name}'.")
    return df

def _cast_numeric_columns_airbnb(df: pd.DataFrame) -> pd.DataFrame:
    """Asegura que las columnas numéricas tengan el tipo correcto (Int64 para nulos, float, int)."""
    logger.info("Realizando casting de columnas numéricas de Airbnb...")
    
    # Columnas que deben ser enteras (Int64 permite pd.NA)
    # Asegúrate que los nombres de columna coincidan después de _rename_columns_airbnb
    int_cols = {
        'id': 'Int64',
        'host_id': 'Int64',
        'construction_year': 'Int64', # Anteriormente 'Construction_year'
        'minimum_nights': 'Int64',
        'number_of_reviews': 'Int64',
        'review_rate_number': 'Int64',
        'calculated_host_listings_count': 'Int64',
        'availability_365': 'Int64'
    }
    # 'last_review' ya se maneja como int en su propia función.

    # Columnas que deben ser flotantes
    float_cols = {
        'lat': 'Float64', # Float64 permite pd.NA
        'long': 'Float64',
        'reviews_per_month': 'Float64'
        # 'price' y 'service_fee' ya son float por _transform_price_columns_airbnb
    }

    for col, dtype in {**int_cols, **float_cols}.items():
        if col in df.columns:
            if df[col].dtype.name != dtype: # Compara el nombre del dtype
                try:
                    # Convertir a numérico primero (maneja strings que representan números)
                    # luego al tipo deseado. 'coerce' pone pd.NA si no puede convertir.
                    df[col] = pd.to_numeric(df[col], errors='coerce').astype(dtype)
                    logger.info(f"Columna '{col}' casteada a {dtype}.")
                except Exception as e:
                    logger.warning(f"No se pudo castear la columna '{col}' a {dtype}. Error: {e}. Dejando como está o pd.NA.")
                    # Podrías decidir llenar con un valor por defecto si el casteo falla y es crítico
            else:
                logger.debug(f"Columna '{col}' ya tiene el tipo {dtype}.")
        else:
            logger.warning(f"Columna '{col}' no encontrada para casting numérico.")
    return df

def _handle_nulls_airbnb(df: pd.DataFrame) -> pd.DataFrame:
    """Imputa valores nulos en columnas clave después de las conversiones."""
    logger.info("Manejando valores NULL/NaN en el DataFrame de Airbnb...")
    
    # Mapa de imputación: {columna: valor_de_imputacion}
    # Usa pd.NA si la columna es Int64/Float64 y quieres mantener nulos explícitos.
    # Usa 0, 0.0, o valores calculados (media, mediana) si prefieres imputar.
    imputation_map = {
        'reviews_per_month': 0.0,
        'review_rate_number': 0,        # Asumiendo 0 si no hay review rate
        'minimum_nights': 1,            # Asumiendo mínimo 1 noche por defecto
        'number_of_reviews': 0,
        # 'price' y 'service_fee' ya se imputaron con 0.0 en su función.
        # 'last_review' se imputó con una fecha por defecto.
    }

    for col, fill_value in imputation_map.items():
        if col in df.columns:
            if df[col].isnull().any():
                original_null_count = df[col].isnull().sum()
                df[col] = df[col].fillna(fill_value)
                # Asegurar tipo después de fillna si es necesario, especialmente si fill_value es 0 para Int64
                if df[col].dtype.name == 'Int64' and fill_value == 0 : #o np.issubdtype(type(fill_value), np.integer)
                    df[col] = df[col].astype('Int64') # Re-cast si fillna cambió el tipo
                logger.info(f"Imputados {original_null_count} NULLs en '{col}' con {fill_value}. Tipo final: {df[col].dtype}")
            else:
                logger.debug(f"No se encontraron NULLs en la columna '{col}'.")
        else:
            logger.warning(f"Columna '{col}' no encontrada para imputación de NULLs.")

    # Caso especial para 'construction_year' con mediana
    col_year = 'construction_year'
    if col_year in df.columns and df[col_year].isnull().any():
        # Calcular mediana solo de valores no nulos y asegurarse que es un escalar
        median_year = df[col_year].dropna().median()
        if pd.notna(median_year):
            original_null_count = df[col_year].isnull().sum()
            df[col_year] = df[col_year].fillna(int(median_year)).astype('Int64')
            logger.info(f"Imputados {original_null_count} NULLs en '{col_year}' con la mediana ({int(median_year)}).")
        else:
            logger.warning(f"No se pudo calcular la mediana para '{col_year}' (quizás todos los valores son NULL?). Imputando con 0 o pd.NA.")
            df[col_year] = df[col_year].fillna(pd.NA).astype('Int64') # o df[col_year].fillna(0).astype('Int64')

    # Verificar NaNs inesperados al final (opcional, pero útil)
    # final_nan_check = df.isnull().sum()
    # nans_remaining = final_nan_check[final_nan_check > 0]
    # if not nans_remaining.empty:
    #     logger.warning(f"Valores NaN aún permanecen en columnas después de la imputación: \n{nans_remaining}")
    # else:
    #     logger.info("No se detectaron valores NaN restantes en columnas clave después de la imputación.")
    return df

# --- Orquestador del Pipeline de Limpieza para Airbnb ---
def clean_airbnb_data_updated(df_input: pd.DataFrame) -> pd.DataFrame:
    """
    Aplica una secuencia de pasos de limpieza y transformación a un DataFrame de Airbnb.
    Esta es la función principal para la tarea de transformación de Airbnb.

    Args:
        df_input (pd.DataFrame): DataFrame crudo de Airbnb.

    Returns:
        pd.DataFrame: DataFrame transformado y limpio.
    """
    if not isinstance(df_input, pd.DataFrame):
        logger.error("La entrada para clean_airbnb_data_updated no es un DataFrame.")
        raise TypeError("La entrada debe ser un pandas DataFrame.")

    if df_input.empty:
        logger.warning("DataFrame de entrada para clean_airbnb_data_updated está vacío. Retornando DataFrame vacío.")
        return df_input.copy()

    logger.info(f"Iniciando pipeline de limpieza para datos de Airbnb. Shape entrada: {df_input.shape}")
    df = df_input.copy() # Trabajar con una copia

    # Aplicar cada paso de limpieza en la secuencia deseada
    df = _rename_columns_airbnb(df)
    df = _transform_price_columns_airbnb(df)
    df = _correct_neighbourhood_group_airbnb(df)
    # Es importante castear ANTES de transformaciones que esperan tipos específicos o imputaciones.
    df = _cast_numeric_columns_airbnb(df)
    df = _transform_last_review_airbnb(df) # Transformar fecha DESPUÉS de asegurar tipos base y renombrado.
    df = _handle_nulls_airbnb(df) # Imputar nulos al final

    # Opcional: Eliminar columnas que no se usarán después de todas las transformaciones
    # columns_to_drop_final = ['columna_temporal_si_hubiera', 'otra_col_no_necesaria']
    # existing_to_drop = [col for col in columns_to_drop_final if col in df.columns]
    # if existing_to_drop:
    #    df = df.drop(columns=existing_to_drop)
    #    logger.info(f"Columnas finales eliminadas: {existing_to_drop}")

    logger.info(f"Pipeline de limpieza de Airbnb finalizado. Shape salida: {df.shape}")
    logger.debug(f"Tipos de datos finales del DataFrame de Airbnb:\n{df.dtypes}")
    # logger.debug(f"Primeras filas del DataFrame de Airbnb transformado:\n{df.head().to_markdown(index=False)}")
    return df

if __name__ == '__main__':
    logging.basicConfig(level=logging.DEBUG, # Poner en DEBUG para ver más detalles durante la prueba
                        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')

    logger.info("--- Iniciando prueba local de clean_airbnb_data_updated ---")

    # Para probar, necesitarías cargar el CSV de Airbnb aquí.
    # Asumimos que existe en la ruta esperada por extract_airbnb_main_data
    # Es mejor si esta prueba es independiente de la extracción para enfocar en la transformación.
    # Puedes crear un pequeño DataFrame de ejemplo o cargar una muestra del CSV.

    # Ejemplo de creación de DataFrame de prueba (simplificado):
    sample_data_airbnb = {
        'ID': [1, 2, 3, 4],
        'NAME': ['Cozy Apartment', 'Sunny Room', None, 'Spacious Loft'],
        'host id': [101, 102, 103, 104],
        'host name': ['John D.', 'Jane S.', 'Alex P.', 'Sam B.'],
        'neighbourhood group': ['Manhattan', 'brookln', 'Queens', 'Manhattan'],
        'neighbourhood': ['Midtown', 'Bushwick', 'Astoria', 'Chelsea'],
        'lat': [40.7549, 40.6971, 40.7642, 40.7443],
        'long': [-73.9840, -73.9308, -73.9239, -73.9953],
        'country': ['United States', 'United States', 'United States', 'United States'],
        'country code': ['US', 'US', 'US', 'US'],
        'instant_bookable': [True, False, True, None], # Booleano o string 'TRUE'/'FALSE'
        'cancellation_policy': ['strict', 'moderate', 'flexible', 'strict'],
        'room type': ['Entire home/apt', 'Private room', 'Entire home/apt', 'Private room'],
        'Construction year': [2010, 2000, None, 1990], # 'Construction Year' en tu código original
        'price': ['$150', '$75 ', ' $200 ', '$120'], # Con espacios y $
        'service fee': ['$15', '$0', '$20', '$12'],
        'minimum nights': [1, 3, None, 2],
        'number of reviews': [100, 50, 0, 75],
        'last review': ['12/31/2022', '01/15/2023', None, '2023-03-10'], # Múltiples formatos
        'reviews per month': [1.5, 0.5, None, 0.8],
        'review rate number': [4, 5, None, 3],
        'calculated host listings count': [1, 2, 1, 3],
        'availability 365': [300, 150, 0, 200],
        'house_rules': ['No parties', None, 'Quiet hours after 10pm', 'No smoking'],
        # 'license': [None, 'LIC123', None, 'LIC456'] # En tu original dataset_clean se mantenía
    }
    test_df_airbnb_raw = pd.DataFrame(sample_data_airbnb)
    logger.info(f"DataFrame de prueba Airbnb original (primeras filas):\n{test_df_airbnb_raw.head().to_markdown(index=False)}")


    try:
        # Aquí podrías cargar el CSV real para una prueba más completa
        # project_root_test = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..'))
        # csv_path_test = os.path.join(project_root_test, 'data', 'raw', 'Airbnb_Open_Data.csv')
        # if os.path.exists(csv_path_test):
        #     logger.info(f"Cargando datos de prueba desde: {csv_path_test}")
        #     test_df_airbnb_raw_from_csv = pd.read_csv(csv_path_test, low_memory=False, encoding='ISO-8859-1')
        #     cleaned_df_airbnb = clean_airbnb_data_updated(test_df_airbnb_raw_from_csv)
        # else:
        #     logger.warning(f"Archivo CSV de prueba no encontrado en {csv_path_test}. Usando DataFrame de ejemplo interno.")
        #     cleaned_df_airbnb = clean_airbnb_data_updated(test_df_airbnb_raw)

        cleaned_df_airbnb = clean_airbnb_data_updated(test_df_airbnb_raw)
        logger.info(f"DataFrame de Airbnb limpio (primeras filas):\n{cleaned_df_airbnb.head().to_markdown(index=False)}")
        logger.info(f"Columnas del DataFrame de Airbnb limpio: {cleaned_df_airbnb.columns.tolist()}")
        logger.info(f"Tipos de datos del DataFrame de Airbnb limpio:\n{cleaned_df_airbnb.dtypes}")

        # Verificar algunos valores transformados específicos
        logger.info(f"Valores de 'price' después de la transformación: {cleaned_df_airbnb['price'].tolist()}")
        logger.info(f"Valores de 'last_review' después de la transformación: {cleaned_df_airbnb['last_review'].tolist()}")
        logger.info(f"Valores de 'neighbourhood_group' después de la corrección: {cleaned_df_airbnb['neighbourhood_group'].unique().tolist()}")


    except Exception as main_exception:
        logger.error(f"Error durante la prueba local: {main_exception}", exc_info=True)
    finally:
        logger.info("--- Prueba local de clean_airbnb_data_updated finalizada ---")