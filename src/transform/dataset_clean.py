# /home/nicolas/Escritorio/proyecto ETL/develop/src/transform/dataset_clean.py
import os
import logging
import pandas as pd
import numpy as np # Necesario para np.nan

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# --- Funciones de Limpieza Individuales (Etapas del Pipeline) ---

def rename_columns(df: pd.DataFrame) -> pd.DataFrame:
    """Renombra columnas quitando espacios, caracteres especiales y convirtiendo a minúsculas."""
    logger.info("Renaming columns (replace spaces, convert to lowercase)...")
    original_cols = df.columns.tolist()
    # 1. Reemplazar espacios
    df.columns = df.columns.str.replace(' ', '_', regex=False)
    # 2. Convertir a minúsculas <-- AÑADIR ESTA LÍNEA
    df.columns = df.columns.str.lower()
    # Opcional: Limpieza más agresiva (si la necesitas)
    # df.columns = df.columns.str.replace('[^A-Za-z0-9_]+', '', regex=True)

    new_cols = df.columns.tolist()
    renamed_cols_map = {oc: nc for oc, nc in zip(original_cols, new_cols) if oc != nc}
    if renamed_cols_map:
         logger.info(f"Renamed {len(renamed_cols_map)} columns.")
         # Loguear solo algunos ejemplos si son muchos
         log_limit = 5
         logged_renames = {k: v for i, (k, v) in enumerate(renamed_cols_map.items()) if i < log_limit}
         logger.debug(f"Example renames: {logged_renames}")
         if len(renamed_cols_map) > log_limit:
              logger.debug(f"...and {len(renamed_cols_map) - log_limit} more.")
    else:
         logger.info("No column names required renaming.")
    return df

def drop_unnecessary_columns(df: pd.DataFrame) -> pd.DataFrame:
    """Elimina columnas que no se necesitan."""
    # Asegúrate que 'name' NO esté en esta lista
    columns_to_drop = [
        # 'host_name',        # Quitado para dim_host
        # 'country_code',     # Quitado para dim_spot_location
        # 'country',          # Quitado para dim_spot_location
        # 'house_rules',      # Quitado para dim_property
        # 'license'           # Quitado para dim_property
    ]
    logger.info(f"Attempting to drop columns: {columns_to_drop}")
    existing_columns_in_df = [col for col in columns_to_drop if col in df.columns]
    if existing_columns_in_df:
        df = df.drop(columns=existing_columns_in_df)
        logger.info(f"Dropped columns: {existing_columns_in_df}")
    else:
        logger.info(f"No columns from the configured drop list ({columns_to_drop}) found in the DataFrame or list is empty.")
    return df

def transform_price_columns(df: pd.DataFrame) -> pd.DataFrame:
    """Convierte columnas 'price' y 'service_fee' a tipo numérico."""
    logger.info("Transforming price columns to numeric...")
    for col in ['price', 'service_fee']:
        if col in df.columns:
             # Guarda el dtype original para comparar
             original_dtype = df[col].dtype
             if pd.api.types.is_object_dtype(original_dtype) or pd.api.types.is_string_dtype(original_dtype):
                  logger.info(f"Converting column '{col}'...")
                  # Elimina '$', ',', convierte a numérico. Los errores se fuerzan a NaN.
                  df[col] = df[col].astype(str).str.replace(r'[$,]', '', regex=True)
                  df[col] = pd.to_numeric(df[col], errors='coerce')
                  # Imputa NaN con 0 después de la conversión (o usa media/mediana si prefieres)
                  # count_nan = df[col].isnull().sum()
                  # if count_nan > 0:
                  #      logger.warning(f"Found {count_nan} NULLs/NaNs in '{col}' after conversion. Imputing with 0.")
                  #      df[col] = df[col].fillna(0.0)
                  logger.info(f"Column '{col}' converted to numeric. New type: {df[col].dtype}")
             elif pd.api.types.is_numeric_dtype(original_dtype):
                  logger.info(f"Column '{col}' is already numeric ({original_dtype}). Skipping conversion.")
             else:
                   logger.warning(f"Column '{col}' has an unexpected type ({original_dtype}). Skipping conversion.")
        else:
             logger.warning(f"Column '{col}' not found for price transformation.")

    # Imputación separada por claridad
    if 'price' in df.columns and df['price'].isnull().any():
        df['price'] = df['price'].fillna(0.0)
        logger.info("Imputed NaN values in 'price' with 0.0")
    if 'service_fee' in df.columns and df['service_fee'].isnull().any():
        df['service_fee'] = df['service_fee'].fillna(0.0)
        logger.info("Imputed NaN values in 'service_fee' with 0.0")

    return df


def transform_last_review(df: pd.DataFrame) -> pd.DataFrame:
    """Convierte 'last_review' a formato de fecha y luego a INTEGER YYYYMMDD."""
    if 'last_review' not in df.columns:
        logger.warning("Column 'last_review' not found. Skipping transformation.")
        return df

    logger.info("Transforming 'last_review' column...")
    # Intenta convertir a datetime, manejando errores y múltiples formatos si es necesario
    # 'infer_datetime_format=True' puede ayudar pero es mejor ser explícito si conoces los formatos
    df['last_review_dt'] = pd.to_datetime(df['last_review'], errors='coerce', infer_datetime_format=True)
    # Si 'infer_datetime_format' falla, puedes probar formatos específicos:
    # df['last_review_dt'] = pd.to_datetime(df['last_review'], errors='coerce', format='%m/%d/%Y') # Prueba un formato
    # mask = df['last_review_dt'].isnull() & df['last_review'].notnull()
    # df.loc[mask, 'last_review_dt'] = pd.to_datetime(df.loc[mask, 'last_review'], errors='coerce', format='%Y-%m-%d') # Prueba otro

    # Cuenta cuántos no se pudieron convertir
    conversion_failures = df['last_review_dt'].isnull().sum() - df['last_review'].isnull().sum()
    if conversion_failures > 0:
        logger.warning(f"{conversion_failures} 'last_review' entries could not be parsed into dates.")

    # Imputar NaT (Not a Time) con una fecha por defecto ANTES de convertir a int
    default_date = pd.Timestamp('1900-01-01')
    df['last_review_dt'] = df['last_review_dt'].fillna(default_date)
    logger.info(f"Imputed missing/unparseable dates in 'last_review_dt' with {default_date.strftime('%Y-%m-%d')}.")

    # Convertir a formato YYYYMMDD como entero
    # Asegúrate de que no haya NaT restantes antes de esta operación
    df['last_review'] = df['last_review_dt'].dt.strftime('%Y%m%d').astype(int)

    # Eliminar la columna temporal de datetime
    df = df.drop(columns=['last_review_dt'])
    logger.info("'last_review' transformed to INTEGER (YYYYMMDD).")
    return df

def correct_neighbourhood_group(df: pd.DataFrame) -> pd.DataFrame:
    """Corrige typos conocidos en 'neighbourhood_group'."""
    if 'neighbourhood_group' not in df.columns:
        logger.warning("Column 'neighbourhood_group' not found. Skipping correction.")
        return df

    logger.info("Correcting typos in 'neighbourhood_group'...")
    corrections = {
        'brookln': 'Brooklyn',
        'manhatan': 'Manhattan'
    }
    # Contar cuántos se van a cambiar
    rows_to_change = df['neighbourhood_group'].isin(corrections.keys()).sum()
    if rows_to_change > 0:
        df['neighbourhood_group'] = df['neighbourhood_group'].replace(corrections)
        logger.info(f"Corrected {rows_to_change} typos in 'neighbourhood_group'.")
    else:
        logger.info("No known typos found in 'neighbourhood_group'.")
    return df

def cast_numeric_columns(df: pd.DataFrame) -> pd.DataFrame:
    """Asegura que las columnas numéricas tengan el tipo correcto (Int64, Float64)."""
    logger.info("Casting numeric columns to appropriate types...")
    # Usamos Int64 (nullable integer) para permitir NaNs si ocurren antes de la imputación
    numeric_int_nullable = {
        'id': 'Int64',
        'host_id': 'Int64',
        'Construction_year': 'Int64', # Año es entero
        'minimum_nights': 'Int64',
        'number_of_reviews': 'Int64',
        'review_rate_number': 'Int64',
        'calculated_host_listings_count': 'Int64',
        'availability_365': 'Int64'
    }
    numeric_float = {
        'lat': 'Float64',
        'long': 'Float64',
        'reviews_per_month': 'Float64'
        # price y service_fee ya deberían ser float si transform_price_columns se ejecutó
    }

    for col, dtype in numeric_int_nullable.items():
        if col in df.columns:
            if df[col].dtype != dtype:
                try:
                    # Convertir a float primero puede ayudar si hay strings como '1.0'
                    df[col] = pd.to_numeric(df[col], errors='coerce').astype(dtype)
                    logger.info(f"Column '{col}' casted to {dtype}.")
                except Exception as e:
                    logger.warning(f"Could not cast column '{col}' to {dtype}. Error: {e}")
            else:
                 logger.debug(f"Column '{col}' already has type {dtype}.")


    for col, dtype in numeric_float.items():
        if col in df.columns:
            if df[col].dtype != dtype:
                try:
                    df[col] = pd.to_numeric(df[col], errors='coerce').astype(dtype)
                    logger.info(f"Column '{col}' casted to {dtype}.")
                except Exception as e:
                    logger.warning(f"Could not cast column '{col}' to {dtype}. Error: {e}")
            else:
                 logger.debug(f"Column '{col}' already has type {dtype}.")

    return df

def handle_nulls(df: pd.DataFrame) -> pd.DataFrame:
    """Imputa valores nulos en columnas clave después de las conversiones."""
    logger.info("Handling NULL/NaN values...")
    imputation_map = {
        # Columna: Valor de imputación
        'reviews_per_month': 0.0,
        'review_rate_number': 0,        # Asumiendo 0 si no hay review rate
        'number_of_reviews': 0,         # Asumiendo 0 si no hay reviews
        'minimum_nights': 1,            # Asumiendo mínimo 1 noche por defecto
        # 'Construction_year': df['Construction_year'].median(), # Ejemplo con mediana si prefieres
        # price y service_fee ya se imputaron en su función de transformación
    }

    for col, value in imputation_map.items():
        if col in df.columns:
            if df[col].isnull().any():
                original_null_count = df[col].isnull().sum()
                if callable(value): # Si el valor es una función (ej. lambda: df[col].median())
                    fill_value = value()
                else:
                    fill_value = value

                df[col] = df[col].fillna(fill_value)
                logger.info(f"Imputed {original_null_count} NULLs in '{col}' with {fill_value}.")
            else:
                 logger.debug(f"No NULLs found in column '{col}'.")
        else:
            logger.warning(f"Column '{col}' not found for NULL imputation.")

    # Caso especial para Construction_year si se decide imputar con mediana/media
    if 'Construction_year' in df.columns and df['Construction_year'].isnull().any():
         median_year = df['Construction_year'].median()
         if pd.notna(median_year):
              original_null_count = df['Construction_year'].isnull().sum()
              df['Construction_year'] = df['Construction_year'].fillna(int(median_year)).astype('Int64') # Asegurar tipo Int
              logger.info(f"Imputed {original_null_count} NULLs in 'Construction_year' with median value {int(median_year)}.")
         else:
              logger.warning("Could not calculate median for 'Construction_year' imputation (maybe all values are NULL?).")


    # Verificar si quedan NaNs inesperados
    final_nan_check = df.isnull().sum()
    nans_remaining = final_nan_check[final_nan_check > 0]
    if not nans_remaining.empty:
        logger.warning(f"NaN values still remain in columns after imputation: \n{nans_remaining}")
    else:
        logger.info("No remaining NaN values detected in key columns after imputation.")

    return df


# --- Orquestador del Pipeline de Limpieza ---

def clean_airbnb_data(df: pd.DataFrame) -> pd.DataFrame:
    """
    Aplica una secuencia de pasos de limpieza y transformación a un DataFrame de Airbnb.
    Este es el punto de entrada principal para la limpieza.
    """
    logger.info(f"Starting data cleaning pipeline for DataFrame with shape {df.shape}...")
    if not isinstance(df, pd.DataFrame):
         logger.error("Input is not a pandas DataFrame.")
         raise TypeError("Input must be a pandas DataFrame.")

    # Aplicar cada paso de limpieza en secuencia
    df = rename_columns(df.copy()) # Usar .copy() para evitar SettingWithCopyWarning
    df = drop_unnecessary_columns(df)
    df = transform_price_columns(df)
    df = correct_neighbourhood_group(df)
    df = cast_numeric_columns(df) # Hacer casting ANTES de transformaciones que esperan tipos específicos
    df = transform_last_review(df) # Transformar fecha DESPUÉS de asegurar tipos base
    df = handle_nulls(df) # Imputar nulos al final

    logger.info(f"Data cleaning pipeline finished. Output DataFrame shape: {df.shape}")
    logger.info(f"Final column types:\n{df.dtypes}")
    return df


# --- Bloque de Ejecución Standalone (para pruebas) ---

if __name__ == "__main__":
    # Este bloque ahora se usaría principalmente para probar la lógica de limpieza
    # cargando el CSV directamente aquí, en lugar de operar sobre la DB.
    logger.info("Running dataset_clean.py in standalone mode for testing...")
    try:
        # Determinar la ruta al CSV de forma robusta
        project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..'))
        csv_path = os.path.join(project_root, 'data', 'Airbnb_Open_Data.csv')
        logger.info(f"Loading test data from: {csv_path}")

        if not os.path.exists(csv_path):
            logger.error(f"CSV file not found at: {csv_path}")
            raise FileNotFoundError(f"CSV file not found at: {csv_path}")

        # Cargar datos de prueba
        test_df = pd.read_csv(csv_path, low_memory=False, encoding='ISO-8859-1')
        logger.info(f"Loaded test DataFrame with shape: {test_df.shape}")

        # Ejecutar el pipeline de limpieza
        cleaned_df = clean_airbnb_data(test_df)

        # Opcional: Mostrar información sobre el DataFrame limpio
        logger.info("Cleaned DataFrame info:")
        cleaned_df.info()
        logger.info("First 5 rows of cleaned DataFrame:")
        print(cleaned_df.head().to_markdown(index=False)) # Usar print para ver el head formateado

        # Opcional: Guardar el DataFrame limpio a un nuevo CSV para inspección
        # output_csv_path = os.path.join(project_root, 'data', 'Airbnb_Open_Data_CLEANED.csv')
        # cleaned_df.to_csv(output_csv_path, index=False)
        # logger.info(f"Cleaned data saved for inspection to: {output_csv_path}")

    except FileNotFoundError as fnf:
        logger.error(f"Testing failed: {fnf}")
    except Exception as e:
        logger.error(f"Error during standalone test execution: {e}", exc_info=True)