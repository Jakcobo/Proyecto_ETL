from rapidfuzz import process, fuzz
import pandas as pd
import numpy as np
import logging
import os

logger = logging.getLogger(__name__)

def clean_airbnb_data(df_input: pd.DataFrame) -> pd.DataFrame:
    """
    Aplica una secuencia de pasos de limpieza y transformación a un DataFrame de Airbnb.
    Esta es la función principal para la tarea de transformación de Airbnb.

    Args:
        df_input (pd.DataFrame): DataFrame crudo de Airbnb.

    Returns:
        pd.DataFrame: DataFrame transformado y limpio.
    """
    logger.info("Inicio de la extración de datos de AirBnB.")

    if not df_input.empty:
        logger.info("Verificando filas duplicadas en df_input.")
        num_duplicados = df_input.duplicated().sum()
        logger.info(f"Número de filas duplicadas encontradas en df_input: {num_duplicados}")
    else:
        logger.critical("El DataFrame df_input está vacío después de la carga. Terminando el script.")
        
    logger.info("Iniciando limpieza preliminar y conversión de tipos de datos (versión optimizada).")
    df_cleaned = pd.DataFrame()

    if not df_input.empty:
        df_cleaned = df_input.copy()
        logger.info("Copia de df_input creada como df_cleaned.")

        original_columns = df_cleaned.columns.tolist()
        df_cleaned.columns = df_cleaned.columns.str.lower().str.replace(' ', '_', regex=False).str.replace('[^0-9a-zA-Z_]', '', regex=True)
        new_columns = df_cleaned.columns.tolist()
        logger.info(f"Columnas de df_cleaned normalizadas.")
        if original_columns != new_columns:
            logger.info(f"Cambios en nombres de columnas: {dict(zip(original_columns, new_columns))}")
        else:
            logger.info("Nombres de columnas ya estaban normalizados o no requirieron cambios significativos.")

        def clean_string_column(series, col_name):
            logger.debug(f"Limpiando columna string: {col_name}")
            series = series.astype(str).str.strip().replace({'nan': pd.NA, '': pd.NA, 'None': pd.NA})
            return series

        def to_numeric_column(series, col_name, numeric_type='Int64'):
            logger.debug(f"Convirtiendo columna a numérica ({numeric_type}): {col_name}")
            nulls_before = series.isna().sum()
            if numeric_type == 'datetime':
                series = pd.to_datetime(series, format='%m/%d/%Y', errors='coerce')
            else:
                series = pd.to_numeric(series, errors='coerce')
                if numeric_type == 'Int64' and not series.empty:
                    if series.dropna().apply(lambda x: x.is_integer()).all() or series.dropna().empty:
                        series = series.astype('Int64')
                    else:
                        logger.warning(f"Columna '{col_name}' contiene flotantes, no se convertirá a Int64, se mantendrá como float.")
            
            coerced_nulls = series.isna().sum() - nulls_before
            if coerced_nulls > 0:
                logger.warning(f"Columna '{col_name}': {coerced_nulls} nuevos NaNs/NaTs por coerción.")
            return series

        def standardize_categorical_fuzz(series, col_name, choices_list, score_cutoff=85):
            logger.debug(f"Estandarizando columna categórica con RapidFuzz: {col_name}")
            unique_values = series.dropna().unique()
            mapping = {}
            for val in unique_values:
                match = process.extractOne(str(val), choices_list, scorer=fuzz.WRatio, score_cutoff=score_cutoff)
                if match:
                    mapping[val] = match[0]
                else:
                    mapping[val] = val
            
            original_na_mask = series.isna()
            series_mapped = series.map(mapping)
            series_mapped[original_na_mask] = pd.NA
            
            changes = (series.dropna() != series_mapped.dropna()).sum()
            if changes > 0:
                logger.info(f"Columna '{col_name}': {changes} valores estandarizados usando RapidFuzz.")
            return series_mapped

        try:
            numeric_cols_int = ['construction_year', 'minimum_nights', 'number_of_reviews', 'review_rate_number', 
                                'calculated_host_listings_count', 'availability_365']
            
            for col in numeric_cols_int:
                if col in df_cleaned.columns:
                    df_cleaned[col] = to_numeric_column(df_cleaned[col], col, 'Int64')
                else: logger.warning(f"Columna '{col}' no encontrada para conversión numérica (Int64).")

            numeric_cols_float = ['lat', 'long', 'reviews_per_month']
            for col in numeric_cols_float:
                if col in df_cleaned.columns:
                    df_cleaned[col] = to_numeric_column(df_cleaned[col], col, 'float')
                else: logger.warning(f"Columna '{col}' no encontrada para conversión numérica (float).")

            df_cleaned = df_cleaned.reset_index(drop=True)
            df_cleaned['id'] = df_cleaned.index + 1
            df_cleaned = df_cleaned.reset_index(drop=True)
            df_cleaned['host_id'] = df_cleaned.index + 150000
            
            string_cols = ['name', 'host_name']
            for col in string_cols:
                if col in df_cleaned.columns:
                    df_cleaned[col] = clean_string_column(df_cleaned[col], col)
                else: logger.warning(f"Columna '{col}' no encontrada para limpieza de string.")

            categorical_cols_pre_fuzz = ['neighbourhood_group', 'neighbourhood']
            for col in categorical_cols_pre_fuzz:
                if col in df_cleaned.columns:
                    df_cleaned[col] = clean_string_column(df_cleaned[col], col)
                    if col == 'neighbourhood_group' and col in df_cleaned.columns:
                        canonical_groups = ['Manhattan', 'Brooklyn', 'Queens', 'Bronx', 'Staten Island']
                        if not df_cleaned[col].dropna().empty:
                            df_cleaned[col] = standardize_categorical_fuzz(df_cleaned[col], col, canonical_groups, score_cutoff=80)
                            logger.info(f"RapidFuzz aplicado a '{col}'.")
                        else:
                            logger.info(f"Columna '{col}' está vacía o solo nulos, RapidFuzz no aplicado.")

                    nunique_threshold = 50 if col == 'neighbourhood' else 20 
                    if col in df_cleaned.columns and df_cleaned[col].nunique(dropna=False) < nunique_threshold:
                        df_cleaned[col] = df_cleaned[col].astype('category')
                        logger.info(f"Columna '{col}' convertida a category.")
                    elif col in df_cleaned.columns:
                        logger.info(f"Columna '{col}' limpiada (no convertida a category debido a alta cardinalidad: {df_cleaned[col].nunique(dropna=False)}).")

                else: logger.warning(f"Columna '{col}' no encontrada para limpieza categórica.")

            category_cols_direct = ['cancellation_policy', 'room_type']
            for col in category_cols_direct:
                if col in df_cleaned.columns:
                    df_cleaned[col] = clean_string_column(df_cleaned[col], col).astype('category')
                    logger.info(f"Columna '{col}' convertida a category.")
                else: logger.warning(f"Columna '{col}' no encontrada para conversión a category.")
            
            if 'last_review' in df_cleaned.columns:
                df_cleaned['last_review'] = to_numeric_column(df_cleaned['last_review'], 'last_review', 'datetime')
                logger.info("Columna 'last_review' convertida a datetime.")
            else: logger.warning("Columna 'last_review' no encontrada.")

            if 'host_identity_verified' in df_cleaned.columns:
                verified_map = {'verified': True, 'unconfirmed': False}
                df_cleaned['host_verification'] = df_cleaned['host_identity_verified'].map(verified_map).astype('boolean')
                logger.info("Columnas 'host_verification', creada a partir de 'host_identity_verified'.")
            else: logger.warning("Columna 'host_identity_verified' no encontrada.")

            if 'instant_bookable' in df_cleaned.columns:
                bookable_map = {'TRUE': True, 'FALSE': False, 'True': True, 'False': False, 'true': True, 'false': False} # Cubrir varias capitalizaciones
                df_cleaned['instant_bookable_flag'] = df_cleaned['instant_bookable'].astype(str).str.upper().map(bookable_map).astype('boolean')
                logger.info("Columna 'instant_bookable_flag' creada a partir de 'instant_bookable'.")
            else: logger.warning("Columna 'instant_bookable' no encontrada.")

            currency_cols = {'price': 'price', 'service_fee': 'service_fee'}
            for original_col, new_col_numeric in currency_cols.items():
                if original_col in df_cleaned.columns:
                    series_cleaned_str = df_cleaned[original_col].astype(str).str.replace('$', '', regex=False).str.replace(',', '', regex=False).str.strip().replace({'nan': pd.NA, '': pd.NA})
                    df_cleaned[new_col_numeric] = to_numeric_column(series_cleaned_str, new_col_numeric, 'float')
                    logger.info(f"Columna '{new_col_numeric}' creada a partir de '{original_col}'.")
                else: logger.warning(f"Columna original '{original_col}' no encontrada para procesar moneda.")

            cols_to_drop_original_names = ['host_identity_verified', 'instant_bookable', 'country', 'country_code', 'license', 'house_rules']
            
            normalized_cols_to_drop = []
            for col_name in cols_to_drop_original_names:
                normalized_name = col_name.lower().replace(' ', '_').replace('[^0-9a-zA-Z_]', '')
                if normalized_name in df_cleaned.columns:
                    normalized_cols_to_drop.append(normalized_name)
                elif col_name in df_cleaned.columns:
                    normalized_cols_to_drop.append(col_name)

            existing_cols_to_drop = [col for col in normalized_cols_to_drop if col in df_cleaned.columns]
            if existing_cols_to_drop:
                df_cleaned.drop(columns=existing_cols_to_drop, inplace=True, errors='ignore')
                logger.info(f"Columnas {existing_cols_to_drop} eliminadas de df_cleaned.")
            
            logger.info("Proceso de limpieza preliminar y conversión de tipos optimizado completado.")
            
            return df_cleaned

        except KeyError as ke:
            logger.error(f"Ocurrió un KeyError durante la limpieza: '{ke}'. Verifica que la columna exista en df_cleaned (posiblemente después de la normalización).")
            logger.error(f"Columnas disponibles en df_cleaned: {df_cleaned.columns.tolist()}")
            logger.error(f"Ocurrió un KeyError: '{ke}'. Revisa los nombres de las columnas y la lógica de normalización.")
        except Exception as e:
            logger.error(f"Ocurrió un error general durante la limpieza: {e}", exc_info=True)
            logger.error(f"Ocurrió un error general: {e}")

    else:
        logger.warning("El DataFrame df_input está vacío. No se puede realizar la limpieza.")
        raise

    logger.info("Proceso finalizado exitosamente.")