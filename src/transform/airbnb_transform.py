# src/transform/clean_airbnb.py

import pandas as pd
import numpy as np
import logging
from rapidfuzz import process, fuzz

logger = logging.getLogger(__name__)

def clean_airbnb_data(df_input: pd.DataFrame) -> pd.DataFrame:
    """
    Limpia y transforma el DataFrame de Airbnb aplicando reglas de imputación,
    eliminación y estandarización.

    Args:
        df_input (pd.DataFrame): Datos crudos de Airbnb.

    Returns:
        pd.DataFrame: DataFrame limpio y transformado.
    """
    df_cleaned = pd.DataFrame()

    if df_input.empty:
        logger.warning("El DataFrame de entrada está vacío. No se puede limpiar.")
        return df_cleaned

    logger.info("Iniciando limpieza de datos de Airbnb.")
    df_cleaned = df_input.copy()

    # --- Normalización de nombres de columnas ---
    original_columns = df_cleaned.columns.tolist()
    df_cleaned.columns = df_cleaned.columns.str.lower().str.replace(' ', '_', regex=False).str.replace('[^0-9a-zA-Z_]', '', regex=True)
    logger.info("Columnas normalizadas.")

    # --- Funciones auxiliares ---
    def clean_string_column(series, col_name):
        return series.astype(str).str.strip().replace({'nan': pd.NA, '': pd.NA, 'None': pd.NA})

    def to_numeric_column(series, col_name, numeric_type='Int64'):
        nulls_before = series.isna().sum()
        if numeric_type == 'datetime':
            series = pd.to_datetime(series, errors='coerce')
        else:
            series = pd.to_numeric(series, errors='coerce')
            if numeric_type == 'Int64' and not series.empty:
                if series.dropna().apply(lambda x: x.is_integer()).all() or series.dropna().empty:
                    series = series.astype('Int64')
        coerced_nulls = series.isna().sum() - nulls_before
        if coerced_nulls > 0:
            logger.warning(f"Columna '{col_name}': {coerced_nulls} nuevos NaNs/NaTs por coerción.")
        return series

    def standardize_categorical_fuzz(series, col_name, choices_list, score_cutoff=85):
        mapping = {}
        for val in series.dropna().unique():
            match = process.extractOne(str(val), choices_list, scorer=fuzz.WRatio, score_cutoff=score_cutoff)
            mapping[val] = match[0] if match else val
        result = series.map(mapping)
        result[series.isna()] = pd.NA
        return result

    # --- Conversión de columnas numéricas ---
    numeric_cols_int = ['construction_year', 'minimum_nights', 'number_of_reviews', 'review_rate_number', 'calculated_host_listings_count', 'availability_365']
    for col in numeric_cols_int:
        if col in df_cleaned.columns:
            df_cleaned[col] = to_numeric_column(df_cleaned[col], col, 'Int64')

    numeric_cols_float = ['lat', 'long', 'reviews_per_month']
    for col in numeric_cols_float:
        if col in df_cleaned.columns:
            df_cleaned[col] = to_numeric_column(df_cleaned[col], col, 'float')

    df_cleaned = df_cleaned.reset_index(drop=True)
    df_cleaned['id'] = df_cleaned.index + 1
    df_cleaned['host_id'] = df_cleaned.index + 150000

    # --- Strings y categorías ---
    for col in ['name', 'host_name']:
        if col in df_cleaned.columns:
            df_cleaned[col] = clean_string_column(df_cleaned[col], col)

    if 'neighbourhood_group' in df_cleaned.columns:
        df_cleaned['neighbourhood_group'] = clean_string_column(df_cleaned['neighbourhood_group'], 'neighbourhood_group')
        df_cleaned['neighbourhood_group'] = standardize_categorical_fuzz(df_cleaned['neighbourhood_group'], 'neighbourhood_group', ['Manhattan', 'Brooklyn', 'Queens', 'Bronx', 'Staten Island'], score_cutoff=80)
        df_cleaned['neighbourhood_group'] = df_cleaned['neighbourhood_group'].astype('category')

    if 'neighbourhood' in df_cleaned.columns:
        df_cleaned['neighbourhood'] = clean_string_column(df_cleaned['neighbourhood'], 'neighbourhood')
        if df_cleaned['neighbourhood'].nunique() < 50:
            df_cleaned['neighbourhood'] = df_cleaned['neighbourhood'].astype('category')

    for col in ['cancellation_policy', 'room_type']:
        if col in df_cleaned.columns:
            df_cleaned[col] = clean_string_column(df_cleaned[col], col).astype('category')

    if 'last_review' in df_cleaned.columns:
        df_cleaned['last_review'] = to_numeric_column(df_cleaned['last_review'], 'last_review', 'datetime')

    if 'host_identity_verified' in df_cleaned.columns:
        df_cleaned['host_verification'] = df_cleaned['host_identity_verified'].map({'verified': True, 'unconfirmed': False}).astype('boolean')

    if 'instant_bookable' in df_cleaned.columns:
        map_bookable = {'TRUE': True, 'FALSE': False, 'True': True, 'False': False, 'true': True, 'false': False}
        df_cleaned['instant_bookable_flag'] = df_cleaned['instant_bookable'].astype(str).str.upper().map(map_bookable).astype('boolean')

    # --- Conversión de columnas monetarias ---
    currency_cols = {'price': 'price', 'service_fee': 'service_fee'}
    for col in currency_cols:
        if col in df_cleaned.columns:
            clean_col = df_cleaned[col].astype(str).str.replace('$', '', regex=False).str.replace(',', '', regex=False).str.strip().replace({'nan': pd.NA, '': pd.NA})
            df_cleaned[col] = to_numeric_column(clean_col, col, 'float')

    # --- Eliminar columnas innecesarias ---
    drop_cols = ['host_identity_verified', 'instant_bookable', 'country', 'country_code', 'license', 'house_rules']
    df_cleaned.drop(columns=[col for col in drop_cols if col in df_cleaned.columns], inplace=True, errors='ignore')

    # --- Imputación personalizada ---
    df_cleaned['name'] = df_cleaned['name'].fillna("no_name")
    df_cleaned['host_name'] = df_cleaned['host_name'].fillna("unknown_host")

    if 'cancellation_policy' in df_cleaned.columns:
        if not isinstance(df_cleaned['cancellation_policy'].dtype, pd.CategoricalDtype):
            df_cleaned['cancellation_policy'] = df_cleaned['cancellation_policy'].astype('category')
        if "moderate" not in df_cleaned['cancellation_policy'].cat.categories:
            df_cleaned['cancellation_policy'] = df_cleaned['cancellation_policy'].cat.add_categories(["moderate"])
        df_cleaned['cancellation_policy'] = df_cleaned['cancellation_policy'].fillna("moderate")

    df_cleaned.dropna(subset=['neighbourhood_group', 'neighbourhood', 'lat', 'long'], inplace=True)

    df_cleaned['construction_year'] = df_cleaned['construction_year'].fillna(2012).astype('Int64')
    df_cleaned['price'] = df_cleaned['price'].fillna(624.0)
    df_cleaned['service_fee'] = df_cleaned['service_fee'].fillna(125.0)
    df_cleaned['minimum_nights'] = df_cleaned['minimum_nights'].fillna(3).astype('Int64')
    df_cleaned['number_of_reviews'] = df_cleaned['number_of_reviews'].fillna(0).astype('Int64')
    df_cleaned['reviews_per_month'] = df_cleaned['reviews_per_month'].fillna(0.0)

    df_cleaned = df_cleaned[~((df_cleaned['number_of_reviews'] == 0) &
                              (df_cleaned['last_review'].isna()) &
                              (df_cleaned['review_rate_number'].isna()))]

    df_cleaned['calculated_host_listings_count'] = df_cleaned['calculated_host_listings_count'].fillna(1).astype('Int64')
    df_cleaned['availability_365'] = df_cleaned['availability_365'].fillna(96.0).astype('Int64')

    df_cleaned['host_verification'] = df_cleaned['host_verification'].fillna(False)
    df_cleaned['instant_bookable_flag'] = df_cleaned['instant_bookable_flag'].fillna(False)

    # --- Imputar last_review con fecha máxima válida ---
    if 'last_review' in df_cleaned.columns:
        fecha_ficticia = pd.Timestamp("2262-04-11 00:00:00")
        df_cleaned['last_review'] = df_cleaned['last_review'].fillna(fecha_ficticia)

    # --- Eliminar filas con review_rate_number nulo ---
    df_cleaned.dropna(subset=['review_rate_number'], inplace=True)

    logger.info("Limpieza de datos de Airbnb completada.")
    return df_cleaned
