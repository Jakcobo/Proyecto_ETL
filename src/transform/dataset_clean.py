<<<<<<< HEAD
<<<<<<< HEAD
=======
=======
>>>>>>> aee7dd91 (restablecimiento a la version anterior)
/home/nicolas/Escritorio/proyecto/otra_prueba/src/transform/dataset_clean.py
# /home/nicolas/Escritorio/proyecto/otra_prueba/src/transform/dataset_clean.py
>>>>>>> aee7dd91 (restablecimiento a la version anterior)
import logging
import pandas as pd
import numpy as np
import os

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def rename_columns(df: pd.DataFrame) -> pd.DataFrame:
    """Renames columns by removing spaces and special characters."""
    logger.info("Renaming columns...")
    original_cols = df.columns.tolist()
    df.columns = df.columns.str.replace(' ', '_', regex=False)
    df.columns = df.columns.str.lower().str.replace('[^A-Za-z0-9_]+', '', regex=True)
    new_cols = df.columns.tolist()
    renamed_count = sum(1 for oc, nc in zip(original_cols, new_cols) if oc != nc)
    logger.info(f"Renamed {renamed_count} columns.")
    if renamed_count > 0:
         logger.debug(f"Column names after rename: {new_cols}")
    return df

def drop_unnecessary_columns(df: pd.DataFrame) -> pd.DataFrame:
    """Drops columns that are not needed."""
    columns_to_drop = [
        'host_name',      # Dropped because we already have host_id
        'country_code',   # Redundant if we have 'country' or if not used
        'country',        # Could be redundant or unnecessary depending on the analysis
        'house_rules',    # Long text, may not be useful for quantitative analysis
        'license'         # Mostly empty or non-standardized column
    ]
    logger.info(f"Attempting to drop columns: {columns_to_drop}")
    existing_columns_to_drop = [col for col in columns_to_drop if col in df.columns]
    if existing_columns_to_drop:
        df = df.drop(columns=existing_columns_to_drop)
        logger.info(f"Dropped columns: {existing_columns_to_drop}")
    else:
        logger.info("No columns from the drop list found in the DataFrame.")
    return df

def transform_price_columns(df: pd.DataFrame) -> pd.DataFrame:
    """Converts 'price' and 'service_fee' columns to numeric type."""
    logger.info("Transforming price columns to numeric...")
    for col in ['price', 'service_fee']:
        if col in df.columns:
            original_dtype = df[col].dtype
            if pd.api.types.is_object_dtype(original_dtype) or pd.api.types.is_string_dtype(original_dtype):
                logger.info(f"Converting column '{col}'...")
                df[col] = df[col].astype(str).str.replace(r'[$,]', '', regex=True)
                df[col] = pd.to_numeric(df[col], errors='coerce')
                # Impute NaN with 0 after conversion (or use mean/median if preferred)
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

    if 'price' in df.columns and df['price'].isnull().any():
        df['price'] = df['price'].fillna(0.0)
        logger.info("Imputed NaN values in 'price' with 0.0")
    if 'service_fee' in df.columns and df['service_fee'].isnull().any():
        df['service_fee'] = df['service_fee'].fillna(0.0)
        logger.info("Imputed NaN values in 'service_fee' with 0.0")

    return df


def transform_last_review(df: pd.DataFrame) -> pd.DataFrame:
    """Converts 'last_review' to date format and then to INTEGER YYYYMMDD."""
    if 'last_review' not in df.columns:
        logger.warning("Column 'last_review' not found. Skipping transformation.")
        return df

    logger.info("Transforming 'last_review' column...")
    df['last_review_dt'] = pd.to_datetime(df['last_review'], errors='coerce', infer_datetime_format=True)
    
    conversion_failures = df['last_review_dt'].isnull().sum() - df['last_review'].isnull().sum()
    if conversion_failures > 0:
        logger.warning(f"{conversion_failures} 'last_review' entries could not be parsed into dates.")

    default_date = pd.Timestamp('1900-01-01')
    df['last_review_dt'] = df['last_review_dt'].fillna(default_date)
    logger.info(f"Imputed missing/unparseable dates in 'last_review_dt' with {default_date.strftime('%Y-%m-%d')}.")
    df['last_review'] = df['last_review_dt'].dt.strftime('%Y%m%d').astype(int)
    df = df.drop(columns=['last_review_dt'])
    logger.info("'last_review' transformed to INTEGER (YYYYMMDD).")
    return df

def correct_neighbourhood_group(df: pd.DataFrame) -> pd.DataFrame:
    """Corrects known typos in 'neighbourhood_group'."""
    if 'neighbourhood_group' not in df.columns:
        logger.warning("Column 'neighbourhood_group' not found. Skipping correction.")
        return df

    logger.info("Correcting typos in 'neighbourhood_group'...")
    corrections = {
        'brookln': 'Brooklyn',
        'manhatan': 'Manhattan'
    }
    rows_to_change = df['neighbourhood_group'].isin(corrections.keys()).sum()
    if rows_to_change > 0:
        df['neighbourhood_group'] = df['neighbourhood_group'].replace(corrections)
        logger.info(f"Corrected {rows_to_change} typos in 'neighbourhood_group'.")
    else:
        logger.info("No known typos found in 'neighbourhood_group'.")
    return df

def cast_numeric_columns(df: pd.DataFrame) -> pd.DataFrame:
    """Ensures numeric columns have the correct type (Int64, Float64)."""
    logger.info("Casting numeric columns to appropriate types...")
    numeric_int_nullable = {
        'id': 'Int64',
        'host_id': 'Int64',
        'construction_year': 'Int64',
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
    }

    for col, dtype in numeric_int_nullable.items():
        if col in df.columns:
            if df[col].dtype != dtype:
                try:
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
    """Imputes null values in key columns after conversions."""
    logger.info("Handling NULL/NaN values...")
    imputation_map = {
        'reviews_per_month': 0.0,
        'review_rate_number': 0,
        'number_of_reviews': 0,
        'minimum_nights': 1,
        'construction_year': df['construction_year'].median(),
    }

    for col, value in imputation_map.items():
        if col in df.columns:
            if df[col].isnull().any():
                original_null_count = df[col].isnull().sum()
                if callable(value):
                    fill_value = value()
                else:
                    fill_value = value

                df[col] = df[col].fillna(fill_value)
                logger.info(f"Imputed {original_null_count} NULLs in '{col}' with {fill_value}.")
            else:
                 logger.debug(f"No NULLs found in column '{col}'.")
        else:
            logger.warning(f"Column '{col}' not found for NULL imputation.")

    if 'construction_year' in df.columns and df['construction_year'].isnull().any():
         median_year = df['construction_year'].median()
         if pd.notna(median_year):
              original_null_count = df['construction_year'].isnull().sum()
              df['construction_year'] = df['construction_year'].fillna(int(median_year)).astype('Int64') # Ensure Int type
              logger.info(f"Imputed {original_null_count} NULLs in 'construction_year' with median value {int(median_year)}.")
         else:
              logger.warning("Could not calculate median for 'construction_year' imputation (maybe all values are NULL?).")


    final_nan_check = df.isnull().sum()
    nans_remaining = final_nan_check[final_nan_check > 0]
    if not nans_remaining.empty:
        logger.warning(f"NaN values still remain in columns after imputation: \n{nans_remaining}")
    else:
        logger.info("No remaining NaN values detected in key columns after imputation.")

    return df

def clean_airbnb_data(df: pd.DataFrame) -> pd.DataFrame:
    """
    Applies a sequence of cleaning and transformation steps to an Airbnb DataFrame.
    This is the main entry point for cleaning.
    """
    logger.info(f"Starting data cleaning pipeline for DataFrame with shape {df.shape}...")
    if not isinstance(df, pd.DataFrame):
         logger.error("Input is not a pandas DataFrame.")
         raise TypeError("Input must be a pandas DataFrame.")

    df = rename_columns(df.copy())
    df = drop_unnecessary_columns(df)
    df = transform_price_columns(df)
    df = correct_neighbourhood_group(df)
    df = cast_numeric_columns(df)
    df = transform_last_review(df)
    df = handle_nulls(df)

    logger.info(f"Data cleaning pipeline finished. Output DataFrame shape: {df.shape}")
    logger.info(f"Final column types:\n{df.dtypes}")
    return df

if __name__ == "__main__":
    logger.info("Running dataset_clean.py in standalone mode for testing...")
    try:
        project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..'))
        csv_path = os.path.join(project_root, 'data', 'Airbnb_Open_Data.csv')
        logger.info(f"Loading test data from: {csv_path}")

        if not os.path.exists(csv_path):
            logger.error(f"CSV file not found at: {csv_path}")
            raise FileNotFoundError(f"CSV file not found at: {csv_path}")

        test_df = pd.read_csv(csv_path, low_memory=False, encoding='ISO-8859-1')
        logger.info(f"Loaded test DataFrame with shape: {test_df.shape}")

        cleaned_df = clean_airbnb_data(test_df)

        logger.info("Cleaned DataFrame info:")
        cleaned_df.info()
        logger.info("First 5 rows of cleaned DataFrame:")
        logger.info(cleaned_df.head().to_markdown(index=False))

    except FileNotFoundError as fnf:
        logger.error(f"Testing failed: {fnf}")
    except Exception as e:
        logger.error(f"Error during standalone test execution: {e}", exc_info=True)