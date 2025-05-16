# /home/nicolas/Escritorio/proyecto ETL/develop/src/load/load_dimensional.py

import pandas as pd
import logging
from sqlalchemy import text
from sqlalchemy.exc import SQLAlchemyError

# Asegurarse de que src esté en el path para importar db y definir PROCESSED_DATA_PATH
import sys
import os
PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..')) # Raíz del proyecto (develop)
SRC_PATH = os.path.join(PROJECT_ROOT, "src")
if SRC_PATH not in sys.path:
    sys.path.insert(0, SRC_PATH)
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

DB_MODULE_PATH = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', 'database'))
if DB_MODULE_PATH not in sys.path:
     sys.path.insert(0, DB_MODULE_PATH)

try:
    from database.db import get_db_engine
except ImportError:
    logger_fallback = logging.getLogger(__name__ + "_fallback")
    logger_fallback.error("Error importando get_db_engine. Asegúrate que 'src' esté en PYTHONPATH.")

logger = logging.getLogger(__name__)

# Definir la ruta a la carpeta de datos procesados
PROCESSED_DATA_PATH = os.path.join(PROJECT_ROOT, "data", "processed") # Asegúrate que "processed" sea correcto

def _ensure_processed_dir_exists():
    """Asegura que el directorio data/processed exista."""
    if not os.path.exists(PROCESSED_DATA_PATH):
        try:
            os.makedirs(PROCESSED_DATA_PATH)
            logger.info(f"Directorio creado: {PROCESSED_DATA_PATH}")
        except OSError as e:
            logger.error(f"Error al crear el directorio {PROCESSED_DATA_PATH}: {e}")
            raise

def _save_df_to_processed_csv(df: pd.DataFrame, table_name_as_filename_base: str):
    """Guarda un DataFrame en la carpeta data/processed como CSV, usando el nombre de la tabla."""
    if df is None or df.empty:
        logger.warning(f"DataFrame para '{table_name_as_filename_base}' está vacío o es None. No se guardará el CSV.")
        return

    _ensure_processed_dir_exists()
    file_name = f"{table_name_as_filename_base}.csv"
    file_path = os.path.join(PROCESSED_DATA_PATH, file_name)
    try:
        df.to_csv(file_path, index=False, encoding='utf-8')
        logger.info(f"DataFrame para '{table_name_as_filename_base}' guardado exitosamente como CSV en: {file_path}")
    except Exception as e:
        logger.error(f"Error al guardar DataFrame para '{table_name_as_filename_base}' como CSV en '{file_path}': {e}", exc_info=True)
        # raise # Opcional: decidir si este error debe ser fatal

# --- Funciones de Carga para Dimensiones (sin cambios en su lógica interna de BD) ---
def load_dimension_data(
    engine,
    df_dimension: pd.DataFrame,
    table_name: str,
    natural_key_cols: list,
    dimension_pk_col: str,
    save_csv: bool = True # Nuevo parámetro
):
    if df_dimension.empty:
        logger.info(f"DataFrame para la dimensión '{table_name}' está vacío. No se procesará.")
        return

    if save_csv:
        _save_df_to_processed_csv(df_dimension, table_name) # Guardar CSV

    # ... (resto de la lógica de carga a BD sin cambios)
    cols_to_insert = [col for col in df_dimension.columns if col != dimension_pk_col]
    if not cols_to_insert:
        logger.error(f"No hay columnas para insertar en la dimensión '{table_name}'.")
        return

    insert_cols_str = ", ".join([f'"{c}"' for c in cols_to_insert])
    values_placeholders = ", ".join([f":{c}" for c in cols_to_insert])
    conflict_target_str = ", ".join([f'"{c}"' for c in natural_key_cols])

    sql_upsert = text(f"""
        INSERT INTO {table_name} ({insert_cols_str})
        VALUES ({values_placeholders})
        ON CONFLICT ({conflict_target_str}) DO NOTHING;
    """)
    try:
        with engine.begin() as connection:
            data_to_insert = df_dimension[cols_to_insert].to_dict(orient='records')
            if data_to_insert:
                connection.execute(sql_upsert, data_to_insert)
                logger.info(f"Datos cargados/actualizados en la dimensión '{table_name}'. {len(data_to_insert)} registros procesados para BD.")
            else:
                logger.info(f"No hay datos para insertar en la dimensión '{table_name}' después de la preparación.")
    except SQLAlchemyError as e:
        logger.error(f"Error al cargar datos en la dimensión '{table_name}': {e}", exc_info=True)
        raise
    except Exception as e:
        logger.error(f"Error inesperado al cargar la dimensión '{table_name}': {e}", exc_info=True)
        raise

# --- Función de Carga para Tabla de Hechos (sin cambios en su lógica interna de BD) ---
def load_fact_data(
    engine,
    df_fact_raw: pd.DataFrame,
    fact_table_name: str,
    dimensions_data: dict,
    save_csv: bool = True # Nuevo parámetro
):
    if df_fact_raw.empty:
        logger.info(f"DataFrame para la tabla de hechos '{fact_table_name}' está vacío. No se procesará.")
        return

    # Guardar el DataFrame de hechos *antes* de la resolución de FKs (con claves naturales)
    if save_csv:
        _save_df_to_processed_csv(df_fact_raw, f"{fact_table_name}_raw_natural_keys")


    logger.info(f"Iniciando preparación y carga para la tabla de hechos '{fact_table_name}'.")
    df_fact_final = df_fact_raw.copy()

    # --- Resolución de Claves Foráneas (sin cambios) ---
    # ... (copia la lógica de resolución de FKs de tu versión anterior de load_fact_data)
    # 1. FK_Host
    if 'dim_host' in dimensions_data and not dimensions_data['dim_host'].empty:
        df_dim_host_db = pd.read_sql_table('dim_host', engine, columns=['host_key', 'host_id'])
        df_fact_final = pd.merge(df_fact_final, df_dim_host_db, on='host_id', how='left')
        df_fact_final.drop(columns=['host_id'], inplace=True, errors='ignore')
        df_fact_final.rename(columns={'host_key': 'fk_host'}, inplace=True)
    else:
        df_fact_final['fk_host'] = pd.NA

    # 2. FK_Property_Location
    if 'dim_property_location' in dimensions_data and not dimensions_data['dim_property_location'].empty:
        df_dim_prop_loc_db = pd.read_sql_table('dim_property_location', engine)
        df_fact_final_renamed_for_loc = df_fact_final.rename(columns={
            'neighbourhood_group': 'airbnb_neighbourhood_group',
            'neighbourhood': 'airbnb_neighbourhood',
            'lat': 'airbnb_lat',
            'long': 'airbnb_long'
        })
        df_fact_final = pd.merge(
            df_fact_final_renamed_for_loc,
            df_dim_prop_loc_db[['property_location_key', 'airbnb_neighbourhood_group', 'airbnb_neighbourhood', 'airbnb_lat', 'airbnb_long']],
            on=['airbnb_neighbourhood_group', 'airbnb_neighbourhood', 'airbnb_lat', 'airbnb_long'],
            how='left'
        )
        df_fact_final.drop(columns=['airbnb_neighbourhood_group', 'airbnb_neighbourhood', 'airbnb_lat', 'airbnb_long'], inplace=True, errors='ignore')
        df_fact_final.rename(columns={'property_location_key': 'fk_property_location'}, inplace=True)
    else:
        df_fact_final['fk_property_location'] = pd.NA

    # 3. FK_Property
    if 'dim_property' in dimensions_data and not dimensions_data['dim_property'].empty:
        df_dim_prop_db = pd.read_sql_table('dim_property', engine, columns=['property_key', 'property_id'])
        df_fact_final = pd.merge(df_fact_final, df_dim_prop_db, left_on='id', right_on='property_id', how='left')
        df_fact_final.drop(columns=['id', 'property_id'], inplace=True, errors='ignore')
        df_fact_final.rename(columns={'property_key': 'fk_property'}, inplace=True)
    else:
        df_fact_final['fk_property'] = pd.NA

    # 4. FK_Last_Review_Date
    if 'dim_date' in dimensions_data and not dimensions_data['dim_date'].empty:
        df_dim_date_db = pd.read_sql_table('dim_date', engine, columns=['date_key']) # Solo necesitamos date_key
        df_fact_final = pd.merge(df_fact_final, df_dim_date_db, left_on='last_review', right_on='date_key', how='left')
        df_fact_final.drop(columns=['last_review'], inplace=True, errors='ignore')
        if 'date_key' in df_fact_final.columns:
             df_fact_final.rename(columns={'date_key': 'fk_last_review_date'}, inplace=True)
        else:
             df_fact_final['fk_last_review_date'] = pd.NA
    else:
        df_fact_final['fk_last_review_date'] = pd.NA

    fk_cols = ['fk_host', 'fk_property_location', 'fk_property', 'fk_last_review_date']
    for fk_col in fk_cols:
        if fk_col in df_fact_final.columns and df_fact_final[fk_col].isnull().any():
            logger.warning(f"Valores nulos en FK '{fk_col}'. Nulos: {df_fact_final[fk_col].isnull().sum()}")
    
    final_fact_columns = [
        'fk_property', 'fk_host', 'fk_property_location', 'fk_last_review_date',
        'price', 'service_fee', 'minimum_nights', 'number_of_reviews',
        'reviews_per_month', 'review_rate_number', 'availability_365',
        'nearby_restaurants_count', 'nearby_parks_and_outdoor_count',
        'nearby_cultural_count', 'nearby_bars_and_clubs_count',
        'total_nearby_pois'
    ]
    df_fact_to_load = df_fact_final[[col for col in final_fact_columns if col in df_fact_final.columns]].copy()
    # ... (fin de la lógica de resolución de FKs) ...

    # Guardar el DataFrame de hechos *después* de la resolución de FKs (con claves sustitutas)
    if save_csv:
        _save_df_to_processed_csv(df_fact_to_load, f"{fact_table_name}_with_fks")


    # --- Carga a la Base de Datos (sin cambios) ---
    try:
        with engine.begin() as connection:
            logger.info(f"Truncando la tabla de hechos '{fact_table_name}' antes de la carga a BD.")
            connection.execute(text(f"TRUNCATE TABLE {fact_table_name} RESTART IDENTITY CASCADE;"))
            if not df_fact_to_load.empty:
                df_fact_to_load.to_sql(
                    name=fact_table_name,
                    con=connection,
                    if_exists='append',
                    index=False,
                    method='multi',
                    chunksize=10000
                )
                logger.info(f"Datos cargados exitosamente en la tabla de hechos '{fact_table_name}'. {len(df_fact_to_load)} filas para BD.")
            else:
                logger.info(f"No hay datos para cargar en la tabla de hechos '{fact_table_name}' después de la preparación de FKs.")
    except SQLAlchemyError as e:
        logger.error(f"Error al cargar datos en la tabla de hechos '{fact_table_name}': {e}", exc_info=True)
        raise
    except Exception as e:
        logger.error(f"Error inesperado al cargar la tabla de hechos '{fact_table_name}': {e}", exc_info=True)
        raise

# --- Función Principal del Módulo ---
def populate_dimensional_model(
    prepared_dataframes: dict,
    db_name: str = "airbnb",
    save_csvs: bool = True # Nuevo parámetro para controlar el guardado de todos los CSVs
):
    """
    Orquesta la carga de datos en el modelo dimensional (dimensiones y hechos)
    y guarda cada DataFrame preparado como un archivo CSV.
    """
    if not isinstance(prepared_dataframes, dict):
        logger.error("La entrada 'prepared_dataframes' debe ser un diccionario.")
        raise TypeError("Se esperaba un diccionario para prepared_dataframes.")

    logger.info("Iniciando carga de datos en el modelo dimensional y guardado de CSVs...")
    engine = None
    try:
        engine = get_db_engine(db_name=db_name)

        # Cargar Dimensiones y guardar sus CSVs
        dim_tables_info = {
            "dim_host": {'natural_keys': ['host_id'], 'pk': 'host_key'},
            "dim_property_location": {'natural_keys': ['airbnb_neighbourhood_group', 'airbnb_neighbourhood', 'airbnb_lat', 'airbnb_long'], 'pk': 'property_location_key'},
            "dim_property": {'natural_keys': ['property_id'], 'pk': 'property_key'},
            "dim_date": {'natural_keys': ['date_key'], 'pk': 'date_key'},
            "dim_api_place_category": {'natural_keys': ['category_group_name'], 'pk': 'api_category_key'}
        }

        for table_name, info in dim_tables_info.items():
            if table_name in prepared_dataframes:
                logger.info(f"Procesando dimensión: {table_name}")
                load_dimension_data(
                    engine,
                    prepared_dataframes[table_name],
                    table_name,
                    natural_key_cols=info['natural_keys'],
                    dimension_pk_col=info['pk'],
                    save_csv=save_csvs # Pasar el flag
                )
            else:
                logger.warning(f"No se encontraron datos preparados para la dimensión '{table_name}'.")

        # Cargar Tabla de Hechos y guardar sus CSVs
        fact_table_name = "fact_listing_pois"
        if fact_table_name in prepared_dataframes:
            logger.info(f"Procesando tabla de hechos: {fact_table_name}")
            load_fact_data(
                engine,
                prepared_dataframes[fact_table_name],
                fact_table_name,
                prepared_dataframes, # Pasar todos los DFs de dimensiones para la resolución de FKs
                save_csv=save_csvs # Pasar el flag
            )
        else:
            logger.warning(f"No se encontraron datos preparados para la tabla de hechos '{fact_table_name}'.")
        
        logger.info("Carga de datos en el modelo dimensional y guardado de CSVs completado.")

    except Exception as e:
        logger.error(f"Error en populate_dimensional_model: {e}", exc_info=True)
        raise
    finally:
        if engine:
            engine.dispose()
            logger.info("Engine de base de datos dispuesto.")


if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO,
                        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    logger.info("--- Iniciando prueba local de load_dimensional.py ---")

    # Simular DataFrames preparados (igual que antes)
    dim_host_test_data = pd.DataFrame({'host_id': [10, 20], 'host_name': ['Host Alice', 'Host Bob'], 'host_identity_verified': [True, False], 'calculated_host_listings_count': [2,1]})
    dim_prop_loc_test_data = pd.DataFrame({'airbnb_neighbourhood_group': ['Manhattan', 'Brooklyn'], 'airbnb_neighbourhood': ['Midtown', 'Williamsburg'], 'airbnb_lat': [40.751, 40.710], 'airbnb_long': [-73.980, -73.950]})
    dim_prop_test_data = pd.DataFrame({'property_id': [1001, 1002], 'property_name': ['Cozy Apt A', 'Sunny Studio B'], 'instant_bookable': [True, False], 'cancellation_policy': ['strict', 'moderate'], 'room_type': ['Entire home/apt', 'Private room'], 'construction_year': [2010, 2015], 'house_rules': ['No parties', 'No pets'], 'license': ['LIC123', None]})
    dim_date_test_data = pd.DataFrame({'date_key': [20230115, 20230210], 'full_date': [pd.Timestamp('2023-01-15'), pd.Timestamp('2023-02-10')], 'year': [2023, 2023], 'month': [1, 2], 'day': [15, 10], 'day_of_week': ['Sunday', 'Friday'], 'month_name': ['January', 'February']})
    dim_api_cat_test_data = pd.DataFrame({'category_group_name': ['restaurants', 'parks_and_outdoor', 'cultural']})
    fact_test_data_raw = pd.DataFrame({'id': [1001, 1002], 'host_id': [10, 20], 'lat': [40.751, 40.710], 'long': [-73.980, -73.950], 'neighbourhood_group': ['Manhattan', 'Brooklyn'], 'neighbourhood': ['Midtown', 'Williamsburg'], 'last_review': [20230115, 20230210], 'price': [150.0, 80.0], 'service_fee': [15.0, 8.0], 'minimum_nights': [1,3], 'number_of_reviews': [10,5], 'reviews_per_month': [1.2,0.5], 'review_rate_number': [4,5], 'availability_365': [300,100], 'nearby_restaurants_count': [5, 2], 'nearby_parks_and_outdoor_count': [1, 0], 'nearby_cultural_count': [0,1], 'nearby_bars_and_clubs_count': [3,1], 'total_nearby_pois': [9, 4]})

    test_prepared_dfs = {
        "dim_host": dim_host_test_data,
        "dim_property_location": dim_prop_loc_test_data,
        "dim_property": dim_prop_test_data,
        "dim_date": dim_date_test_data,
        "dim_api_place_category": dim_api_cat_test_data,
        "fact_listing_pois": fact_test_data_raw
    }
    test_db_name_param = "airbnb_test_model"
    logger.info(f"Usando base de datos de prueba: {test_db_name_param}")

    try:
        logger.info("Iniciando carga de datos dimensionales de prueba y guardado de CSVs...")
        populate_dimensional_model(test_prepared_dfs, db_name=test_db_name_param, save_csvs=True)
        logger.info(f"Prueba de carga dimensional y guardado de CSVs completada.")
        logger.info(f"Verifica los datos en las tablas de '{test_db_name_param}' y los archivos CSV en '{PROCESSED_DATA_PATH}'.")
        for table_name in test_prepared_dfs.keys():
            if table_name == "fact_listing_pois":
                logger.info(f"  - Archivo esperado: {os.path.join(PROCESSED_DATA_PATH, table_name + '_raw_natural_keys.csv')}")
                logger.info(f"  - Archivo esperado: {os.path.join(PROCESSED_DATA_PATH, table_name + '_with_fks.csv')}")
            else:
                logger.info(f"  - Archivo esperado: {os.path.join(PROCESSED_DATA_PATH, table_name + '.csv')}")


    except Exception as e:
        logger.error(f"Error durante la prueba local de load_dimensional.py: {e}", exc_info=True)
    finally:
        logger.info("--- Prueba local de load_dimensional.py finalizada ---")