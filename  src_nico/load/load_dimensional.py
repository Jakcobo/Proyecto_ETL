# /home/nicolas/Escritorio/proyecto ETL/develop/src/load/load_dimensional.py

import pandas as pd
import logging # Usaremos logging directamente
from sqlalchemy import text
from sqlalchemy.exc import SQLAlchemyError
import re
import sys
import os

# Configuración básica de logging (si no se configura en otro lugar, como en el DAG)
# Esto es importante si el script se ejecuta solo. Airflow manejará su propia config.
if not logging.getLogger().hasHandlers(): # Evitar añadir handlers múltiples si ya está configurado
    logging.basicConfig(
        level=logging.INFO, # Puedes cambiar a DEBUG para más detalle
        format='%(asctime)s - [%(levelname)s] - %(module)s.%(funcName)s:%(lineno)d - %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )

# --- Configuración de Paths ---
PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..'))
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
    logging.error("Error importando get_db_engine desde database.db. Asegúrate que 'src' esté en PYTHONPATH o la estructura del proyecto sea correcta.")
    sys.exit("Fallo crítico: no se pudo importar get_db_engine.")


PROCESSED_DATA_PATH = os.path.join(PROJECT_ROOT, "data", "processed")

# --- Funciones Helper ---

def _ensure_processed_dir_exists():
    if not os.path.exists(PROCESSED_DATA_PATH):
        try:
            os.makedirs(PROCESSED_DATA_PATH)
            logging.info(f"Directorio creado: {PROCESSED_DATA_PATH}")
        except OSError as e:
            logging.error(f"Error al crear el directorio {PROCESSED_DATA_PATH}: {e}")
            raise

def _df_to_str_sample(df: pd.DataFrame, n: int = 5, name: str = "") -> str:
    if df is None:
        return f"DataFrame '{name}' es None."
    if df.empty:
        return f"DataFrame '{name}' está vacío. Shape: {df.shape}, Columnas: {df.columns.tolist()}"
    
    truncated_cols = [col[:30] + '...' if len(col) > 30 else col for col in df.columns.tolist()]
    header = f"DataFrame '{name}' - Shape: {df.shape}, Columnas (truncadas si >30char): {truncated_cols}"
    
    sample_df = df.head(n)
    for col_name in sample_df.columns:
        if sample_df[col_name].dtype == 'object':
            original_series = df[col_name].head(n).astype(str)
            sample_df[col_name] = original_series.apply(lambda x: x[:47] + '...' if len(x) > 50 else x)
            
    sample = sample_df.to_string()
    return f"{header}\nPrimeras {n} filas (valores string truncados a ~50char):\n{sample}"

def _save_df_to_processed_csv(df: pd.DataFrame, table_name_as_filename_base: str):
    if df is None or df.empty:
        logging.warning(f"DataFrame para '{table_name_as_filename_base}' está vacío o es None. No se guardará el CSV.")
        return
    _ensure_processed_dir_exists()
    file_name = f"{table_name_as_filename_base}.csv"
    file_path = os.path.join(PROCESSED_DATA_PATH, file_name)
    try:
        df.to_csv(file_path, index=False, encoding='utf-8')
        logging.info(f"DataFrame para '{table_name_as_filename_base}' guardado exitosamente como CSV en: {file_path}")
    except Exception as e:
        logging.error(f"Error al guardar DataFrame para '{table_name_as_filename_base}' como CSV en '{file_path}'.", exc_info=True)

def _log_sqlalchemy_error_concise(e: SQLAlchemyError, context_message: str, df_sample_data: pd.DataFrame = None, sample_name: str = ""):
    """Loguea errores de SQLAlchemy de forma más concisa, enfocándose en el mensaje y DETAIL."""
    error_message = str(e.orig) if hasattr(e, 'orig') and e.orig is not None else str(e) # Usar e.orig para el error del driver DBAPI
    
    # La primera línea del error del driver suele ser la más útil
    first_line = error_message.split('\n')[0]
    
    # Intentar extraer el DETAIL de PostgreSQL
    detail_info = ""
    if isinstance(e.orig, Exception): # psycopg2.Error hereda de Exception
        pg_detail_match = re.search(r"DETAIL:\s*(.*)", str(e.orig), re.IGNORECASE)
        if pg_detail_match:
            detail_info = f"DETAIL: {pg_detail_match.group(1).strip()}"

    logging.error(f"{context_message}: {first_line}")
    if detail_info:
        logging.error(detail_info)
    
    if df_sample_data is not None and not df_sample_data.empty:
        logging.error(f"Muestra de datos ({sample_name}) que se intentó procesar:\n{_df_to_str_sample(df_sample_data.head(), n=5, name=sample_name)}")
    
    # El traceback completo se puede obtener si el nivel de logging del handler es DEBUG y se usa exc_info=True
    # Aquí solo logueamos el mensaje principal. El DAG runner mostrará el traceback.
    logging.debug("Traceback completo del error de SQLAlchemy:", exc_info=True)


# --- Funciones de Carga ---

def load_dimension_data(
    engine,
    df_dimension: pd.DataFrame,
    table_name: str,
    natural_key_cols: list,
    dimension_pk_col: str,
    save_csv: bool = True
):
    logging.info(f"Iniciando carga para dimensión '{table_name}'.")
    logging.debug(_df_to_str_sample(df_dimension, name=f"Entrada para {table_name}"))

    if df_dimension.empty:
        logging.info(f"DataFrame para la dimensión '{table_name}' está vacío. No se procesará.")
        return

    if save_csv:
        _save_df_to_processed_csv(df_dimension, table_name)

    cols_to_insert = [col for col in df_dimension.columns if col != dimension_pk_col]
    if not cols_to_insert:
        logging.error(f"No hay columnas para insertar en la dimensión '{table_name}'. Columnas disponibles: {df_dimension.columns.tolist()}")
        return

    logging.debug(f"Columnas a insertar en '{table_name}': {cols_to_insert}")
    logging.debug(f"Claves naturales para conflicto en '{table_name}': {natural_key_cols}")

    insert_cols_str = ", ".join([f'"{c}"' for c in cols_to_insert])
    values_placeholders = ", ".join([f":{c}" for c in cols_to_insert])
    conflict_target_str = ", ".join([f'"{c}"' for c in natural_key_cols])

    sql_upsert_query = f"""
        INSERT INTO "{table_name}" ({insert_cols_str})
        VALUES ({values_placeholders})
        ON CONFLICT ({conflict_target_str}) DO NOTHING;
    """
    logging.debug(f"SQL Upsert para '{table_name}': {sql_upsert_query[:500]}...")

    data_to_insert = []
    try:
        with engine.begin() as connection:
            df_to_insert_actual = df_dimension[cols_to_insert]
            logging.debug(_df_to_str_sample(df_to_insert_actual, name=f"Datos listos para insertar en {table_name}"))
            data_to_insert = df_to_insert_actual.to_dict(orient='records')

            if data_to_insert:
                logging.info(f"Intentando insertar/actualizar {len(data_to_insert)} registros en '{table_name}'.")
                connection.execute(text(sql_upsert_query), data_to_insert)
                logging.info(f"Datos cargados/actualizados en la dimensión '{table_name}'. {len(data_to_insert)} registros procesados para BD.")
            else:
                logging.info(f"No hay datos para insertar en la dimensión '{table_name}' después de la preparación.")
    except SQLAlchemyError as e:
        sample_for_error = pd.DataFrame(data_to_insert[:5]) if data_to_insert else pd.DataFrame()
        _log_sqlalchemy_error_concise(e, f"Error de SQLAlchemy al cargar datos en la dimensión '{table_name}'",
                                      df_sample_data=sample_for_error, sample_name=f"Primeros_registros_{table_name}")
        raise
    except Exception as e:
        logging.error(f"Error inesperado al cargar la dimensión '{table_name}'.", exc_info=True)
        raise

def load_fact_data(
    engine,
    df_fact_raw: pd.DataFrame,
    fact_table_name: str,
    dimensions_data: dict, # Diccionario de DataFrames de dimensiones ya preparados
    save_csv: bool = True
):
    logging.info(f"Iniciando preparación y carga para la tabla de hechos '{fact_table_name}'.")
    logging.debug(_df_to_str_sample(df_fact_raw, name=f"Entrada cruda para {fact_table_name}"))

    if df_fact_raw.empty:
        logging.info(f"DataFrame para la tabla de hechos '{fact_table_name}' está vacío. No se procesará.")
        return

    if save_csv:
        _save_df_to_processed_csv(df_fact_raw, f"{fact_table_name}_raw_natural_keys")

    df_fact_resolved_fks = df_fact_raw.copy()
    logging.info(f"Iniciando resolución de claves foráneas para {fact_table_name}. Shape inicial: {df_fact_resolved_fks.shape}")

    # --- Resolución de FKs ---
    # 1. FK_Host
    if 'dim_host' in dimensions_data and not dimensions_data['dim_host'].empty:
        logging.debug("Resolviendo FK_Host...")
        # Usar el DataFrame de dimensión ya cargado en la BD para obtener las PKs
        df_dim_host_db = pd.read_sql_table('dim_host', engine, columns=['host_key', 'host_id'])
        logging.debug(_df_to_str_sample(df_dim_host_db, n=2, name="dim_host_db (leída para FK)"))
        df_fact_resolved_fks = pd.merge(df_fact_resolved_fks, df_dim_host_db, on='host_id', how='left')
        df_fact_resolved_fks.drop(columns=['host_id'], inplace=True, errors='ignore')
        df_fact_resolved_fks.rename(columns={'host_key': 'fk_host'}, inplace=True)
        logging.debug(f"Shape después de merge FK_Host: {df_fact_resolved_fks.shape}")
    else:
        logging.warning("No hay datos en dimensions_data['dim_host'] o no existe. Asignando pd.NA a fk_host.")
        df_fact_resolved_fks['fk_host'] = pd.NA

    # 2. FK_Property_Location
    if 'dim_property_location' in dimensions_data and not dimensions_data['dim_property_location'].empty:
        logging.debug("Resolviendo FK_Property_Location...")
        df_dim_prop_loc_db = pd.read_sql_table('dim_property_location', engine)
        logging.debug(_df_to_str_sample(df_dim_prop_loc_db, n=2, name="dim_property_location_db (leída para FK)"))
        rename_map_loc = {'neighbourhood_group': 'airbnb_neighbourhood_group', 'neighbourhood': 'airbnb_neighbourhood', 'lat': 'airbnb_lat', 'long': 'airbnb_long'}
        actual_rename_map_loc = {k: v for k, v in rename_map_loc.items() if k in df_fact_resolved_fks.columns}
        df_fact_renamed_for_loc = df_fact_resolved_fks.rename(columns=actual_rename_map_loc)
        merge_on_cols_loc = ['airbnb_neighbourhood_group', 'airbnb_neighbourhood', 'airbnb_lat', 'airbnb_long']
        missing_cols_fact = [col for col in merge_on_cols_loc if col not in df_fact_renamed_for_loc.columns]
        missing_cols_dim = [col for col in merge_on_cols_loc if col not in df_dim_prop_loc_db.columns]

        if not missing_cols_fact and not missing_cols_dim:
            df_fact_resolved_fks = pd.merge(df_fact_renamed_for_loc, df_dim_prop_loc_db[['property_location_key'] + merge_on_cols_loc], on=merge_on_cols_loc, how='left')
            df_fact_resolved_fks.drop(columns=merge_on_cols_loc, inplace=True, errors='ignore') # Eliminar las columnas usadas para el join que vinieron de la dim
            df_fact_resolved_fks.rename(columns={'property_location_key': 'fk_property_location'}, inplace=True)
        else:
            logging.error(f"Merge para FK_Property_Location no realizado. Faltan cols en fact: {missing_cols_fact}, en dim: {missing_cols_dim}. Asignando pd.NA.")
            df_fact_resolved_fks['fk_property_location'] = pd.NA
        logging.debug(f"Shape después de merge FK_Property_Location: {df_fact_resolved_fks.shape}")
    else:
        logging.warning("No hay datos en dimensions_data['dim_property_location']. Asignando pd.NA a fk_property_location.")
        df_fact_resolved_fks['fk_property_location'] = pd.NA

    # 3. FK_Property
    if 'dim_property' in dimensions_data and not dimensions_data['dim_property'].empty:
        logging.debug("Resolviendo FK_Property...")
        df_dim_prop_db = pd.read_sql_table('dim_property', engine, columns=['property_key', 'property_id'])
        logging.debug(_df_to_str_sample(df_dim_prop_db, n=2, name="dim_property_db (leída para FK)"))
        df_fact_resolved_fks = pd.merge(df_fact_resolved_fks, df_dim_prop_db, left_on='id', right_on='property_id', how='left')
        df_fact_resolved_fks.drop(columns=['id', 'property_id'], inplace=True, errors='ignore')
        df_fact_resolved_fks.rename(columns={'property_key': 'fk_property'}, inplace=True)
        logging.debug(f"Shape después de merge FK_Property: {df_fact_resolved_fks.shape}")
    else:
        logging.warning("No hay datos en dimensions_data['dim_property']. Asignando pd.NA a fk_property.")
        df_fact_resolved_fks['fk_property'] = pd.NA

    # 4. FK_Last_Review_Date
    if 'dim_date' in dimensions_data and not dimensions_data['dim_date'].empty:
        logging.debug("Resolviendo FK_Last_Review_Date...")
        df_dim_date_db = pd.read_sql_table('dim_date', engine, columns=['date_key']) # Solo necesitamos date_key
        logging.debug(_df_to_str_sample(df_dim_date_db, n=2, name="dim_date_db (leída para FK)"))

        if 'last_review' in df_fact_resolved_fks.columns:
            logging.debug(f"Valores únicos de df_fact_resolved_fks['last_review'] (antes del merge FK_Date, muestra):\n{df_fact_resolved_fks['last_review'].dropna().unique()[:5]}")
            logging.debug(f"Tipo de df_fact_resolved_fks['last_review']: {df_fact_resolved_fks['last_review'].dtype}")
            logging.debug(f"Valores únicos de df_dim_date_db['date_key'] (leída de BD, muestra):\n{df_dim_date_db['date_key'].dropna().unique()[:5]}")
            logging.debug(f"Tipo de df_dim_date_db['date_key']: {df_dim_date_db['date_key'].dtype}")
            try:
                # Asegurar que ambos sean del mismo tipo numérico para el merge
                s_last_review_fact = pd.to_numeric(df_fact_resolved_fks['last_review'], errors='coerce').astype('Int64')
                s_date_key_dim = pd.to_numeric(df_dim_date_db['date_key'], errors='coerce').astype('Int64')
                
                logging.debug(f"Tipos para merge FK_Date: fact_last_review ({s_last_review_fact.dtype}), dim_date_key ({s_date_key_dim.dtype})")
                
                # Crear DataFrames temporales para el merge con nombres de columna explícitos
                df_fact_temp = pd.DataFrame({'last_review_for_merge': s_last_review_fact, 'original_index_fact': df_fact_resolved_fks.index})
                df_dim_temp = pd.DataFrame({'date_key_for_merge': s_date_key_dim})
                
                merged_dates = pd.merge(df_fact_temp, df_dim_temp, left_on='last_review_for_merge', right_on='date_key_for_merge', how='left')
                merged_dates.set_index('original_index_fact', inplace=True) # Alinear con df_fact_resolved_fks
                
                df_fact_resolved_fks['fk_last_review_date'] = merged_dates['date_key_for_merge'] # Asignar la FK resuelta
                
            except Exception as e_cast:
                logging.error(f"Error al castear o procesar columnas para merge de FK_Last_Review_Date: {e_cast}. Asignando pd.NA a fk_last_review_date.")
                df_fact_resolved_fks['fk_last_review_date'] = pd.NA
            
            df_fact_resolved_fks.drop(columns=['last_review'], inplace=True, errors='ignore')
        else:
            logging.error("Columna 'last_review' no encontrada en df_fact_resolved_fks antes del merge para FK_Last_Review_Date.")
            df_fact_resolved_fks['fk_last_review_date'] = pd.NA
        logging.debug(f"Shape después de merge FK_Last_Review_Date: {df_fact_resolved_fks.shape}")
    else:
        logging.warning("No hay datos en dimensions_data['dim_date']. Asignando pd.NA a fk_last_review_date.")
        df_fact_resolved_fks['fk_last_review_date'] = pd.NA

    logging.info("Resolución de claves foráneas completada.")
    fk_cols_check = ['fk_host', 'fk_property_location', 'fk_property', 'fk_last_review_date']
    for fk_col_name in fk_cols_check:
        if fk_col_name in df_fact_resolved_fks.columns:
            null_count = df_fact_resolved_fks[fk_col_name].isnull().sum()
            total_rows = len(df_fact_resolved_fks)
            if null_count > 0:
                logging.warning(f"Valores nulos encontrados en FK '{fk_col_name}': {null_count} de {total_rows} filas.")
            else:
                logging.debug(f"No se encontraron valores nulos en FK '{fk_col_name}'.")
        else:
            logging.warning(f"Columna FK '{fk_col_name}' no encontrada en df_fact_resolved_fks después de los merges.")
    
    final_fact_columns_ordered = [
        'fk_property', 'fk_host', 'fk_property_location', 'fk_last_review_date',
        'price', 'service_fee', 'minimum_nights', 'number_of_reviews',
        'reviews_per_month', 'review_rate_number', 'availability_365',
        'nearby_restaurants_count', 'nearby_parks_and_outdoor_count',
        'nearby_cultural_count', 'nearby_bars_and_clubs_count',
        'total_nearby_pois'
    ]
    
    df_fact_to_load_final = df_fact_resolved_fks[[col for col in final_fact_columns_ordered if col in df_fact_resolved_fks.columns]].copy()
    logging.info(f"Columnas finales seleccionadas para cargar en '{fact_table_name}': {df_fact_to_load_final.columns.tolist()}")
    logging.debug(_df_to_str_sample(df_fact_to_load_final, name=f"Datos listos para cargar en {fact_table_name}"))

    if save_csv:
        _save_df_to_processed_csv(df_fact_to_load_final, f"{fact_table_name}_with_fks")

    try:
        with engine.begin() as connection:
            logging.info(f"Truncando la tabla de hechos '{fact_table_name}' antes de la carga a BD.")
            connection.execute(text(f'TRUNCATE TABLE "{fact_table_name}" RESTART IDENTITY CASCADE;'))
            
            if not df_fact_to_load_final.empty:
                logging.info(f"Intentando cargar {len(df_fact_to_load_final)} filas en '{fact_table_name}'.")
                df_fact_to_load_final.to_sql(
                    name=fact_table_name,
                    con=connection,
                    if_exists='append',
                    index=False,
                    method='multi',
                    chunksize=10000
                )
                logging.info(f"Datos cargados exitosamente en la tabla de hechos '{fact_table_name}'. {len(df_fact_to_load_final)} filas para BD.")
            else:
                logging.info(f"No hay datos para cargar en la tabla de hechos '{fact_table_name}' después de la preparación de FKs.")
    except SQLAlchemyError as e:
        _log_sqlalchemy_error_concise(e, f"Error de SQLAlchemy al cargar datos en la tabla de hechos '{fact_table_name}'",
                                      df_sample_data=df_fact_to_load_final, sample_name="ErrorSampleFact")
        raise
    except Exception as e:
        logging.error(f"Error inesperado al cargar la tabla de hechos '{fact_table_name}'.", exc_info=True)
        raise

# --- Función Principal del Módulo ---
def populate_dimensional_model(
    prepared_dataframes: dict,
    db_name: str = "airbnb",
    save_csvs: bool = True
):
    if not isinstance(prepared_dataframes, dict):
        logging.error("La entrada 'prepared_dataframes' debe ser un diccionario.")
        raise TypeError("Se esperaba un diccionario para prepared_dataframes.")

    logging.info("Iniciando carga de datos en el modelo dimensional y guardado de CSVs...")
    logging.debug(f"Diccionario 'prepared_dataframes' recibido. Claves: {list(prepared_dataframes.keys())}")
    for name, df_sample in prepared_dataframes.items():
        logging.debug(_df_to_str_sample(df_sample, n=2, name=f"Muestra de prepared_dataframes['{name}']"))

    engine = None
    try:
        engine = get_db_engine(db_name=db_name)
        logging.info(f"Engine para BD '{db_name}' obtenido para poblar modelo dimensional.")

        dim_tables_info = {
            "dim_host": {'natural_keys': ['host_id'], 'pk': 'host_key'},
            "dim_property_location": {'natural_keys': ['airbnb_neighbourhood_group', 'airbnb_neighbourhood', 'airbnb_lat', 'airbnb_long'], 'pk': 'property_location_key'},
            "dim_property": {'natural_keys': ['property_id'], 'pk': 'property_key'},
            "dim_date": {'natural_keys': ['date_key'], 'pk': 'date_key'},
            "dim_api_place_category": {'natural_keys': ['category_group_name'], 'pk': 'api_category_key'}
        }

        for table_name, info in dim_tables_info.items():
            if table_name in prepared_dataframes:
                df_dim = prepared_dataframes[table_name]
                if not isinstance(df_dim, pd.DataFrame):
                    logging.error(f"Elemento para '{table_name}' en prepared_dataframes no es un DataFrame. Tipo: {type(df_dim)}. Saltando carga.")
                    continue
                logging.info(f"Procesando dimensión: {table_name} (Shape: {df_dim.shape if df_dim is not None else 'None'})")
                load_dimension_data(engine, df_dim, table_name, info['natural_keys'], info['pk'], save_csvs)
            else:
                logging.warning(f"No se encontraron datos preparados para la dimensión '{table_name}'.")

        fact_table_name = "fact_listing_pois"
        if fact_table_name in prepared_dataframes:
            df_fact = prepared_dataframes[fact_table_name]
            if not isinstance(df_fact, pd.DataFrame):
                 logging.error(f"Elemento para '{fact_table_name}' en prepared_dataframes no es un DataFrame. Tipo: {type(df_fact)}. Saltando carga.")
            else:
                logging.info(f"Procesando tabla de hechos: {fact_table_name} (Shape: {df_fact.shape if df_fact is not None else 'None'})")
                load_fact_data(engine, df_fact, fact_table_name, prepared_dataframes, save_csvs)
        else:
            logging.warning(f"No se encontraron datos preparados para la tabla de hechos '{fact_table_name}'.")
        
        logging.info("Carga de datos en el modelo dimensional y guardado de CSVs completado.")

    except Exception as e:
        # Para la excepción general en populate_dimensional_model, podemos ser más directos
        logging.error(f"Error en populate_dimensional_model: {type(e).__name__} - {str(e)[:200]}...", exc_info=True)
        raise
    finally:
        if engine:
            engine.dispose()
            logging.info("Engine de base de datos (populate_dimensional_model) dispuesto.")


if __name__ == '__main__':
    # La configuración de logging ya está al inicio del script
    logging.info("--- Iniciando prueba local de load_dimensional.py ---")

    # ... (el resto del bloque if __name__ == '__main__' se mantiene igual que en la respuesta anterior) ...
    # ... (asegúrate de que los datos de prueba sean consistentes con la lógica de transformación) ...

    dim_host_test_data = pd.DataFrame({'host_id': [10, 20], 'host_name': ['Host Alice', 'Host Bob'], 'host_identity_verified': [True, False], 'calculated_host_listings_count': [2,1]})
    dim_prop_loc_test_data = pd.DataFrame({'airbnb_neighbourhood_group': ['Manhattan', 'Brooklyn'], 'airbnb_neighbourhood': ['Midtown', 'Williamsburg'], 'airbnb_lat': [40.751, 40.710], 'airbnb_long': [-73.980, -73.950]})
    dim_prop_test_data = pd.DataFrame({'property_id': [1001, 1002], 'property_name': ['Cozy Apt A', 'Sunny Studio B'], 'instant_bookable': [True, False], 'cancellation_policy': ['strict', 'moderate'], 'room_type': ['Entire home/apt', 'Private room'], 'construction_year': [2010, 2015], 'house_rules': ['No parties', 'No pets'], 'license': ['LIC123', None]})
    # Asegurar que 19000101 esté en dim_date para las pruebas
    dim_date_test_data = pd.DataFrame({'date_key': [20230115, 20230210, 19000101], 
                                     'full_date': [pd.Timestamp('2023-01-15'), pd.Timestamp('2023-02-10'), pd.Timestamp('1900-01-01')], 
                                     'year': [2023, 2023, 1900], 'month': [1, 2, 1], 'day': [15, 10, 1], 
                                     'day_of_week': ['Sunday', 'Friday', 'Monday'], 'month_name': ['January', 'February', 'January']})
    dim_api_cat_test_data = pd.DataFrame({'category_group_name': ['restaurants', 'parks_and_outdoor', 'cultural']})
    
    fact_test_data_raw = pd.DataFrame({
        'id': [1001, 1002, 1003, 1004], 
        'host_id': [10, 20, 10, 30], # El host_id 30 no está en dim_host_test_data, resultará en FK nula
        'lat': [40.751, 40.710, 40.751, 40.800], 
        'long': [-73.980, -73.950, -73.980, -73.900], 
        'neighbourhood_group': ['Manhattan', 'Brooklyn', 'Manhattan', 'Bronx'], # 'Bronx' no está en dim_prop_loc
        'neighbourhood': ['Midtown', 'Williamsburg', 'Midtown', 'Fordham'], 
        'last_review': [20230115, 20230210, 19000101, 20240101], # 20240101 no está en dim_date_test_data
        'price': [150.0, 80.0, 160.0, 120.0], 
        'service_fee': [15.0, 8.0, 16.0, 12.0], 
        'minimum_nights': [1,3,1,2], 'number_of_reviews': [10,5,12,0], 
        'reviews_per_month': [1.2,0.5,1.3,0.0], 'review_rate_number': [4,5,4,0], 
        'availability_365': [300,100,290,365], 
        'nearby_restaurants_count': [5, 2, 5, 1], 'nearby_parks_and_outdoor_count': [1, 0, 1, 2], 
        'nearby_cultural_count': [0,1,0,0], 'nearby_bars_and_clubs_count': [3,1,3,0], 
        'total_nearby_pois': [9, 4, 9, 3]
    })
    # No es necesario el fillna aquí si la lógica de transformación de last_review ya lo hace.
    # fact_test_data_raw['last_review'] = fact_test_data_raw['last_review'].fillna(19000101).astype(int)


    test_prepared_dfs = {
        "dim_host": dim_host_test_data,
        "dim_property_location": dim_prop_loc_test_data,
        "dim_property": dim_prop_test_data,
        "dim_date": dim_date_test_data,
        "dim_api_place_category": dim_api_cat_test_data,
        "fact_listing_pois": fact_test_data_raw
    }
    test_db_name_param = "airbnb_test_model"
    logging.info(f"Usando base de datos de prueba: {test_db_name_param}")

    try:
        logging.info("Iniciando carga de datos dimensionales de prueba y guardado de CSVs...")
        temp_engine = get_db_engine(db_name=test_db_name_param)
        # Asumimos que model_dimensional.py ya creó las tablas
        # from database.model_dimensional import create_dimensional_tables_if_not_exist 
        # create_dimensional_tables_if_not_exist(temp_engine) # Descomentar si es necesario
        
        # Poblar dimensiones de prueba
        for dim_name_key, dim_df_val in test_prepared_dfs.items():
            if dim_name_key.startswith("dim_"):
                # ... (código para obtener pk_col_val y nk_cols_val como antes)
                pk_col_val = {"dim_host": "host_key", "dim_property_location": "property_location_key", 
                              "dim_property": "property_key", "dim_date": "date_key", 
                              "dim_api_place_category": "api_category_key"}[dim_name_key]
                nk_cols_val = {"dim_host": ["host_id"], 
                               "dim_property_location": ["airbnb_neighbourhood_group", "airbnb_neighbourhood", "airbnb_lat", "airbnb_long"],
                               "dim_property": ["property_id"], 
                               "dim_date": ["date_key"],
                               "dim_api_place_category": ["category_group_name"]}[dim_name_key]

                with temp_engine.connect() as conn_val:
                    conn_val.execute(text(f'TRUNCATE TABLE "{dim_name_key}" RESTART IDENTITY CASCADE;'))
                    conn_val.commit()
                load_dimension_data(temp_engine, dim_df_val, dim_name_key, nk_cols_val, pk_col_val, save_csv=False)
        
        temp_engine.dispose()
        logging.info("Tablas dimensionales de prueba pobladas.")

        populate_dimensional_model(test_prepared_dfs, db_name=test_db_name_param, save_csvs=True)
        
        logging.info(f"Prueba de carga dimensional y guardado de CSVs completada.")
        # ... (resto del logging de archivos esperados) ...

    except Exception as e:
        logging.error(f"Error durante la prueba local de load_dimensional.py.", exc_info=True) # exc_info=True para el traceback
    finally:
        logging.info("--- Prueba local de load_dimensional.py finalizada ---")