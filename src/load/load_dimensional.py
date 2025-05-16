# /home/nicolas/Escritorio/proyecto ETL/develop/src/load/load_dimensional.py

import pandas as pd
import logging
from sqlalchemy import text
from sqlalchemy.exc import SQLAlchemyError

# Asegurarse de que src esté en el path para importar db
import sys
import os
SRC_PATH = os.path.abspath(os.path.join(os.path.dirname(__file__), '..')) # .. porque estamos en src/load/
if SRC_PATH not in sys.path:
    sys.path.append(SRC_PATH)
DB_MODULE_PATH = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', 'database')) # para importar src.database.db
if DB_MODULE_PATH not in sys.path:
     sys.path.insert(0, DB_MODULE_PATH)


try:
    from database.db import get_db_engine
except ImportError:
    logger_fallback = logging.getLogger(__name__ + "_fallback")
    logger_fallback.error("Error importando get_db_engine. Asegúrate que 'src' esté en PYTHONPATH.")
    # from ..database.db import get_db_engine # Alternativa


logger = logging.getLogger(__name__)

# --- Funciones de Carga para Dimensiones ---

def load_dimension_data(
    engine,
    df_dimension: pd.DataFrame,
    table_name: str,
    natural_key_cols: list, # Columnas que forman la clave natural de la dimensión
    dimension_pk_col: str # Nombre de la columna de la clave primaria sustituta (ej. 'host_key')
):
    """
    Carga datos en una tabla de dimensión usando una estrategia de UPSERT (INSERT ON CONFLICT DO NOTHING).
    Asume que la PK de la dimensión es autoincremental.

    Args:
        engine: SQLAlchemy engine.
        df_dimension (pd.DataFrame): DataFrame con los datos de la dimensión.
        table_name (str): Nombre de la tabla de dimensión.
        natural_key_cols (list): Lista de nombres de columnas que componen la clave natural.
        dimension_pk_col (str): Nombre de la columna PK sustituta (no se inserta directamente, es para info).
    """
    if df_dimension.empty:
        logger.info(f"DataFrame para la dimensión '{table_name}' está vacío. No se cargará nada.")
        return

    # Columnas a insertar (todas excepto la PK sustituta si estuviera presente por error)
    cols_to_insert = [col for col in df_dimension.columns if col != dimension_pk_col]
    if not cols_to_insert:
        logger.error(f"No hay columnas para insertar en la dimensión '{table_name}'.")
        return

    insert_cols_str = ", ".join([f'"{c}"' for c in cols_to_insert]) # Encomillar por si tienen espacios/mayus
    values_placeholders = ", ".join([f":{c}" for c in cols_to_insert])
    conflict_target_str = ", ".join([f'"{c}"' for c in natural_key_cols])

    # Construir la query de UPSERT
    # Para PostgreSQL: INSERT ... ON CONFLICT (columnas_clave_natural) DO NOTHING
    # Si las columnas en df_dimension ya son snake_case, no necesitaríamos las comillas dobles, pero es más seguro.
    sql_upsert = text(f"""
        INSERT INTO {table_name} ({insert_cols_str})
        VALUES ({values_placeholders})
        ON CONFLICT ({conflict_target_str}) DO NOTHING;
    """)

    try:
        with engine.begin() as connection: # Inicia una transacción
            # Convertir DataFrame a lista de diccionarios para la ejecución en batch
            data_to_insert = df_dimension[cols_to_insert].to_dict(orient='records')
            if data_to_insert: # Solo ejecutar si hay datos
                connection.execute(sql_upsert, data_to_insert)
                logger.info(f"Datos cargados/actualizados en la dimensión '{table_name}'. {len(data_to_insert)} registros procesados.")
            else:
                logger.info(f"No hay datos para insertar en la dimensión '{table_name}' después de la preparación.")
    except SQLAlchemyError as e:
        logger.error(f"Error al cargar datos en la dimensión '{table_name}': {e}", exc_info=True)
        raise
    except Exception as e:
        logger.error(f"Error inesperado al cargar la dimensión '{table_name}': {e}", exc_info=True)
        raise

# --- Función de Carga para Tabla de Hechos ---

def load_fact_data(
    engine,
    df_fact_raw: pd.DataFrame, # DataFrame de hechos con claves naturales
    fact_table_name: str,
    dimensions_data: dict # Diccionario con DataFrames de dimensiones ya cargadas (o para leerlas)
                          # {'dim_host': df_dim_host, 'dim_property': df_dim_property ...}
):
    """
    Prepara (resuelve FKs) y carga datos en la tabla de hechos.
    La tabla de hechos se trunca antes de la carga.
    """
    if df_fact_raw.empty:
        logger.info(f"DataFrame para la tabla de hechos '{fact_table_name}' está vacío. No se cargará nada.")
        return

    logger.info(f"Iniciando preparación y carga para la tabla de hechos '{fact_table_name}'.")
    df_fact_final = df_fact_raw.copy()

    # --- Resolución de Claves Foráneas ---
    # Necesitamos leer las dimensiones de la BD para obtener las claves sustitutas (PKs)
    # y hacer merge con el df_fact_final usando las claves naturales.

    # 1. FK_Host
    if 'dim_host' in dimensions_data and not dimensions_data['dim_host'].empty:
        # Leer dim_host de la BD para asegurar que tenemos las PKs actualizadas
        df_dim_host_db = pd.read_sql_table('dim_host', engine, columns=['host_key', 'host_id'])
        df_fact_final = pd.merge(df_fact_final, df_dim_host_db, on='host_id', how='left')
        df_fact_final.drop(columns=['host_id'], inplace=True, errors='ignore') # Eliminar clave natural
        df_fact_final.rename(columns={'host_key': 'fk_host'}, inplace=True)
    else:
        logger.warning("No se proporcionaron datos para dim_host o está vacía. fk_host no se resolverá o será NaN.")
        df_fact_final['fk_host'] = pd.NA


    # 2. FK_Property_Location
    if 'dim_property_location' in dimensions_data and not dimensions_data['dim_property_location'].empty:
        # Claves naturales para dim_property_location: 'airbnb_neighbourhood_group', 'airbnb_neighbourhood', 'airbnb_lat', 'airbnb_long'
        # El df_fact_final debe tener estas columnas (con los nombres originales de Airbnb: 'neighbourhood_group', 'neighbourhood', 'lat', 'long')
        df_dim_prop_loc_db = pd.read_sql_table('dim_property_location', engine) # Cargar todas las columnas
        # Renombrar en df_fact_final para el merge
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
        logger.warning("No se proporcionaron datos para dim_property_location. fk_property_location no se resolverá.")
        df_fact_final['fk_property_location'] = pd.NA

    # 3. FK_Property
    if 'dim_property' in dimensions_data and not dimensions_data['dim_property'].empty:
        df_dim_prop_db = pd.read_sql_table('dim_property', engine, columns=['property_key', 'property_id'])
        df_fact_final = pd.merge(df_fact_final, df_dim_prop_db, left_on='id', right_on='property_id', how='left') # 'id' de airbnb es property_id
        df_fact_final.drop(columns=['id', 'property_id'], inplace=True, errors='ignore')
        df_fact_final.rename(columns={'property_key': 'fk_property'}, inplace=True)
    else:
        logger.warning("No se proporcionaron datos para dim_property. fk_property no se resolverá.")
        df_fact_final['fk_property'] = pd.NA

    # 4. FK_Last_Review_Date
    if 'dim_date' in dimensions_data and not dimensions_data['dim_date'].empty:
        df_dim_date_db = pd.read_sql_table('dim_date', engine, columns=['date_key', 'date_key']) # 'date_key' es la PK y la NK
        # El df_fact_final tiene 'last_review' que es el YYYYMMDD (date_key)
        df_fact_final = pd.merge(df_fact_final, df_dim_date_db, left_on='last_review', right_on='date_key', how='left')
        # Renombrar la columna 'date_key' resultante del merge a 'fk_last_review_date'
        # y eliminar la 'last_review' original y la 'date_key' duplicada si existe.
        df_fact_final.drop(columns=['last_review'], inplace=True, errors='ignore')
        # Si 'date_key' existe por el merge, renombrar. Si no, algo falló.
        if 'date_key' in df_fact_final.columns: # date_key de dim_date
             df_fact_final.rename(columns={'date_key': 'fk_last_review_date'}, inplace=True)
        else: # Esto puede pasar si el merge no añadió la columna (ej. todos los last_review son nulos y no hay match)
             logger.warning("La columna 'date_key' no se añadió después del merge con dim_date. fk_last_review_date podría ser incorrecta o NaN.")
             df_fact_final['fk_last_review_date'] = pd.NA

    else:
        logger.warning("No se proporcionaron datos para dim_date. fk_last_review_date no se resolverá.")
        df_fact_final['fk_last_review_date'] = pd.NA


    # Verificar nulos en FKs (importante para integridad referencial)
    fk_cols = ['fk_host', 'fk_property_location', 'fk_property', 'fk_last_review_date']
    for fk_col in fk_cols:
        if fk_col in df_fact_final.columns and df_fact_final[fk_col].isnull().any():
            logger.warning(f"Se encontraron valores nulos en la columna de FK '{fk_col}' de la tabla de hechos. "
                           f"Esto puede indicar datos faltantes en las dimensiones o problemas en el merge de FKs. "
                           f"Número de nulos: {df_fact_final[fk_col].isnull().sum()}")
            # Aquí podrías decidir eliminar estas filas o cargarlas con FKs nulas si el modelo lo permite.
            # Por ahora, las cargaremos tal cual.
    
    # Seleccionar solo las columnas finales para la tabla de hechos
    # (fk_property, fk_host, fk_property_location, fk_last_review_date, price, service_fee, etc.)
    # La definición de la tabla 'fact_listing_pois' en model_dimensional.py tiene las columnas esperadas.
    # Necesitamos asegurar que df_fact_final tenga esas columnas y en ese orden.
    final_fact_columns = [
        'fk_property', 'fk_host', 'fk_property_location', 'fk_last_review_date',
        'price', 'service_fee', 'minimum_nights', 'number_of_reviews',
        'reviews_per_month', 'review_rate_number', 'availability_365',
        # Columnas de conteo de POIs (asegúrate que los nombres coincidan con la definición de la tabla)
        'nearby_restaurants_count', 'nearby_parks_and_outdoor_count', # Nombre corregido de la definicion de tabla
        'nearby_cultural_count', 'nearby_bars_and_clubs_count', # Nombre corregido
        'total_nearby_pois'
        # 'distance_to_nearest_poi_km' # Si la tienes
    ]
    
    # Filtrar solo las columnas que existen en df_fact_final y están en la lista final
    df_fact_to_load = df_fact_final[[col for col in final_fact_columns if col in df_fact_final.columns]].copy()

    missing_fact_cols = [col for col in final_fact_columns if col not in df_fact_to_load.columns]
    if missing_fact_cols:
        logger.warning(f"Columnas esperadas faltantes en el DataFrame final de hechos: {missing_fact_cols}. Se cargarán las disponibles.")


    # --- Carga a la Base de Datos ---
    try:
        with engine.begin() as connection:
            # Truncar la tabla de hechos antes de cargar (estrategia común)
            logger.info(f"Truncando la tabla de hechos '{fact_table_name}' antes de la carga.")
            connection.execute(text(f"TRUNCATE TABLE {fact_table_name} RESTART IDENTITY CASCADE;")) # CASCADE si hay vistas dependientes

            # Cargar el DataFrame de hechos
            if not df_fact_to_load.empty:
                df_fact_to_load.to_sql(
                    name=fact_table_name,
                    con=connection,
                    if_exists='append', # Ya truncamos, así que append está bien
                    index=False,
                    method='multi',
                    chunksize=10000
                )
                logger.info(f"Datos cargados exitosamente en la tabla de hechos '{fact_table_name}'. {len(df_fact_to_load)} filas.")
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
    prepared_dataframes: dict, # Diccionario de DFs de model_dimensional.py
    db_name: str = "airbnb",
    source_table_name: str = "final_merged_data" # No se usa directamente si prepared_dataframes ya tiene todo
):
    """
    Orquesta la carga de datos en el modelo dimensional (dimensiones y hechos).

    Args:
        prepared_dataframes (dict): Diccionario de DataFrames preparados, donde las claves son
                                    los nombres de las tablas y los valores son los DataFrames.
        db_name (str, optional): Nombre de la base de datos.
        source_table_name (str, optional): Nombre de la tabla de donde se originaron los datos mergeados.
                                           Actualmente no se usa directamente en esta función, ya que
                                           prepared_dataframes contiene los datos listos.
    """
    if not isinstance(prepared_dataframes, dict):
        logger.error("La entrada 'prepared_dataframes' debe ser un diccionario.")
        raise TypeError("Se esperaba un diccionario para prepared_dataframes.")

    logger.info("Iniciando carga de datos en el modelo dimensional...")
    engine = None
    try:
        engine = get_db_engine(db_name=db_name)

        # Definir el orden de carga: dimensiones primero, luego hechos.
        # Y la información necesaria para cada una (clave natural, pk de dimensión)
        
        # Cargar Dimensiones
        # dim_host
        if "dim_host" in prepared_dataframes:
            load_dimension_data(engine, prepared_dataframes["dim_host"], "dim_host",
                                natural_key_cols=['host_id'], dimension_pk_col='host_key')
        # dim_property_location
        if "dim_property_location" in prepared_dataframes:
            load_dimension_data(engine, prepared_dataframes["dim_property_location"], "dim_property_location",
                                natural_key_cols=['airbnb_neighbourhood_group', 'airbnb_neighbourhood', 'airbnb_lat', 'airbnb_long'],
                                dimension_pk_col='property_location_key')
        # dim_property
        if "dim_property" in prepared_dataframes:
            load_dimension_data(engine, prepared_dataframes["dim_property"], "dim_property",
                                natural_key_cols=['property_id'], dimension_pk_col='property_key')
        # dim_date
        if "dim_date" in prepared_dataframes:
            load_dimension_data(engine, prepared_dataframes["dim_date"], "dim_date",
                                natural_key_cols=['date_key'], dimension_pk_col='date_key') # PK es la NK
        # dim_api_place_category
        if "dim_api_place_category" in prepared_dataframes:
            load_dimension_data(engine, prepared_dataframes["dim_api_place_category"], "dim_api_place_category",
                                natural_key_cols=['category_group_name'], dimension_pk_col='api_category_key')

        # Cargar Tabla de Hechos
        if "fact_listing_pois" in prepared_dataframes:
            # Pasamos prepared_dataframes para que load_fact_data pueda acceder a los DFs de dimensiones si los necesita
            # (aunque actualmente los lee de la BD para asegurar las PKs más recientes)
            load_fact_data(engine, prepared_dataframes["fact_listing_pois"], "fact_listing_pois", prepared_dataframes)
        
        logger.info("Carga de datos en el modelo dimensional completada.")

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

    # Para probar esto, necesitaríamos la salida de `create_and_prepare_dimensional_model_data`
    # Es decir, un diccionario de DataFrames. Vamos a simularlo.

    # Simular DataFrames preparados (salida de model_dimensional.py)
    dim_host_test_data = pd.DataFrame({
        'host_id': [10, 20], 'host_name': ['Host Alice', 'Host Bob'],
        'host_identity_verified': [True, False], 'calculated_host_listings_count': [2,1]
    })
    dim_prop_loc_test_data = pd.DataFrame({
        'airbnb_neighbourhood_group': ['Manhattan', 'Brooklyn'],
        'airbnb_neighbourhood': ['Midtown', 'Williamsburg'],
        'airbnb_lat': [40.751, 40.710], 'airbnb_long': [-73.980, -73.950]
    })
    dim_prop_test_data = pd.DataFrame({
        'property_id': [1001, 1002], 'property_name': ['Cozy Apt A', 'Sunny Studio B'],
        'instant_bookable': [True, False], 'cancellation_policy': ['strict', 'moderate'],
        'room_type': ['Entire home/apt', 'Private room'], 'construction_year': [2010, 2015],
        'house_rules': ['No parties', 'No pets'], 'license': ['LIC123', None]
    })
    dim_date_test_data = pd.DataFrame({
        'date_key': [20230115, 20230210], 'full_date': [pd.Timestamp('2023-01-15'), pd.Timestamp('2023-02-10')],
        'year': [2023, 2023], 'month': [1, 2], 'day': [15, 10],
        'day_of_week': ['Sunday', 'Friday'], 'month_name': ['January', 'February']
    })
    dim_api_cat_test_data = pd.DataFrame({
        'category_group_name': ['restaurants', 'parks_and_outdoor', 'cultural']
    })
    fact_test_data_raw = pd.DataFrame({ # Con claves naturales
        'id': [1001, 1002], # property_id
        'host_id': [10, 20],
        'lat': [40.751, 40.710], 'long': [-73.980, -73.950],
        'neighbourhood_group': ['Manhattan', 'Brooklyn'], 'neighbourhood': ['Midtown', 'Williamsburg'],
        'last_review': [20230115, 20230210],
        'price': [150.0, 80.0], 'service_fee': [15.0, 8.0], 'minimum_nights': [1,3],
        'number_of_reviews': [10,5], 'reviews_per_month': [1.2,0.5], 'review_rate_number': [4,5],
        'availability_365': [300,100],
        'nearby_restaurants_count': [5, 2], 'nearby_parks_and_outdoor_count': [1, 0],
        'nearby_cultural_count': [0,1], 'nearby_bars_and_clubs_count': [3,1],
        'total_nearby_pois': [9, 4]
    })

    test_prepared_dfs = {
        "dim_host": dim_host_test_data,
        "dim_property_location": dim_prop_loc_test_data,
        "dim_property": dim_prop_test_data,
        "dim_date": dim_date_test_data,
        "dim_api_place_category": dim_api_cat_test_data,
        "fact_listing_pois": fact_test_data_raw
    }

    test_db_name_param = "airbnb_test_model" # La misma BD de prueba usada en model_dimensional.py
    logger.info(f"Usando base de datos de prueba: {test_db_name_param}")
    # Asegúrate de que las tablas fueron creadas por model_dimensional.py en esta BD.

    try:
        logger.info("Iniciando carga de datos dimensionales de prueba...")
        populate_dimensional_model(test_prepared_dfs, db_name=test_db_name_param)
        logger.info(f"Prueba de carga dimensional completada. Verifica los datos en las tablas de '{test_db_name_param}'.")

    except Exception as e:
        logger.error(f"Error durante la prueba local de load_dimensional.py: {e}", exc_info=True)
    finally:
        # No limpiamos aquí, ya que queremos verificar los datos.
        # La limpieza se puede hacer al final de todas las pruebas o manualmente.
        logger.info("--- Prueba local de load_dimensional.py finalizada ---")