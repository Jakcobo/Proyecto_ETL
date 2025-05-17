# /home/nicolas/Escritorio/proyecto ETL/develop/src/database/model_dimensional.py

import pandas as pd
import logging
from sqlalchemy import (
    MetaData, Table, Column, Integer, String, Boolean,
    Float, DECIMAL, Date, BIGINT, ForeignKeyConstraint, Text, DateTime,
    UniqueConstraint
)
from sqlalchemy.sql import func # Para func.now()

# Asegurarse de que src esté en el path para importar db
import sys
import os
SRC_PATH = os.path.abspath(os.path.join(os.path.dirname(__file__), '..')) # .. porque estamos en src/database/
if SRC_PATH not in sys.path:
    sys.path.append(SRC_PATH)
DB_MODULE_PATH = os.path.abspath(os.path.join(os.path.dirname(__file__))) # Para importar .db
if DB_MODULE_PATH not in sys.path:
     sys.path.append(DB_MODULE_PATH)

try:
    from .db import get_db_engine # Usar importación relativa para db.py dentro del mismo paquete 'database'
except ImportError:
    # Fallback si se ejecuta directamente y la importación relativa falla
    from db import get_db_engine


logger = logging.getLogger(__name__)

# --- Definición de Tablas del Modelo Dimensional ---
# Estas funciones son similares a las de tu modeldb.py original,
# adaptadas para el contexto del nuevo DataFrame mergeado si es necesario.
# Los nombres de columna en df_merged deben coincidir con lo que se espera aquí.

def define_dim_host(metadata_obj: MetaData) -> Table:
    logger.info("Definiendo tabla: dim_host")
    return Table('dim_host', metadata_obj,
        Column('host_key', Integer, primary_key=True, autoincrement=True),
        Column('host_id', BIGINT, unique=True, nullable=False), # De Airbnb
        Column('host_name', String(255)),                      # De Airbnb
        Column('host_identity_verified', Boolean),             # De Airbnb
        # 'calculated_host_listings_count' podría venir de Airbnb si está en df_merged
        Column('calculated_host_listings_count', Integer)
    )

def define_dim_property_location(metadata_obj: MetaData) -> Table: # Renombrada de dim_spot_location
    logger.info("Definiendo tabla: dim_property_location")
    # Claves naturales de la propiedad de Airbnb
    natural_key_cols = ['airbnb_neighbourhood_group', 'airbnb_neighbourhood', 'airbnb_lat', 'airbnb_long']
    return Table('dim_property_location', metadata_obj,
        Column('property_location_key', Integer, primary_key=True, autoincrement=True),
        Column('airbnb_neighbourhood_group', String(100)), # De Airbnb
        Column('airbnb_neighbourhood', String(100)),       # De Airbnb
        Column('airbnb_lat', DECIMAL(10, 7)),              # De Airbnb
        Column('airbnb_long', DECIMAL(10, 7)),             # De Airbnb
        # Podríamos añadir 'country' y 'country_code' de Airbnb si están en df_merged
        # Column('country', String(100)),
        # Column('country_code', String(10)),
        UniqueConstraint(*natural_key_cols, name='uq_dim_property_location_nk')
    )

def define_dim_property(metadata_obj: MetaData) -> Table:
    logger.info("Definiendo tabla: dim_property")
    return Table('dim_property', metadata_obj,
        Column('property_key', Integer, primary_key=True, autoincrement=True),
        Column('property_id', BIGINT, unique=True, nullable=False), # ID del listado Airbnb
        Column('property_name', Text),                              # 'name' del listado Airbnb
        Column('instant_bookable', Boolean),                        # De Airbnb
        Column('cancellation_policy', String(100)),                 # De Airbnb
        Column('room_type', String(50)),                            # De Airbnb
        Column('construction_year', Integer),                       # De Airbnb
        Column('house_rules', Text),                                # De Airbnb
        Column('license', Text)                                     # De Airbnb
    )

def define_dim_date(metadata_obj: MetaData) -> Table: # Renombrada de dim_last_review
    logger.info("Definiendo tabla: dim_date (para last_review)")
    return Table('dim_date', metadata_obj,
        Column('date_key', Integer, primary_key=True), # YYYYMMDD (de last_review de Airbnb)
        Column('full_date', Date),
        Column('year', Integer),
        Column('month', Integer),
        Column('day', Integer),
        Column('day_of_week', String(10)), # Lunes, Martes...
        Column('month_name', String(20)) # Enero, Febrero...
    )

def define_dim_api_place_category(metadata_obj: MetaData) -> Table:
    logger.info("Definiendo tabla: dim_api_place_category")
    return Table('dim_api_place_category', metadata_obj,
        Column('api_category_key', Integer, primary_key=True, autoincrement=True),
        Column('category_group_name', String(100), unique=True, nullable=False) # ej. 'restaurants', 'parks_&_outdoor'
    )

def define_fact_listing_pois(metadata_obj: MetaData) -> Table: # Renombrada de fact_publication
    logger.info("Definiendo tabla: fact_listing_pois")
    return Table('fact_listing_pois', metadata_obj,
        Column('listing_poi_key', Integer, primary_key=True, autoincrement=True), # Nueva PK para esta tabla de hechos
        Column('fk_property', Integer, nullable=False),      # Ref a dim_property.property_key
        Column('fk_host', Integer, nullable=False),          # Ref a dim_host.host_key
        Column('fk_property_location', Integer, nullable=False), # Ref a dim_property_location.property_location_key
        Column('fk_last_review_date', Integer, nullable=False), # Ref a dim_date.date_key

        # Métricas de Airbnb
        Column('price', Float),
        Column('service_fee', DECIMAL(10, 2)),
        Column('minimum_nights', Integer),
        Column('number_of_reviews', Integer),
        Column('reviews_per_month', Float),
        Column('review_rate_number', Integer),
        Column('availability_365', Integer),

        # Métricas agregadas del merge (ejemplos, ajusta según lo que generes en merge.py)
        Column('nearby_restaurants_count', Integer),
        Column('nearby_parks_and_outdoor_count', Integer),
        Column('nearby_cultural_count', Integer),
        Column('nearby_bars_and_clubs_count', Integer),
        Column('total_nearby_pois', Integer),
        # Column('distance_to_nearest_poi_km', Float), # Si la calculas y guardas

        ForeignKeyConstraint(['fk_property'], ['dim_property.property_key']),
        ForeignKeyConstraint(['fk_host'], ['dim_host.host_key']),
        ForeignKeyConstraint(['fk_property_location'], ['dim_property_location.property_location_key']),
        ForeignKeyConstraint(['fk_last_review_date'], ['dim_date.date_key'])
    )

# --- Orquestador de Creación de Tablas ---
def create_dimensional_tables_if_not_exist(engine):
    """Crea todas las tablas del modelo dimensional si no existen."""
    metadata = MetaData()
    try:
        # Definir todas las tablas
        define_dim_host(metadata)
        define_dim_property_location(metadata)
        define_dim_property(metadata)
        define_dim_date(metadata)
        define_dim_api_place_category(metadata) # Nueva dimensión
        define_fact_listing_pois(metadata)

        logger.info("Intentando crear/actualizar todas las tablas dimensionales definidas en la BD...")
        metadata.create_all(engine, checkfirst=True)
        logger.info("Comprobación/creación/actualización de tablas dimensionales completada.")
        return True
    except Exception as e:
        logger.error(f"Error creando/actualizando tablas del modelo dimensional: {e}", exc_info=True)
        raise

# --- Funciones de Preparación de Datos para cada Dimensión/Hecho ---
# Estas funciones tomarán df_merged y extraerán/transformarán los datos para cada tabla del modelo.

def prepare_dim_host_data(df_merged: pd.DataFrame) -> pd.DataFrame:
    logger.info("Preparando datos para dim_host...")
    # Asumimos que las columnas ya tienen los nombres correctos después de la limpieza de Airbnb
    # y se mantuvieron en el merge.
    # Columnas necesarias: 'host_id', 'host_name', 'host_identity_verified', 'calculated_host_listings_count'
    # Estas columnas deben existir en df_merged.
    required_cols = ['host_id', 'host_name', 'host_identity_verified', 'calculated_host_listings_count']
    if not all(col in df_merged.columns for col in required_cols):
        missing = [col for col in required_cols if col not in df_merged.columns]
        logger.error(f"Columnas faltantes en df_merged para dim_host: {missing}")
        raise ValueError(f"Columnas faltantes para dim_host: {missing}")

    dim_host = df_merged[required_cols].drop_duplicates(subset=['host_id']).copy()
    dim_host['host_identity_verified'] = dim_host['host_identity_verified'].apply(
        lambda x: True if isinstance(x, str) and x.lower() == 'verified' else (True if x is True else False)
    )
    dim_host.rename(columns={'calculated_host_listings_count': 'calculated_host_listings_count'}, inplace=True) # No-op si ya se llama así
    logger.info(f"Datos para dim_host preparados. Filas: {len(dim_host)}")
    return dim_host

def prepare_dim_property_location_data(df_merged: pd.DataFrame) -> pd.DataFrame:
    logger.info("Preparando datos para dim_property_location...")
    # Renombrar columnas de df_merged si es necesario para que coincidan con la definición de la tabla
    # Asumimos que las columnas de Airbnb 'neighbourhood_group', 'neighbourhood', 'lat', 'long' están presentes
    # y las renombramos con prefijo 'airbnb_' para la dimensión.
    df_merged_renamed = df_merged.rename(columns={
        'neighbourhood_group': 'airbnb_neighbourhood_group',
        'neighbourhood': 'airbnb_neighbourhood',
        'lat': 'airbnb_lat', # Latitud de la propiedad Airbnb
        'long': 'airbnb_long' # Longitud de la propiedad Airbnb
    })
    required_cols = ['airbnb_neighbourhood_group', 'airbnb_neighbourhood', 'airbnb_lat', 'airbnb_long']
    if not all(col in df_merged_renamed.columns for col in required_cols):
        missing = [col for col in required_cols if col not in df_merged_renamed.columns]
        logger.error(f"Columnas faltantes en df_merged (después de renombrar) para dim_property_location: {missing}")
        raise ValueError(f"Columnas faltantes para dim_property_location: {missing}")

    dim_loc = df_merged_renamed[required_cols].drop_duplicates().copy()
    # Asegurar tipos correctos (DECIMAL se maneja en la carga a BD, aquí float está bien)
    dim_loc['airbnb_lat'] = pd.to_numeric(dim_loc['airbnb_lat'], errors='coerce')
    dim_loc['airbnb_long'] = pd.to_numeric(dim_loc['airbnb_long'], errors='coerce')
    dim_loc.dropna(subset=['airbnb_lat', 'airbnb_long'], inplace=True) # Crucial para PK natural
    logger.info(f"Datos para dim_property_location preparados. Filas: {len(dim_loc)}")
    return dim_loc

def prepare_dim_property_data(df_merged: pd.DataFrame) -> pd.DataFrame:
    logger.info("Preparando datos para dim_property...")
    # Asumimos que 'id' de Airbnb es 'property_id', y 'NAME' es 'property_name'
    # y el resto de columnas tienen nombres que coinciden o se renombran aquí.
    df_merged_renamed = df_merged.rename(columns={'id': 'property_id', 'name': 'property_name'})
    
    required_cols = [
        'property_id', 'property_name', 'instant_bookable', 'cancellation_policy',
        'room_type', 'construction_year', 'house_rules', 'license'
    ]
    if not all(col in df_merged_renamed.columns for col in required_cols):
        missing = [col for col in required_cols if col not in df_merged_renamed.columns]
        logger.error(f"Columnas faltantes en df_merged para dim_property: {missing}")
        raise ValueError(f"Columnas faltantes para dim_property: {missing}")

    dim_prop = df_merged_renamed[required_cols].drop_duplicates(subset=['property_id']).copy()
    # Limpieza de 'instant_bookable' (True/False)
    dim_prop['instant_bookable'] = dim_prop['instant_bookable'].apply(
         lambda x: True if isinstance(x, str) and x.lower() == 'true' else (True if x is True else False)
    )
    dim_prop['construction_year'] = pd.to_numeric(dim_prop['construction_year'], errors='coerce').astype('Int64')
    logger.info(f"Datos para dim_property preparados. Filas: {len(dim_prop)}")
    return dim_prop

def prepare_dim_date_data(df_merged: pd.DataFrame) -> pd.DataFrame:
    logger.info("Preparando datos para dim_date (de 'last_review')...")
    if 'last_review' not in df_merged.columns:
        logger.error("Columna 'last_review' faltante en df_merged para dim_date.")
        raise ValueError("Columna 'last_review' faltante.")

    # 'last_review' ya debería ser YYYYMMDD entero de la transformación de Airbnb
    dates_int = df_merged['last_review'].dropna().unique()
    dates_dt = pd.to_datetime(dates_int.astype(str), format='%Y%m%d', errors='coerce')
    
    dim_date = pd.DataFrame({
        'date_key': dates_int, # YYYYMMDD
        'full_date': dates_dt,
        'year': dates_dt.year,
        'month': dates_dt.month,
        'day': dates_dt.day,
        'day_of_week': dates_dt.strftime('%A'), # Lunes, Martes...
        'month_name': dates_dt.strftime('%B')  # Enero, Febrero...
    }).drop_duplicates(subset=['date_key']).sort_values(by='date_key').reset_index(drop=True)
    
    # Filtrar NaT si la conversión falló para algún valor (aunque YYYYMMDD debería ser robusto)
    dim_date.dropna(subset=['full_date'], inplace=True)
    # Convertir columnas a tipos enteros después de extraer partes de fecha
    for col in ['year', 'month', 'day']:
        dim_date[col] = dim_date[col].astype(int)

    logger.info(f"Datos para dim_date preparados. Filas: {len(dim_date)}")
    return dim_date

def prepare_dim_api_place_category_data(df_merged: pd.DataFrame) -> pd.DataFrame:
    logger.info("Preparando datos para dim_api_place_category...")
    # Esta dimensión se puebla con las categorías únicas de los POIs cercanos.
    # Necesitamos encontrar todas las columnas de conteo de POIs generadas por el merge.
    poi_count_cols = [col for col in df_merged.columns if col.startswith('nearby_') and col.endswith('_count')]
    
    all_categories = set()
    for col_name in poi_count_cols:
        # Extraer el nombre de la categoría del nombre de la columna
        # Ej: 'nearby_restaurants_count' -> 'restaurants'
        # Ej: 'nearby_parks_and_outdoor_count' -> 'parks_and_outdoor'
        cat_name_reconstructed = col_name.replace('nearby_', '').replace('_count', '').replace('_and_', '&')
        all_categories.add(cat_name_reconstructed)
    
    # Añadir 'other' si es una categoría posible que no generó una columna de conteo explícita
    # o si df_api tiene una columna 'category_group' que queremos usar directamente.
    # Si df_api fue usado en el merge y tenía 'category_group', podemos extraerlas de ahí también
    # (sería más robusto). Por ahora, usamos las columnas de conteo.
    # if 'other' not in all_categories: # Si 'Other' es una categoría válida de Foursquare
    #     all_categories.add('other')

    dim_cat = pd.DataFrame({'category_group_name': sorted(list(all_categories))})
    dim_cat = dim_cat[dim_cat['category_group_name'] != ''] # Eliminar si se genera una categoría vacía
    
    logger.info(f"Datos para dim_api_place_category preparados. Filas: {len(dim_cat)}")
    return dim_cat

def prepare_fact_listing_pois_data(df_merged: pd.DataFrame) -> pd.DataFrame:
    logger.info("Preparando datos para fact_listing_pois...")
    # Esta función tomará df_merged y seleccionará/renombrará las columnas para la tabla de hechos.
    # Las FKs se resolverán durante el proceso de carga a la BD.
    # Aquí, solo preparamos el DataFrame con los valores "naturales" o métricas.
    
    # Renombrar columnas de df_merged para que coincidan con los nombres de la tabla de hechos
    # y seleccionar las columnas necesarias.
    df_fact_prep = df_merged.copy()

    # Columnas de métricas de Airbnb (nombres ya deberían ser snake_case de la transformación)
    airbnb_metrics = [
        'price', 'service_fee', 'minimum_nights', 'number_of_reviews',
        'reviews_per_month', 'review_rate_number', 'availability_365'
    ]
    # Columnas de métricas agregadas de POIs (generadas por el merge)
    poi_metrics = [col for col in df_merged.columns if col.startswith('nearby_') and col.endswith('_count')]
    poi_metrics.append('total_nearby_pois')
    # if 'distance_to_nearest_poi_km' in df_merged.columns:
    #     poi_metrics.append('distance_to_nearest_poi_km')

    # Columnas que actuarán como claves naturales para buscar las FKs luego
    natural_key_columns_for_fk_lookup = [
        'id', # Para FK_Property (property_id)
        'host_id', # Para FK_Host
        'lat', 'long', 'neighbourhood_group', 'neighbourhood', # Para FK_Property_Location
        'last_review' # Para FK_Last_Review_Date (date_key)
    ]
    
    all_required_cols_for_fact = airbnb_metrics + poi_metrics + natural_key_columns_for_fk_lookup
    
    # Verificar que todas las columnas necesarias existan
    missing_cols = [col for col in all_required_cols_for_fact if col not in df_fact_prep.columns]
    if missing_cols:
        logger.error(f"Columnas faltantes en df_merged para fact_listing_pois: {missing_cols}")
        raise ValueError(f"Columnas faltantes para fact_listing_pois: {missing_cols}")

    df_fact = df_fact_prep[all_required_cols_for_fact].copy()
    
    # Asegurar tipos numéricos para métricas
    for col in airbnb_metrics + poi_metrics:
        if col in df_fact.columns:
            df_fact[col] = pd.to_numeric(df_fact[col], errors='coerce')
    
    # 'service_fee' es DECIMAL(10,2), Pandas no tiene un tipo DECIMAL directo, se maneja en la carga.
    # Asegurar que 'price' y 'service_fee' son float.
    if 'price' in df_fact.columns: df_fact['price'] = df_fact['price'].astype(float)
    if 'service_fee' in df_fact.columns: df_fact['service_fee'] = df_fact['service_fee'].astype(float)


    logger.info(f"Datos para fact_listing_pois preparados (con claves naturales). Filas: {len(df_fact)}")
    return df_fact


# --- Función Principal del Módulo ---
def create_and_prepare_dimensional_model_data(df_merged: pd.DataFrame, db_name: str = "airbnb") -> dict:
    """
    Orquesta la creación de tablas dimensionales y la preparación de DataFrames
    para cada tabla del modelo a partir del DataFrame mergeado.

    Args:
        df_merged (pd.DataFrame): El DataFrame resultante del paso de merge (3.1).
        db_name (str, optional): Nombre de la base de datos.

    Returns:
        dict: Un diccionario donde las claves son nombres de tablas dimensionales/hechos
              y los valores son los DataFrames preparados para esas tablas.
    """
    if not isinstance(df_merged, pd.DataFrame):
        logger.error("La entrada principal debe ser un DataFrame de Pandas.")
        raise TypeError("df_merged debe ser un DataFrame.")
    if df_merged.empty:
        logger.warning("El DataFrame mergeado de entrada está vacío. Se devolverá un diccionario de DataFrames vacíos.")
        # Devolver estructura vacía para que el siguiente paso no falle por falta de claves
        return {
            "dim_host": pd.DataFrame(),
            "dim_property_location": pd.DataFrame(),
            "dim_property": pd.DataFrame(),
            "dim_date": pd.DataFrame(),
            "dim_api_place_category": pd.DataFrame(),
            "fact_listing_pois": pd.DataFrame()
        }

    logger.info("Iniciando creación de esquema dimensional y preparación de datos...")
    engine = None
    try:
        engine = get_db_engine(db_name=db_name)
        
        # Paso 1: Crear las tablas en la BD si no existen
        creation_success = create_dimensional_tables_if_not_exist(engine)
        if not creation_success:
            # Esto no debería pasar ya que la función lanza error, pero por si acaso.
            raise RuntimeError("Falló la creación de tablas dimensionales.")

        # Paso 2: Preparar los DataFrames para cada tabla del modelo
        prepared_data = {}
        prepared_data["dim_host"] = prepare_dim_host_data(df_merged)
        prepared_data["dim_property_location"] = prepare_dim_property_location_data(df_merged)
        prepared_data["dim_property"] = prepare_dim_property_data(df_merged)
        prepared_data["dim_date"] = prepare_dim_date_data(df_merged)
        prepared_data["dim_api_place_category"] = prepare_dim_api_place_category_data(df_merged)
        prepared_data["fact_listing_pois"] = prepare_fact_listing_pois_data(df_merged)
        
        logger.info("Preparación de datos para el modelo dimensional completada.")
        return prepared_data

    except Exception as e:
        logger.error(f"Error en create_and_prepare_dimensional_model_data: {e}", exc_info=True)
        raise
    finally:
        if engine:
            engine.dispose()
            logger.info("Engine de base de datos dispuesto.")


if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO,
                        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    logger.info("--- Iniciando prueba local de model_dimensional.py ---")

    # Crear un df_merged de ejemplo (más completo que el de merge.py)
    # Este debería simular la salida de tu tarea de merge.
    sample_merged_data_for_model = {
        'id': [1001, 1002, 1003], # Airbnb listing ID
        'name': ['Cozy Apt A', 'Sunny Studio B', 'Spacious Loft C'], # Airbnb name
        'host_id': [10, 20, 10],
        'host_name': ['Host Alice', 'Host Bob', 'Host Alice'],
        'host_identity_verified': ['verified', 'unconfirmed', 'verified'],
        'calculated_host_listings_count': [2,1,2],
        'neighbourhood_group': ['Manhattan', 'Brooklyn', 'Manhattan'],
        'neighbourhood': ['Midtown', 'Williamsburg', 'Midtown'],
        'lat': [40.751, 40.710, 40.752],
        'long': [-73.980, -73.950, -73.982],
        'instant_bookable': [True, False, True],
        'cancellation_policy': ['strict', 'moderate', 'strict'],
        'room_type': ['Entire home/apt', 'Private room', 'Entire home/apt'],
        'construction_year': [2010, 2015, 2010],
        'price': [150.0, 80.0, 160.0],
        'service_fee': [15.0, 8.0, 16.0],
        'minimum_nights': [1, 3, 2],
        'number_of_reviews': [10, 5, 10],
        'last_review': [20230115, 20230210, 20230120], # YYYYMMDD int
        'reviews_per_month': [1.2, 0.5, 1.5],
        'review_rate_number': [4, 5, 4],
        'availability_365': [300, 100, 250],
        'house_rules': ['No parties', 'No pets', 'No parties'],
        'license': ['LIC123', None, 'LIC123'],
        # Columnas agregadas por el merge
        'nearby_restaurants_count': [5, 2, 6],
        'nearby_parks_and_outdoor_count': [1, 0, 1],
        'nearby_cultural_count': [0,1,0],
        'nearby_bars_and_clubs_count': [3,1,3],
        'total_nearby_pois': [9, 4, 10]
    }
    df_merged_test = pd.DataFrame(sample_merged_data_for_model)

    test_db_name_param = "airbnb_test_model" # Usar una BD de prueba para no afectar la principal
    logger.info(f"Usando base de datos de prueba: {test_db_name_param}")

    try:
        logger.info(f"DataFrame mergeado de prueba (entrada):\n{df_merged_test.head().to_markdown(index=False)}")
        
        prepared_dfs_dict = create_and_prepare_dimensional_model_data(df_merged_test, db_name=test_db_name_param)
        
        logger.info("\n--- DataFrames preparados para el modelo dimensional ---")
        for table_name, df_table_data in prepared_dfs_dict.items():
            logger.info(f"\nDataFrame para tabla: {table_name} (Shape: {df_table_data.shape})")
            if not df_table_data.empty:
                logger.info(f"{df_table_data.head().to_markdown(index=False)}")
            else:
                logger.info(" (DataFrame vacío)")
        
        logger.info(f"\nPrueba completada. Las tablas deberían existir en la BD '{test_db_name_param}'.")
        logger.info("Los datos NO han sido cargados a estas tablas por este script, solo preparados como DataFrames.")

    except Exception as e:
        logger.error(f"Error durante la prueba local de model_dimensional.py: {e}", exc_info=True)
    finally:
        # Opcional: Limpiar la base de datos de prueba
        # engine_cleanup = get_db_engine(db_name=test_db_name_param)
        # if engine_cleanup:
        #     try:
        #         with engine_cleanup.connect() as connection:
        #             metadata_cleanup = MetaData()
        #             metadata_cleanup.reflect(bind=engine_cleanup) # Cargar todas las tablas existentes
        #             metadata_cleanup.drop_all(bind=engine_cleanup) # Eliminar todas las tablas
        #             logger.info(f"Todas las tablas han sido eliminadas de la base de datos de prueba '{test_db_name_param}'.")
        #     except Exception as e_cleanup:
        #         logger.error(f"Error limpiando la base de datos de prueba: {e_cleanup}")
        #     finally:
        #         engine_cleanup.dispose()
        logger.info("--- Prueba local de model_dimensional.py finalizada ---")