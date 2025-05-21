import logging
import pandas as pd
from datetime import datetime
from sqlalchemy import MetaData, select, and_, text
from sqlalchemy.exc import SQLAlchemyError
from database.db import get_db_engine
from database.create_dimensional import (
    define_dim_host,
    define_dim_property_location,
    define_dim_property,
    define_dim_date,
    define_fact_publication
)

logger = logging.getLogger(__name__)

def truncate_table(table_name: str, engine):
    """
    Elimina todos los registros de una tabla. ¡USAR CON PRECAUCIÓN!
    """
    try:
        with engine.connect() as conn:
            trans = conn.begin()
            logger.info(f"Truncando tabla '{table_name}'...")
            conn.execute(text(f'DELETE FROM public."{table_name}";'))
            trans.commit()
            logger.info(f"Tabla '{table_name}' truncada correctamente.")
    except SQLAlchemyError as e:
        logger.error(f"Error al truncar '{table_name}': {e}", exc_info=True)
        if 'trans' in locals() and trans.is_active:
            trans.rollback()
        raise
    except Exception as e:
        logger.error(f"Error inesperado truncando '{table_name}': {e}", exc_info=True)
        if 'trans' in locals() and trans.is_active:
            trans.rollback()
        raise


def load_dimensional_data(input_data: dict, db_name: str, load_order=None) -> bool:
    """
    Carga en el modelo dimensional: primero borra tablas, carga dimensiones en bloque
    y finalmente inserta la tabla de hechos desde el DataFrame de fact_publication.

    Args:
        input_data (dict): Diccionario con DataFrames para cada tabla dimensional y 'fact_publication'.
        db_name (str): Nombre de la base de datos.
        load_order (list, optional): Orden de carga de dimensiones (ignorado).

    Returns:
        bool: True si la carga fue exitosa, False en caso de error.
    """
    engine = get_db_engine(db_name)
    metadata = MetaData()
    # Definir tablas
    tbl_host = define_dim_host(metadata)
    tbl_loc = define_dim_property_location(metadata)
    tbl_prop = define_dim_property(metadata)
    tbl_date = define_dim_date(metadata)
    tbl_fact = define_fact_publication(metadata)
    metadata.create_all(engine, checkfirst=True)

    # Truncar todas las tablas para evitar duplicados
    for table in ['fact_publication', 'dim_date', 'dim_property', 'dim_property_location', 'dim_host']:
        truncate_table(table, engine)

    try:
        # 1) Carga masiva de dimensiones
        dim_tables = ['dim_host', 'dim_property_location', 'dim_property', 'dim_date']
        with engine.begin() as conn:
            for table in dim_tables:
                df = input_data.get(table)
                if isinstance(df, pd.DataFrame) and not df.empty:
                    logger.info(f"Cargando dimensión '{table}' con {len(df)} registros.")
                    df.to_sql(
                        name=table,
                        con=conn,
                        if_exists='append',
                        index=False,
                        method='multi',
                        chunksize=1000
                    )
                else:
                    logger.info(f"No hay datos para '{table}', se omite.")

        # 2) Inserción fila a fila en fact_publication
        fact_df = input_data.get('fact_publication')
        if not isinstance(fact_df, pd.DataFrame) or fact_df.empty:
            logger.warning("No hay datos en 'fact_publication', carga final omitida.")
            return True

        # Mapear columnas count_nearby a nearby
        col_map = {
            'count_nearby_restaurants': 'nearby_restaurants',
            'count_nearby_parks_and_outdoor': 'nearby_parks_and_outdoor',
            'count_nearby_retail_and_shopping': 'nearby_retail_and_shopping',
            'count_nearby_bars_and_clubs': 'nearby_bars_and_clubs',
            'count_nearby_landmarks': 'nearby_landmarks',
            'count_nearby_entertainment_and_leisure': 'nearby_entertainment_leisure',
            'count_nearby_cultural': 'nearby_cultural'
        }
        poi_keys = list(col_map.values())

        conn = engine.connect()
        trans = conn.begin()
        for _, row in fact_df.iterrows():
            # Upsert en dimensiones claves
            # host
            host_sel = select(tbl_host.c.host_key).where(tbl_host.c.host_id == row['host_id'])
            host_key = conn.execute(host_sel).scalar()
            if host_key is None:
                host_key = conn.execute(tbl_host.insert().values(
                    host_id=row['host_id'],
                    host_name=row['host_name'],
                    host_verification=row['host_verification'],
                    calculated_host_listings_count=row['calculated_host_listings_count']
                )).inserted_primary_key[0]
            # property_location
            loc_sel = select(tbl_loc.c.property_location_key).where(and_(
                tbl_loc.c.airbnb_neighbourhood_group == row['neighbourhood_group'],
                tbl_loc.c.airbnb_neighbourhood == row['neighbourhood'],
                tbl_loc.c.airbnb_lat == row['lat'],
                tbl_loc.c.airbnb_long == row['long']
            ))
            loc_key = conn.execute(loc_sel).scalar()
            if loc_key is None:
                loc_key = conn.execute(tbl_loc.insert().values(
                    airbnb_neighbourhood_group=row['neighbourhood_group'],
                    airbnb_neighbourhood=row['neighbourhood'],
                    airbnb_lat=row['lat'],
                    airbnb_long=row['long']
                )).inserted_primary_key[0]
            # property
            prop_sel = select(tbl_prop.c.property_key).where(tbl_prop.c.property_id == row['id'])
            prop_key = conn.execute(prop_sel).scalar()
            if prop_key is None:
                prop_key = conn.execute(tbl_prop.insert().values(
                    property_id=row['id'],
                    property_name=row['name'],
                    instant_bookable_flag=row['instant_bookable_flag'],
                    cancellation_policy=row['cancellation_policy'],
                    room_type=row['room_type'],
                    construction_year=row.get('construction_year')
                )).inserted_primary_key[0]
            # date
            date_key = int(row['last_review'])
            date_sel = select(tbl_date.c.date_key).where(tbl_date.c.date_key == date_key)
            if conn.execute(date_sel).scalar() is None:
                dt = datetime.strptime(str(date_key), '%Y%m%d').date()
                conn.execute(tbl_date.insert().values(
                    date_key=date_key,
                    full_date=dt,
                    year=dt.year,
                    month=dt.month,
                    day=dt.day,
                    day_of_week=dt.strftime('%A'),
                    month_name=dt.strftime('%B')
                ))
            # Insertar hecho
            fact_vals = {
                'fk_property': prop_key,
                'fk_host': host_key,
                'fk_property_location': loc_key,
                'fk_last_review_date': date_key,
                'price': row['price'],
                'service_fee': row['service_fee'],
                'minimum_nights': row['minimum_nights'],
                'number_of_reviews': row['number_of_reviews'],
                'reviews_per_month': row['reviews_per_month'],
                'review_rate_number': row['review_rate_number'],
                'availability_365': row['availability_365']
            }
            # Inicializar nearby y asignar valores reales
            for k in poi_keys:
                fact_vals[k] = int(row.get('count_nearby_' + k.split('_', 1)[1], 0))
            conn.execute(tbl_fact.insert().values(**fact_vals))
        trans.commit()
        conn.close()
        return True
    except Exception as e:
        logger.error(f"Error en carga dimensional: {e}", exc_info=True)
        return False
    finally:
        engine.dispose()
