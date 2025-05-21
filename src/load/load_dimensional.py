# Proyecto_ETL/src/load/load_dimensional.py

import logging
import pandas as pd
from datetime import datetime
from sqlalchemy import MetaData, select, and_, text
from sqlalchemy.exc import SQLAlchemyError
import sys
import os

PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
SRC_PATH = os.path.join(PROJECT_ROOT, "src")
if SRC_PATH not in sys.path:
    sys.path.append(SRC_PATH)
if PROJECT_ROOT not in sys.path:
    sys.path.append(PROJECT_ROOT)

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
    Carga en el modelo dimensional:
      1) Elimina columna obsoleta property_id
      2) Crea tablas (checkfirst=True)
      3) Trunca tablas
      4) Carga en bloque las dimensiones
      5) Inserta fila a fila la fact_publication
    """
    engine = get_db_engine(db_name)

    # 1) DEBUG: URL de conexión
    logger.info("DEBUG: Conectando a %s", engine.url)

    # 2) Eliminar columna obsoleta property_id de dim_property
    with engine.connect() as conn:
        logger.info("DEBUG: DROP COLUMN IF EXISTS property_id en dim_property")
        conn.execute(text('ALTER TABLE public.dim_property DROP COLUMN IF EXISTS property_id;'))

    # 3) Definir esquema en SQLAlchemy
    metadata = MetaData()
    tbl_host = define_dim_host(metadata)
    tbl_loc  = define_dim_property_location(metadata)
    tbl_prop = define_dim_property(metadata)
    tbl_date = define_dim_date(metadata)
    tbl_fact = define_fact_publication(metadata)

    # 4) Crear tablas si no existen
    metadata.create_all(engine, checkfirst=True)

    # 5) Truncar tablas para empezar limpias
    for table in ['fact_publication', 'dim_date', 'dim_property', 'dim_property_location', 'dim_host']:
        truncate_table(table, engine)

    try:
        # 6) Carga masiva de dimensiones
        dim_tables = ['dim_host', 'dim_property_location', 'dim_property', 'dim_date']
        with engine.begin() as conn:
            for table in dim_tables:
                df = input_data.get(table)
                if isinstance(df, pd.DataFrame) and not df.empty:
                    if table == 'dim_property':
                        logger.info("DEBUG dim_property HEAD:\n%s", df.head().to_markdown(index=False))
                        logger.info("DEBUG dim_property COLUMNS: %s", df.columns.tolist())
                        logger.info("DEBUG dim_property DTYPES:\n%s", df.dtypes.to_string())
                    logger.info(f"Cargando dimensión '{table}' ({len(df)} registros)...")
                    df.to_sql(
                        name=table,
                        con=conn,
                        if_exists='append',
                        index=False,
                        method='multi',
                        chunksize=1000
                    )
                else:
                    logger.info(f"No hay datos para '{table}' — se omite.")

        # 7) Inserción fila a fila en fact_publication
        fact_df = input_data.get('fact_publication')
        if not isinstance(fact_df, pd.DataFrame) or fact_df.empty:
            logger.warning("No hay datos en fact_publication — se omite la carga final.")
            return True

        # Preparar mapeo de POIs
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
            # upsert en dim_host
            host_key = conn.execute(
                select(tbl_host.c.host_key).where(tbl_host.c.host_id == row['host_id'])
            ).scalar()
            if host_key is None:
                host_key = conn.execute(
                    tbl_host.insert().values(
                        host_id=row['host_id'],
                        host_name=row['host_name'],
                        host_verification=row['host_verification'],
                        calculated_host_listings_count=row['calculated_host_listings_count']
                    )
                ).inserted_primary_key[0]

            # upsert en dim_property_location
            loc_key = conn.execute(
                select(tbl_loc.c.property_location_key)
                .where(and_(
                    tbl_loc.c.airbnb_neighbourhood_group == row['neighbourhood_group'],
                    tbl_loc.c.airbnb_neighbourhood       == row['neighbourhood'],
                    tbl_loc.c.airbnb_lat                 == row['lat'],
                    tbl_loc.c.airbnb_long                == row['long']
                ))
            ).scalar()
            if loc_key is None:
                loc_key = conn.execute(
                    tbl_loc.insert().values(
                        airbnb_neighbourhood_group=row['neighbourhood_group'],
                        airbnb_neighbourhood=row['neighbourhood'],
                        airbnb_lat=row['lat'],
                        airbnb_long=row['long']
                    )
                ).inserted_primary_key[0]

            # upsert en dim_property (usa publication_key)
            prop_key = conn.execute(
                select(tbl_prop.c.property_key)
                .where(tbl_prop.c.property_key == row['publication_key'])
            ).scalar()
            if prop_key is None:
                prop_key = conn.execute(
                    tbl_prop.insert().values(
                        property_key=row['publication_key'],
                        property_name=row['name'],
                        instant_bookable_flag=row['instant_bookable_flag'],
                        cancellation_policy=row['cancellation_policy'],
                        room_type=row['room_type'],
                        construction_year=row.get('construction_year')
                    )
                ).inserted_primary_key[0]

            # upsert en dim_date
            date_key = int(row['last_review'])
            exists = conn.execute(
                select(tbl_date.c.date_key).where(tbl_date.c.date_key == date_key)
            ).scalar()
            if exists is None:
                dt = datetime.strptime(str(date_key), '%Y%m%d').date()
                conn.execute(
                    tbl_date.insert().values(
                        date_key=date_key,
                        full_date=dt,
                        year=dt.year,
                        month=dt.month,
                        day=dt.day,
                        day_of_week=dt.strftime('%A'),
                        month_name=dt.strftime('%B')
                    )
                )

            # insertar hecho en fact_publication
            fact_vals = {
                'property_key': prop_key,
                'host_key': host_key,
                'property_location_key': loc_key,
                'date_key': date_key,
                'price': row['price'],
                'service_fee': row['service_fee'],
                'minimum_nights': row['minimum_nights'],
                'number_of_reviews': row['number_of_reviews'],
                'reviews_per_month': row['reviews_per_month'],
                'review_rate_number': row['review_rate_number'],
                'availability_365': row['availability_365']
            }
            for k in poi_keys:
                fact_vals[k] = int(row.get('count_nearby_' + k.split('_',1)[1], 0))

            conn.execute(tbl_fact.insert().values(**fact_vals))

        trans.commit()
        conn.close()
        return True

    except Exception as e:
        logger.error(f"Error en carga dimensional: {e}", exc_info=True)
        return False

    finally:
        engine.dispose()
