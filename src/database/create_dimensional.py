# Proyecto_ETL/src/
import pandas as pd
import logging
from sqlalchemy import (
    MetaData, Table, Column, Integer, String, Boolean,
    Float, DECIMAL, Date, BIGINT, ForeignKeyConstraint, Text,
    UniqueConstraint
)
from sqlalchemy.sql import func
import sys
import os

SRC_PATH = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
if SRC_PATH not in sys.path:
    sys.path.append(SRC_PATH)
DB_MODULE_PATH = os.path.abspath(os.path.join(os.path.dirname(__file__)))
if DB_MODULE_PATH not in sys.path:
    sys.path.append(DB_MODULE_PATH)

try:
    from .db import get_db_engine
except ImportError:
    from db import get_db_engine

logger = logging.getLogger(__name__)

def define_dim_host(metadata_obj: MetaData) -> Table:
    logger.info("Definiendo tabla: dim_host")
    return Table('dim_host', metadata_obj,
        Column('host_key', Integer, primary_key=True, autoincrement=True),
        Column('host_id', BIGINT, unique=True, nullable=False),
        Column('host_name', String(255)),
        Column('host_verification', Boolean),
        Column('calculated_host_listings_count', Integer)
    )

def define_dim_property_location(metadata_obj: MetaData) -> Table:
    logger.info("Definiendo tabla: dim_property_location")
    natural_key_cols = ['airbnb_neighbourhood_group', 'airbnb_neighbourhood', 'airbnb_lat', 'airbnb_long']
    return Table('dim_property_location', metadata_obj,
        Column('property_location_key', Integer, primary_key=True, autoincrement=True),
        Column('airbnb_neighbourhood_group', String(100)),
        Column('airbnb_neighbourhood', String(100)),
        Column('airbnb_lat', DECIMAL(10, 7)),
        Column('airbnb_long', DECIMAL(10, 7)),
        UniqueConstraint(*natural_key_cols, name='uq_dim_property_location_nk')
    )

def define_dim_property(metadata_obj: MetaData) -> Table:
    logger.info("Definiendo tabla: dim_property")
    return Table('dim_property', metadata_obj,
        Column('property_key', Integer, primary_key=True, autoincrement=True),
        Column('property_id', BIGINT, unique=True, nullable=False),
        Column('property_name', Text),
        Column('instant_bookable_flag', Boolean),
        Column('cancellation_policy', String(100)),
        Column('room_type', String(50)),
        Column('construction_year', Integer)
    )

def define_dim_date(metadata_obj: MetaData) -> Table:
    logger.info("Definiendo tabla: dim_date")
    return Table('dim_date', metadata_obj,
        Column('date_key', Integer, primary_key=True),
        Column('full_date', Date),
        Column('year', Integer),
        Column('month', Integer),
        Column('day', Integer),
        Column('day_of_week', String(10)),
        Column('month_name', String(20))
    )

def define_fact_publication(metadata_obj: MetaData) -> Table:
    logger.info("Definiendo tabla: fact_publication")
    return Table('fact_publication', metadata_obj,
        Column('id', Integer, primary_key=True, autoincrement=True),
        Column('fk_property', Integer, nullable=False),
        Column('fk_host', Integer, nullable=False),
        Column('fk_property_location', Integer, nullable=False),
        Column('fk_last_review_date', Integer, nullable=False),
        Column('price', Float),
        Column('service_fee', DECIMAL(10, 2)),
        Column('minimum_nights', Integer),
        Column('number_of_reviews', Integer),
        Column('reviews_per_month', Float),
        Column('review_rate_number', Integer),
        Column('availability_365', Integer),
        Column('nearby_restaurants', Integer),
        Column('nearby_parks_and_outdoor', Integer),
        Column('nearby_cultural', Integer),
        Column('nearby_retail_and_shopping', Integer),
        Column('nearby_bars_and_clubs', Integer),
        Column('nearby_landmarks', Integer),
        Column('nearby_entertainment_leisure', Integer),
        ForeignKeyConstraint(['fk_property'], ['dim_property.property_key']),
        ForeignKeyConstraint(['fk_host'], ['dim_host.host_key']),
        ForeignKeyConstraint(['fk_property_location'], ['dim_property_location.property_location_key']),
        ForeignKeyConstraint(['fk_last_review_date'], ['dim_date.date_key'])
    )

def create_dimensional_tables_if_not_exist(engine):
    metadata = MetaData()
    try:
        define_dim_host(metadata)
        define_dim_property_location(metadata)
        define_dim_property(metadata)
        define_dim_date(metadata)
        define_fact_publication(metadata)
        metadata.create_all(engine, checkfirst=True)
        logger.info("Tablas creadas/verificadas correctamente.")
        return True
    except Exception as e:
        logger.error(f"Error creando tablas: {e}", exc_info=True)
        raise

def prepare_dim_host_data(df_merged: pd.DataFrame) -> pd.DataFrame:
    logger.info("Preparando datos para dim_host...")
    required_cols = ['host_id', 'host_name', 'host_verification', 'calculated_host_listings_count']
    if not all(col in df_merged.columns for col in required_cols):
        missing = [col for col in required_cols if col not in df_merged.columns]
        raise ValueError(f"Faltan columnas en dim_host: {missing}")
    dim_host = df_merged[required_cols].drop_duplicates(subset=['host_id']).copy()
    dim_host['host_verification'] = dim_host['host_verification'].apply(
        lambda x: True if isinstance(x, str) and x.lower() == 'verified' else bool(x)
    )
    return dim_host

def prepare_dim_property_location_data(df_merged: pd.DataFrame) -> pd.DataFrame:
    logger.info("Preparando datos para dim_property_location...")
    df = df_merged.rename(columns={
        'neighbourhood_group': 'airbnb_neighbourhood_group',
        'neighbourhood': 'airbnb_neighbourhood',
        'lat': 'airbnb_lat',
        'long': 'airbnb_long'
    })
    cols = ['airbnb_neighbourhood_group', 'airbnb_neighbourhood', 'airbnb_lat', 'airbnb_long']
    df = df[cols].drop_duplicates().copy()
    df['airbnb_lat'] = pd.to_numeric(df['airbnb_lat'], errors='coerce')
    df['airbnb_long'] = pd.to_numeric(df['airbnb_long'], errors='coerce')
    return df.dropna(subset=['airbnb_lat', 'airbnb_long'])

def prepare_dim_property_data(df_merged: pd.DataFrame) -> pd.DataFrame:
    logger.info("Preparando datos para dim_property...")
    df = df_merged.rename(columns={'id': 'property_id', 'name': 'property_name'})
    cols = [
        'property_id', 'property_name', 'instant_bookable_flag', 'cancellation_policy',
        'room_type', 'construction_year'
    ]
    df = df[cols].drop_duplicates(subset=['property_id']).copy()
    df['instant_bookable_flag'] = df['instant_bookable_flag'].apply(lambda x: str(x).lower() == 'true')
    df['construction_year'] = pd.to_numeric(df['construction_year'], errors='coerce').astype('Int64')
    return df

def prepare_dim_date_data(df_merged: pd.DataFrame) -> pd.DataFrame:
    logger.info("Preparando datos para dim_date...")
    dates_int = df_merged['last_review'].dropna().unique()
    dates_dt = pd.to_datetime(dates_int.astype(str), format='%Y%m%d', errors='coerce')
    df = pd.DataFrame({
        'date_key': dates_int,
        'full_date': dates_dt,
        'year': dates_dt.year,
        'month': dates_dt.month,
        'day': dates_dt.day,
        'day_of_week': dates_dt.strftime('%A'),
        'month_name': dates_dt.strftime('%B')
    }).dropna(subset=['full_date']).drop_duplicates(subset=['date_key']).sort_values(by='date_key')
    for col in ['year', 'month', 'day']:
        df[col] = df[col].astype(int)
    return df

def prepare_fact_publication_data(df_merged: pd.DataFrame) -> pd.DataFrame:
    logger.info("Preparando datos para fact_publication...")
    airbnb_metrics = [
        'price', 'service_fee', 'minimum_nights', 'number_of_reviews',
        'reviews_per_month', 'review_rate_number', 'availability_365'
    ]
    poi_metrics = [col for col in df_merged.columns if col.startswith('nearby_') and col.endswith('_count')]

    lookup_cols = ['id', 'host_id', 'lat', 'long', 'neighbourhood_group', 'neighbourhood', 'last_review']
    required_cols = airbnb_metrics + poi_metrics + lookup_cols
    missing_cols = [col for col in required_cols if col not in df_merged.columns]
    if missing_cols:
        raise ValueError(f"Faltan columnas en fact_publication: {missing_cols}")
    df = df_merged[required_cols].copy()
    for col in airbnb_metrics + poi_metrics:
        df[col] = pd.to_numeric(df[col], errors='coerce')
    return df

def create_and_prepare_dimensional_model_data(df_merged: pd.DataFrame, db_name: str) -> dict:
    if not isinstance(df_merged, pd.DataFrame):
        raise TypeError("df_merged debe ser un DataFrame.")
    if df_merged.empty:
        logger.warning("df_merged está vacío. Se devuelven tablas vacías.")
        return {k: pd.DataFrame() for k in ["dim_host", "dim_property_location", "dim_property", "dim_date", "fact_publication"]}

    logger.info("Iniciando proceso de creación de modelo dimensional...")
    engine = get_db_engine(db_name)
    try:
        create_dimensional_tables_if_not_exist(engine)
        return {
            "dim_host": prepare_dim_host_data(df_merged),
            "dim_property_location": prepare_dim_property_location_data(df_merged),
            "dim_property": prepare_dim_property_data(df_merged),
            "dim_date": prepare_dim_date_data(df_merged),
            "fact_publication": prepare_fact_publication_data(df_merged)
        }
    finally:
        engine.dispose()
        logger.info("Conexión cerrada.")
