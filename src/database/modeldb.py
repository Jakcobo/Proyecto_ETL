# /home/nicolas/Escritorio/proyecto/otra_prueba/src/database/modeldb.py
import os
import logging
from sqlalchemy import (
    MetaData, Table, Column, Integer, String, Boolean,
    Float, DECIMAL, Date, BIGINT, ForeignKeyConstraint, inspect, create_engine,
    UniqueConstraint, Text
)
from .db import get_db_engine, load_enviroment_variables

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s", datefmt="%m/%d/%Y %I:%M:%S %p")
logger = logging.getLogger(__name__)

# --- Funciones con Nombres en snake_case ---

def define_dim_host(metadata):
    logger.info("Definiendo tabla: dim_host")
    return Table('dim_host', metadata,
        # --- CORREGIDO: snake_case ---
        Column('host_key', Integer, primary_key=True, autoincrement=True),
        Column('host_id', BIGINT, unique=True, nullable=False),
        Column('host_name', String(100)),
        Column('host_identity_verified', Boolean),
        Column('calculated_host_listings_count', Integer)
    )

def define_dim_spot_location(metadata):
    logger.info("Definiendo tabla: dim_spot_location")
    natural_key_cols = ['neighbourhood_group', 'neighbourhood', 'lat', 'long', 'country', 'country_code']
    return Table('dim_spot_location', metadata,
        # --- CORREGIDO: snake_case ---
        Column('location_key', Integer, primary_key=True, autoincrement=True),
        Column('neighbourhood_group', String(100), nullable=False),
        Column('neighbourhood', String(100), nullable=False),
        Column('lat', DECIMAL(10, 7), nullable=False),
        Column('long', DECIMAL(10, 7), nullable=False),
        Column('country', String(100), nullable=False),
        Column('country_code', String(10), nullable=False),
        UniqueConstraint(*natural_key_cols, name='uq_dim_spot_location_nk') # Nombre constraint más corto
    )

def define_dim_property(metadata):
    logger.info("Definiendo tabla: dim_property")
    return Table('dim_property', metadata,
        # --- CORREGIDO: snake_case ---
        Column('property_key', Integer, primary_key=True, autoincrement=True),
        Column('property_id', BIGINT, unique=True, nullable=False),
        Column('name', Text),
        Column('instant_bookable', Boolean),
        Column('cancellation_policy', String(100)),
        Column('room_type', String(50)),
        Column('construction_year', Integer),
        Column('house_rules', Text),
        Column('license', Text)
    )

def define_dim_last_review(metadata):
    logger.info("Definiendo tabla: dim_last_review")
    return Table('dim_last_review', metadata,
        # --- CORREGIDO: snake_case ---
        Column('date_key', Integer, primary_key=True), # PK natural
        Column('full_date', Date),
        Column('year', Integer),
        Column('month', Integer),
        Column('day', Integer)
    )

def define_fact_publication(metadata):
    logger.info("Definiendo tabla: fact_publication")
    return Table('fact_publication', metadata,
        # --- CORREGIDO: snake_case ---
        Column('publication_id', Integer, primary_key=True, autoincrement=True),
        Column('fk_property', Integer, nullable=False), # Referencia a dim_property.property_key
        Column('fk_host', Integer, nullable=False),     # Referencia a dim_host.host_key
        Column('fk_location', Integer, nullable=False), # Referencia a dim_spot_location.location_key
        Column('fk_date', Integer, nullable=False),     # Referencia a dim_last_review.date_key
        Column('price', Float),
        Column('service_fee', DECIMAL(10, 2)),
        Column('minimum_nights', Integer),
        Column('number_of_reviews', Integer),
        Column('reviews_per_month', Float),
        Column('review_rate_number', Integer),
        Column('availability_365', Integer),

        # --- CORREGIDO: Referencias FK a columnas snake_case ---
        ForeignKeyConstraint(['fk_host'], ['dim_host.host_key']),
        ForeignKeyConstraint(['fk_location'], ['dim_spot_location.location_key']),
        ForeignKeyConstraint(['fk_property'], ['dim_property.property_key']),
        ForeignKeyConstraint(['fk_date'], ['dim_last_review.date_key'])
    )

# ... (create_dimensional_model_tables y bloque __main__ sin cambios lógicos) ...
def create_dimensional_model_tables(engine):
    metadata = MetaData()
    try:
        define_dim_host(metadata)
        define_dim_spot_location(metadata)
        define_dim_property(metadata)
        define_dim_last_review(metadata)
        define_fact_publication(metadata)
    # ... (resto de la función create_dimensional_model_tables sin cambios) ...
    except Exception as e:
        logger.error(f"Error definiendo las tablas en metadata: {e}", exc_info=True)
        raise
    try:
        logger.info("Attempting to create/update all defined dimensional tables in the database...")
        metadata.create_all(engine, checkfirst=True)
        logger.info("Dimensional tables check/creation/update completed.")
        # ... (verificaciones opcionales) ...
    except Exception as e:
        logger.error(f"Error creating/updating dimensional model tables: {e}", exc_info=True)
        raise

if __name__ == "__main__":
    # ... (bloque sin cambios lógicos) ...
    logger.info("Running modeldb.py in standalone mode for testing table creation/update...")
    engine = None
    try:
        if load_enviroment_variables():
             logger.info("Environment variables loaded.")
        else:
             logger.warning("Could not load environment variables from .env file.")
        engine = get_db_engine()
        if engine:
            create_dimensional_model_tables(engine)
            logger.info("Standalone test script finished successfully.")
        else:
            logger.error("Failed to create database engine. Aborting test.")
    except ValueError as ve:
        logger.error(f"Configuration error: {ve}")
    except Exception as e:
        logger.error(f"An unexpected error occurred during standalone execution: {e}", exc_info=True)
    finally:
        if engine:
            engine.dispose()
            logger.info("Database engine disposed.")