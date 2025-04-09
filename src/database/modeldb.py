# /home/nicolas/Escritorio/proyecto/otra_prueba/src/database/modeldb.py
import os
import logging
from sqlalchemy import (
    MetaData, Table, Column, Integer, String, Boolean,
    Float, DECIMAL, Date, BIGINT, ForeignKeyConstraint, inspect, create_engine # Add create_engine for the main block
)
from .db import get_db_engine

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s", datefmt="%m/%d/%Y %I:%M:%S %p")
logger = logging.getLogger(__name__)

def define_dim_host(metadata):
    """Define table on dim_host in the metadata."""
    logger.debug("Defining table: dim_host")
    
    return Table('dim_host', metadata,
        Column('host_id', BIGINT, primary_key=True, comment="Original host ID. Used as a natural PK."),
        Column('host_name', String(100), comment="Host name."),
        Column('host_since', Date, comment="Date since the host is active."),
        Column('host_location', String(255), comment="Location reported by the host."),
        Column('host_response_time', String(50), comment="Host response time."),
        Column('host_is_superhost', Boolean, comment="Indicates if the host is a superhost."),
        Column('host_identity_verified', Boolean, comment="Indicates if the host's identity is verified."),
        Column('calculated_host_listings_count', Integer, comment="Number of calculated listings for the host.")
    )

def define_dim_spot_location(metadata):
    """Define table on dim_spot_location in the metadata."""
    logger.debug("Defining table: dim_spot_location")
    return Table('dim_spot_location', metadata,
        Column('location_key', Integer, primary_key=True, comment="Surrogate key for the location dimension."),
        Column('neighbourhood_group', String(50), comment="Neighborhood group."),
        Column('neighbourhood', String(100), comment="Specific neighborhood."),
        Column('lat', DECIMAL(9, 6), comment="Latitude of the location."), # DECIMAL is more precise for coordinates than Float
        Column('long', DECIMAL(9, 6), comment="Longitude of the location."),
        Column('country', String(100), comment="Country."), # Was not in your original definition
        Column('country_code', String(3), comment="Country code (e.g., 'ESP').")
        
    )

def define_dim_property(metadata):
    """Define table dim_property in the metadata."""
    logger.debug("Defining table: dim_property")
    return Table('dim_property', metadata,
        Column('Property_Key', Integer, primary_key=True, comment="Surrogate key for the property dimension."),
        Column('name', String(100), comment="Name or description of the property."),
        Column('instant_bookable', Boolean, comment="Indicates if the property can be booked instantly."),
        Column('cancellation_policy', String(100), comment="Cancellation policy."),
        Column('room_type', String(50), comment="Type of room or property."),
        Column('construction_year', Integer, comment="Year of construction."),
        Column('house_rules', String(250), comment="House rules."),
        Column('license', String(100), comment="License number, if applicable.")
    )

def define_dim_last_review(metadata):
    """Define table dim_last_review in the metadata."""
    logger.debug("Defining table: dim_last_review")
    return Table('dim_last_review', metadata,
        Column('Date_Key', Integer, primary_key=True, comment="Surrogate key for the time dimension (based on the last review)."),
        Column('full_date', Date, comment="Full date of the last review."),
        Column('year', Integer, comment="Year of the last review."),
        Column('month', Integer, comment="Month of the last review."),
        Column('day', Integer, comment="Day of the last review.")
    )

def define_fact_publication(metadata):
    """Define table fact_publication in the metadata, including FKs."""
    logger.debug("Defining table: fact_publication")
    return Table('fact_publication', metadata,
        Column('Publication_ID', Integer, primary_key=True, comment="Primary key of the fact table (can be original or surrogate ID)."),
        Column('FK_Host', Integer, comment="Foreign key to dim_host."),
        Column('FK_Location', Integer, comment="Foreign key to dim_spot_location."),
        Column('FK_Property', Integer, comment="Foreign key to dim_property."),
        Column('FK_Date', Integer, comment="Foreign key to dim_last_review."),
        Column('price', Float, comment="Price per night."), # Float is common for prices, but DECIMAL might be better if exact precision is required.
        Column('service_fee', DECIMAL(10, 2), comment="Service fee."),
        Column('minimum_nights', Integer, comment="Minimum required nights."),
        Column('number_of_reviews', Integer, comment="Total number of reviews."),
        Column('reviews_per_month', Float, comment="Average reviews per month."),
        Column('review_rate_number', Integer, comment="Review score (scale?)."), # Assuming it's an integer
        Column('availability_365', Integer, comment="Number of days available in the next 365 days."),

        # FK restrictions definition
        ForeignKeyConstraint(['FK_Host'], ['dim_host.Host_Key']),
        ForeignKeyConstraint(['FK_Location'], ['dim_spot_location.Location_Key']),
        ForeignKeyConstraint(['FK_Property'], ['dim_property.Property_Key']),
        ForeignKeyConstraint(['FK_Date'], ['dim_last_review.Date_Key'])
    )


def create_dimensional_model_tables(db_name="airbnb"):
    """Define and create all the tables in the dimensional model if not exists, using engine."""
    engine = get_db_engine(db_name)
    if not engine:
        raise ValueError(f"Connection to the database '{db_name}' failed")
    metadata = MetaData()
    logger.info("Defining dimensional model schema...")

    define_dim_host(metadata)
    define_dim_spot_location(metadata) # Tentative definition with composite PK
    define_dim_property(metadata)
    define_dim_last_review(metadata)
    define_fact_publication(metadata) # Fact table defined at the end

    try:
        logger.info("Attempting to create/verify all dimensional tables...")
        metadata.create_all(engine, checkfirst=True)
        logger.info("Dimensional tables check/creation complete.")

        inspector = inspect(engine)
        created_tables = inspector.get_table_names()
        logger.debug(f"Tables existing in the database: {created_tables}")
        all_defined_created = True
        for table_name in metadata.tables.keys():
            if table_name not in created_tables:
                logger.warning(f"Defined table '{table_name}' was not found after creation attempt.")
                all_defined_created = False
        if all_defined_created:
            logger.info("All defined dimensional tables exist in the database.")

    except Exception as e:
        logger.error(f"Error creating dimensional model tables: {e}", exc_info=True)
        raise

if __name__ == "__main__":
    logger.info("Starting script to create/verify dimensional model schema...")
    db_engine = None
    try:
        db_engine = get_db_engine()
        create_dimensional_model_tables(db_engine)
        logger.info("Dimensional schema script finished successfully.")

    except ValueError as ve:
        logging.error(f"Configuration error: {ve}")
    except ConnectionError as ce:
        logging.error(f"Database connection error: {ce}")
    except Exception as e:
        logging.error(f"An unexpected error occurred during schema creation: {e}", exc_info=True)
    finally:
        if db_engine:
            db_engine.dispose()
            logging.info("Database engine disposed.")