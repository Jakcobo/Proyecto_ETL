import os
import logging
import pandas as pd
from sqlalchemy.exc import SQLAlchemyError
from database.db import get_db_engine

# Basic logger configuration
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
    datefmt="%m/%d/%Y %I:%M:%S %p"
)
logger = logging.getLogger(__name__)

def load_dim_host(df, engine):
    """
    Loads the host dimension into the 'dim_host' table.
    The following columns are expected in the DataFrame:
      - host_id
      - host_name
      - host_since
      - host_location
      - host_response_time
      - host_is_superhost
      - host_identity_verified
      - calculated_host_listings_count
    """
    try:
        df_host = df[['host_id', 'host_name', 'host_since', 'host_location', 
                      'host_response_time', 'host_is_superhost', 'host_identity_verified', 
                      'calculated_host_listings_count']]
        # "append" is used to avoid overwriting existing data
        df_host.to_sql('dim_host', engine, if_exists='append', index=False)
        logger.info("Dimension 'dim_host' loaded successfully.")
    except Exception as e:
        logger.error(f"Error loading dim_host: {e}", exc_info=True)
        raise

def load_dim_spot_location(df, engine):
    """
    Loads the location dimension into the 'dim_spot_location' table.
    The following columns are expected in the DataFrame:
      - location_key
      - neighbourhood_group
      - neighbourhood
      - lat
      - long
      - country
      - country_code
    """
    try:
        df_location = df[['location_key', 'neighbourhood_group', 'neighbourhood', 
                          'lat', 'long', 'country', 'country_code']]
        df_location.to_sql('dim_spot_location', engine, if_exists='append', index=False)
        logger.info("Dimension 'dim_spot_location' loaded successfully.")
    except Exception as e:
        logger.error(f"Error loading dim_spot_location: {e}", exc_info=True)
        raise

def load_dim_property(df, engine):
    """
    Loads the property dimension into the 'dim_property' table.
    The following columns are expected in the DataFrame:
      - Property_Key
      - name
      - instant_bookable
      - cancellation_policy
      - room_type
      - construction_year
      - house_rules
      - license
    """
    try:
        df_property = df[['Property_Key', 'name', 'instant_bookable', 
                          'cancellation_policy', 'room_type', 'construction_year', 
                          'house_rules', 'license']]
        df_property.to_sql('dim_property', engine, if_exists='append', index=False)
        logger.info("Dimension 'dim_property' loaded successfully.")
    except Exception as e:
        logger.error(f"Error loading dim_property: {e}", exc_info=True)
        raise

def load_dim_last_review(df, engine):
    """
    Loads the last review date dimension into the 'dim_last_review' table.
    The following columns are expected in the DataFrame:
      - Date_Key
      - full_date
      - year
      - month
      - day
    """
    try:
        df_last_review = df[['Date_Key', 'full_date', 'year', 'month', 'day']]
        df_last_review.to_sql('dim_last_review', engine, if_exists='append', index=False)
        logger.info("Dimension 'dim_last_review' loaded successfully.")
    except Exception as e:
        logger.error(f"Error loading dim_last_review: {e}", exc_info=True)
        raise

def load_fact_publication(df, engine):
    """
    Loads the fact table into the 'fact_publication' table.
    The following columns are expected in the DataFrame:
      - Publication_ID
      - FK_Host
      - FK_Location
      - FK_Property
      - FK_Date
      - price
      - service_fee
      - minimum_nights
      - number_of_reviews
      - reviews_per_month
      - review_rate_number
      - availability_365
    """
    try:
        fact_columns = ['Publication_ID', 'FK_Host', 'FK_Location', 'FK_Property', 
                        'FK_Date', 'price', 'service_fee', 'minimum_nights', 
                        'number_of_reviews', 'reviews_per_month', 'review_rate_number', 
                        'availability_365']
        df_fact = df[fact_columns]
        df_fact.to_sql('fact_publication', engine, if_exists='append', index=False)
        logger.info("Fact table 'fact_publication' loaded successfully.")
    except Exception as e:
        logger.error(f"Error loading fact_publication: {e}", exc_info=True)
        raise

def load_all_tables(df):
    """
    Orchestrator function: gets the engine, executes the load process for each table, 
    and ensures the connection is released at the end.
    """
    logger.info("Starting the dimensional model load process...")
    engine = None
    try:
        engine = get_db_engine()
        load_dim_host(df, engine)
        load_dim_spot_location(df, engine)
        load_dim_property(df, engine)
        load_dim_last_review(df, engine)
        load_fact_publication(df, engine)
        logger.info("All tables loaded successfully.")
    except SQLAlchemyError as sql_e:
        logger.error("Database operation error.", exc_info=True)
        raise
    except Exception as e:
        logger.error("Error in the load process.", exc_info=True)
        raise
    finally:
        if engine:
            engine.dispose()
            logger.info("Database connection closed.")

if __name__ == "__main__":
    # For this example, it is assumed that the resulting DataFrame from the transformation is saved in a CSV.
    # In Airflow, the path can be received or even the DataFrame can be passed between tasks.
    try:
        # Define the file path with the transformed data. This can be defined via environment variable.
        df_path = os.getenv('TRANSFORMED_DF_PATH', 'transformed_data.csv')
        logger.info(f"Reading transformed data from {df_path}...")
        df = pd.read_csv(df_path)

        # Perform, if necessary, conversions or adjustments in the DataFrame (e.g., parsing dates)
        # df['host_since'] = pd.to_datetime(df['host_since'])
        # df['full_date'] = pd.to_datetime(df['full_date'])

        load_all_tables(df)
    except Exception as e:
        logger.error("Error executing the load script.", exc_info=True)
