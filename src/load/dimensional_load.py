#/home/nicolas/Escritorio/proyecto ETL/develop/src/load/dimensional_load.py
import logging
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy import text

logger = logging.getLogger(__name__)
SOURCE_TABLE = "airbnb_cleaned" # Ya debería estar en minúsculas aquí

def load_dim_host(engine):
    logger.info(f"Loading data into dim_host from {SOURCE_TABLE}...")
    # --- CORREGIDO: snake_case en INSERT y ON CONFLICT ---
    sql = text(f"""
        INSERT INTO dim_host (
            host_id, host_name, host_identity_verified, calculated_host_listings_count
        )
        SELECT DISTINCT
            ac.host_id,
            ac.host_name,
            (LOWER(ac.host_identity_verified) = 'verified'),
            ac.calculated_host_listings_count
        FROM {SOURCE_TABLE} ac
        WHERE ac.host_id IS NOT NULL
        ON CONFLICT (host_id) DO NOTHING;
    """)
    try:
        with engine.begin() as connection:
            result = connection.execute(sql)
            logger.info(f"dim_host loading complete. Rows affected (approx): {result.rowcount}")
    except SQLAlchemyError as e:
        logger.error(f"Error loading dim_host: {e}", exc_info=True)
        raise

def load_dim_spot_location(engine):
    logger.info(f"Loading data into dim_spot_location from {SOURCE_TABLE}...")
    # --- CORREGIDO: snake_case en ON CONFLICT ---
    sql = text(f"""
        INSERT INTO dim_spot_location (
            neighbourhood_group, neighbourhood, lat, long, country, country_code
        )
        SELECT DISTINCT
            ac.neighbourhood_group, ac.neighbourhood, ac.lat, ac.long, ac.country, ac.country_code
        FROM {SOURCE_TABLE} ac
        WHERE ac.neighbourhood_group IS NOT NULL AND ac.neighbourhood IS NOT NULL AND
              ac.lat IS NOT NULL AND ac.long IS NOT NULL AND ac.country IS NOT NULL AND
              ac.country_code IS NOT NULL
        ON CONFLICT (neighbourhood_group, neighbourhood, lat, long, country, country_code) DO NOTHING;
    """)
    try:
        with engine.begin() as connection:
            result = connection.execute(sql)
            logger.info(f"dim_spot_location loading complete. Rows affected (approx): {result.rowcount}")
    except SQLAlchemyError as e:
        logger.error(f"Error loading dim_spot_location: {e}", exc_info=True)
        raise

def load_dim_property(engine):
    logger.info(f"Loading data into dim_property from {SOURCE_TABLE}...")
    # --- CORREGIDO: snake_case en INSERT y ON CONFLICT ---
    # --- CORREGIDO: nombre columna fuente 'construction_year' ---
    sql = text(f"""
        INSERT INTO dim_property (
            property_id, name, instant_bookable, cancellation_policy, room_type,
            construction_year, house_rules, license
        )
        SELECT DISTINCT
            ac.id, -- Clave natural de la fuente
            ac.name,
            CASE LOWER(ac.instant_bookable::text) WHEN 'true' THEN TRUE ELSE FALSE END,
            ac.cancellation_policy,
            ac.room_type,
            ac.construction_year, -- Fuente ya debería estar en minúsculas por limpieza
            ac.house_rules,
            ac.license
        FROM {SOURCE_TABLE} ac
        WHERE ac.id IS NOT NULL
        ON CONFLICT (property_id) DO NOTHING;
    """)
    try:
        with engine.begin() as connection:
            result = connection.execute(sql)
            logger.info(f"dim_property loading complete. Rows affected (approx): {result.rowcount}")
    except SQLAlchemyError as e:
        logger.error(f"Error loading dim_property: {e}", exc_info=True)
        raise

def load_dim_last_review(engine):
    logger.info(f"Loading data into dim_last_review from {SOURCE_TABLE}...")
    # --- CORREGIDO: snake_case en INSERT, Alias, ON CONFLICT ---
    sql = text(f"""
        INSERT INTO dim_last_review (
            date_key, full_date, year, month, day
        )
        SELECT DISTINCT
            ac.last_review AS date_key, -- Alias a snake_case
            TO_DATE(ac.last_review::text, 'YYYYMMDD') as full_date,
            CAST(SUBSTRING(ac.last_review::text FROM 1 FOR 4) AS INTEGER) AS year,
            CAST(SUBSTRING(ac.last_review::text FROM 5 FOR 2) AS INTEGER) AS month,
            CAST(SUBSTRING(ac.last_review::text FROM 7 FOR 2) AS INTEGER) AS day
        FROM {SOURCE_TABLE} ac
        WHERE ac.last_review IS NOT NULL AND ac.last_review >= 19000101
        ON CONFLICT (date_key) DO NOTHING; -- Conflict target a snake_case
    """)
    try:
        with engine.begin() as connection:
            result = connection.execute(sql)
            logger.info(f"dim_last_review loading complete. Rows affected (approx): {result.rowcount}")
    except SQLAlchemyError as e:
        logger.error(f"Error loading dim_last_review: {e}", exc_info=True)
        raise

def load_fact_publication(engine):
    logger.info(f"Loading data into fact_publication from {SOURCE_TABLE}...")
    # --- TRUNCATE sin cambios ---
    try:
        with engine.begin() as connection:
            logger.warning("Truncating fact_publication table before loading.")
            connection.execute(text("TRUNCATE TABLE fact_publication;"))
    except SQLAlchemyError as e:
        logger.error(f"Error truncating fact_publication: {e}", exc_info=True)
        raise

    # --- CORREGIDO: snake_case en INSERT y en SELECT de Keys ---
    # --- CORREGIDO: snake_case en JOINs a Keys de dimensiones ---
    sql = text(f"""
        INSERT INTO fact_publication (
            fk_property, fk_host, fk_location, fk_date,
            price, service_fee, minimum_nights, number_of_reviews,
            reviews_per_month, review_rate_number, availability_365
        )
        SELECT
            dp.property_key, -- Seleccionar snake_case
            dh.host_key,     -- Seleccionar snake_case
            dl.location_key, -- Seleccionar snake_case
            dlr.date_key,    -- Seleccionar snake_case
            ac.price,
            CAST(ac.service_fee AS DECIMAL(10, 2)),
            ac.minimum_nights,
            ac.number_of_reviews,
            ac.reviews_per_month,
            ac.review_rate_number,
            ac.availability_365
        FROM {SOURCE_TABLE} ac
        LEFT JOIN dim_property dp ON ac.id = dp.property_id -- JOIN con clave natural (id)
        LEFT JOIN dim_host dh ON ac.host_id = dh.host_id       -- JOIN con clave natural (host_id)
        LEFT JOIN dim_spot_location dl ON ac.neighbourhood_group = dl.neighbourhood_group -- JOIN con NK compuesta
                                     AND ac.neighbourhood = dl.neighbourhood
                                     AND ac.lat = dl.lat
                                     AND ac.long = dl.long
                                     AND ac.country = dl.country
                                     AND ac.country_code = dl.country_code
        LEFT JOIN dim_last_review dlr ON ac.last_review = dlr.date_key -- JOIN con NK (fecha int)
        WHERE dp.property_key IS NOT NULL -- Verificar existencia usando la PK de la dimensión
          AND dh.host_key IS NOT NULL
          AND dl.location_key IS NOT NULL
          AND dlr.date_key IS NOT NULL;
    """)
    try:
        with engine.begin() as connection:
            result = connection.execute(sql)
            logger.info(f"fact_publication loading complete. Rows affected (approx): {result.rowcount}")
    except SQLAlchemyError as e:
        logger.error(f"Error loading fact_publication: {e}", exc_info=True)
        raise

# --- load_dimensional_data (sin cambios lógicos) ---
def load_dimensional_data(engine):
    logger.info("Starting dimensional data loading process...")
    try:
        load_dim_host(engine)
        load_dim_spot_location(engine)
        load_dim_property(engine)
        load_dim_last_review(engine) # Ahora usa snake_case
        load_fact_publication(engine)
        logger.info("Dimensional data loading process completed successfully.")
        return True
    except Exception as e:
        logger.error(f"Dimensional data loading process failed: {e}")
        return False