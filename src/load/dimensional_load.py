# /home/nicolas/Escritorio/proyecto/otra_prueba/src/load/dimensional_load.py
import logging
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy import text, engine # Importar engine para type hinting

logger = logging.getLogger(__name__)
# Ya no se necesita la constante SOURCE_TABLE aquí

# --- Funciones de Carga Dimensional Modificadas ---

def load_dim_host(engine: engine.Engine, source_table_name: str):
    """Carga datos únicos de hosts desde source_table_name a dim_host."""
    logger.info(f"Loading data into dim_host from {source_table_name}...")
    # Asegúrate que las columnas en el SELECT coincidan con tu tabla fuente limpia (en minúsculas)
    sql = text(f"""
        INSERT INTO dim_host (
            host_id, host_name, host_identity_verified, calculated_host_listings_count
        )
        SELECT DISTINCT
            ac.host_id,
            ac.host_name,
            (LOWER(ac.host_identity_verified) = 'verified'), -- Asume transformación a booleano
            ac.calculated_host_listings_count
        FROM {source_table_name} ac  -- Usa el parámetro
        WHERE ac.host_id IS NOT NULL
        ON CONFLICT (host_id) DO NOTHING; -- Asume que host_id es la clave única
    """)
    try:
        with engine.begin() as connection:
            result = connection.execute(sql)
            logger.info(f"dim_host loading complete. Rows affected (approx): {result.rowcount}")
    except SQLAlchemyError as e:
        logger.error(f"Error loading dim_host from {source_table_name}: {e}", exc_info=True)
        raise

def load_dim_spot_location(engine: engine.Engine, source_table_name: str):
    """Carga datos únicos de ubicación desde source_table_name a dim_spot_location."""
    logger.info(f"Loading data into dim_spot_location from {source_table_name}...")
    # Clave natural: combinación de columnas de ubicación
    natural_key_cols = ['neighbourhood_group', 'neighbourhood', 'lat', 'long', 'country', 'country_code']
    sql = text(f"""
        INSERT INTO dim_spot_location (
            neighbourhood_group, neighbourhood, lat, long, country, country_code
        )
        SELECT DISTINCT
            ac.neighbourhood_group, ac.neighbourhood, ac.lat, ac.long, ac.country, ac.country_code
        FROM {source_table_name} ac -- Usa el parámetro
        WHERE ac.neighbourhood_group IS NOT NULL AND ac.neighbourhood IS NOT NULL AND
              ac.lat IS NOT NULL AND ac.long IS NOT NULL AND ac.country IS NOT NULL AND
              ac.country_code IS NOT NULL
        ON CONFLICT (neighbourhood_group, neighbourhood, lat, long, country, country_code) DO NOTHING; -- Usa la NK para conflicto
    """)
    try:
        with engine.begin() as connection:
            result = connection.execute(sql)
            logger.info(f"dim_spot_location loading complete. Rows affected (approx): {result.rowcount}")
    except SQLAlchemyError as e:
        logger.error(f"Error loading dim_spot_location from {source_table_name}: {e}", exc_info=True)
        raise

def load_dim_property(engine: engine.Engine, source_table_name: str):
    """Carga datos únicos de propiedades desde source_table_name a dim_property."""
    logger.info(f"Loading data into dim_property from {source_table_name}...")
    # Clave natural: id de la propiedad en la fuente
    sql = text(f"""
        INSERT INTO dim_property (
            property_id, name, instant_bookable, cancellation_policy, room_type,
            construction_year, house_rules, license
        )
        SELECT DISTINCT
            ac.id, -- Usando 'id' de la tabla limpia como clave natural
            ac.name,
            CASE
                WHEN LOWER(ac.instant_bookable::text) = 'true' THEN TRUE
                WHEN LOWER(ac.instant_bookable::text) = 'false' THEN FALSE
                ELSE NULL -- O un valor por defecto si prefieres
            END,
            ac.cancellation_policy,
            ac.room_type,
            ac.construction_year, -- Asegúrate que esta columna exista en la tabla limpia
            ac.house_rules,       -- Asegúrate que esta columna exista
            ac.license            -- Asegúrate que esta columna exista
        FROM {source_table_name} ac -- Usa el parámetro
        WHERE ac.id IS NOT NULL
        ON CONFLICT (property_id) DO NOTHING; -- Usa property_id (que mapea a ac.id) para conflicto
    """)
    try:
        with engine.begin() as connection:
            result = connection.execute(sql)
            logger.info(f"dim_property loading complete. Rows affected (approx): {result.rowcount}")
    except SQLAlchemyError as e:
        logger.error(f"Error loading dim_property from {source_table_name}: {e}", exc_info=True)
        raise

def load_dim_last_review(engine: engine.Engine, source_table_name: str):
    """Carga fechas únicas de 'last_review' desde source_table_name a dim_last_review."""
    logger.info(f"Loading data into dim_last_review from {source_table_name}...")
    # Asume que 'last_review' en la tabla limpia es un INTEGER YYYYMMDD
    sql = text(f"""
        INSERT INTO dim_last_review (
            date_key, full_date, year, month, day
        )
        SELECT DISTINCT
            ac.last_review AS date_key,
            -- Intenta convertir YYYYMMDD a DATE, maneja errores si es posible
            -- TO_DATE puede variar según el dialecto SQL (esto es para PostgreSQL)
            -- Si last_review ya es DATE en la tabla limpia, simplifica esto.
            -- Si es INTEGER, necesitamos convertirlo.
            CASE
                WHEN ac.last_review IS NOT NULL AND ac.last_review >= 19000101 THEN -- Evitar fechas inválidas
                   TO_DATE(ac.last_review::text, 'YYYYMMDD')
                ELSE NULL -- O una fecha por defecto como '1900-01-01'
            END as full_date,
            CASE
                WHEN ac.last_review IS NOT NULL AND ac.last_review >= 19000101 THEN
                   CAST(SUBSTRING(ac.last_review::text FROM 1 FOR 4) AS INTEGER)
                ELSE NULL -- O 1900
            END AS year,
            CASE
                WHEN ac.last_review IS NOT NULL AND ac.last_review >= 19000101 THEN
                   CAST(SUBSTRING(ac.last_review::text FROM 5 FOR 2) AS INTEGER)
                ELSE NULL -- O 1
            END AS month,
            CASE
                WHEN ac.last_review IS NOT NULL AND ac.last_review >= 19000101 THEN
                   CAST(SUBSTRING(ac.last_review::text FROM 7 FOR 2) AS INTEGER)
                ELSE NULL -- O 1
            END AS day
        FROM {source_table_name} ac -- Usa el parámetro
        WHERE ac.last_review IS NOT NULL -- Considera filtrar fechas inválidas aquí también si es necesario
        ON CONFLICT (date_key) DO NOTHING; -- Usa date_key (que mapea a ac.last_review) para conflicto
    """)
    try:
        with engine.begin() as connection:
            result = connection.execute(sql)
            logger.info(f"dim_last_review loading complete. Rows affected (approx): {result.rowcount}")
    except SQLAlchemyError as e:
        logger.error(f"Error loading dim_last_review from {source_table_name}: {e}", exc_info=True)
        raise

def load_fact_publication(engine: engine.Engine, source_table_name: str):
    """
    Carga datos en la tabla de hechos fact_publication desde source_table_name,
    uniendo con las dimensiones para obtener las claves foráneas.
    TRUNCA la tabla de hechos antes de insertar.
    """
    logger.info(f"Loading data into fact_publication from {source_table_name}...")
    # 1. Truncar la tabla de hechos (esto la hace no incremental, sino de carga completa)
    try:
        with engine.begin() as connection:
            logger.warning(f"Truncating fact_publication table before loading from {source_table_name}.")
            connection.execute(text("TRUNCATE TABLE fact_publication;"))
            logger.info("fact_publication table truncated.")
    except SQLAlchemyError as e:
        logger.error(f"Error truncating fact_publication: {e}", exc_info=True)
        raise

    # 2. Insertar datos haciendo JOIN con las dimensiones
    # Asegúrate que las columnas seleccionadas de 'ac' y los JOINs sean correctos
    # y que las claves foráneas (fk_*) coincidan con las claves primarias de las dimensiones.
    sql = text(f"""
        INSERT INTO fact_publication (
            fk_property, fk_host, fk_location, fk_date,
            price, service_fee, minimum_nights, number_of_reviews,
            reviews_per_month, review_rate_number, availability_365
        )
        SELECT
            dp.property_key, -- FK a dim_property
            dh.host_key,     -- FK a dim_host
            dl.location_key, -- FK a dim_spot_location
            dlr.date_key,    -- FK a dim_last_review
            ac.price,
            -- Asegurar conversión a DECIMAL si es necesario
            CAST(ac.service_fee AS DECIMAL(10, 2)),
            ac.minimum_nights,
            ac.number_of_reviews,
            ac.reviews_per_month,
            ac.review_rate_number,
            ac.availability_365
        FROM {source_table_name} ac -- Usa el parámetro
        -- JOIN con dimensiones para buscar las claves subrogadas (keys)
        LEFT JOIN dim_property dp ON ac.id = dp.property_id -- JOIN por clave natural
        LEFT JOIN dim_host dh ON ac.host_id = dh.host_id    -- JOIN por clave natural
        LEFT JOIN dim_spot_location dl ON ac.neighbourhood_group = dl.neighbourhood_group
                                     AND ac.neighbourhood = dl.neighbourhood
                                     AND ac.lat = dl.lat -- Cuidado con JOINs en floats/decimals, puede necesitar redondeo o tolerancia
                                     AND ac.long = dl.long
                                     AND ac.country = dl.country
                                     AND ac.country_code = dl.country_code
        LEFT JOIN dim_last_review dlr ON ac.last_review = dlr.date_key -- JOIN por clave natural (fecha YYYYMMDD)
        -- Es importante añadir WHERE clauses si los JOINs pueden fallar, para evitar insertar FKs nulas
        -- aunque las FK constraints deberían fallar si intentas insertar NULLs (si son NOT NULL)
        WHERE dp.property_key IS NOT NULL -- Asegura que la propiedad existe en la dimensión
          AND dh.host_key IS NOT NULL     -- Asegura que el host existe
          AND dl.location_key IS NOT NULL -- Asegura que la ubicación existe
          AND dlr.date_key IS NOT NULL;   -- Asegura que la fecha de revisión existe
          -- Nota: El LEFT JOIN con WHERE IS NOT NULL actúa como un INNER JOIN.
          -- Si quieres cargar hechos incluso si falta una dimensión (con FK nula), quita el WHERE
          -- y asegúrate que las columnas FK en fact_publication permitan NULLs.
    """)
    try:
        with engine.begin() as connection:
            result = connection.execute(sql)
            logger.info(f"fact_publication loading complete. Rows affected (approx): {result.rowcount}")
    except SQLAlchemyError as e:
        logger.error(f"Error loading fact_publication from {source_table_name}: {e}", exc_info=True)
        raise

# --- Orquestador de Carga Dimensional Modificado ---

def load_dimensional_data(engine: engine.Engine, source_table_name: str):
    """
    Orquesta la carga de todas las tablas dimensionales y de hechos.
    Recibe el engine de SQLAlchemy y el nombre de la tabla fuente limpia.
    """
    logger.info(f"Starting dimensional data loading process from source '{source_table_name}'...")
    try:
        # Llamar a cada función de carga pasando el engine y el nombre de la tabla fuente
        load_dim_host(engine, source_table_name)
        load_dim_spot_location(engine, source_table_name)
        load_dim_property(engine, source_table_name)
        load_dim_last_review(engine, source_table_name)
        # Cargar la tabla de hechos al final, después de que las dimensiones estén pobladas
        load_fact_publication(engine, source_table_name)

        logger.info(f"Dimensional data loading process from '{source_table_name}' completed successfully.")
        return True
    except Exception as e:
        # El error específico ya debería haber sido logueado por la función que falló
        logger.error(f"Dimensional data loading process failed (source: '{source_table_name}'). Error: {e}")
        # No relanzar aquí necesariamente, la función de tarea en task_etl.py ya lo hará.
        return False