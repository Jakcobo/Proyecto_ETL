import pandas as pd
import logging
from sqlalchemy import create_engine
from database.db import get_db_engine, disposing_engine, load_data, load_clean_data, creating_engine

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(message)s", datefmt="%d/%m/%Y %I:%M:%S %p")
logger = logging.getLogger(__name__)

from sqlalchemy import inspect, text

def exe_load_data(df):
    logging.info("Starting to load the data.")
    engine = None

    try:
        # Obtener el engine
        engine = get_db_engine("airbnb")
        table_name = "airbnb_data"

        # Convertir a DataFrame si aún no lo es
        if not isinstance(df, pd.DataFrame):
            df = pd.DataFrame(df)

        # Inspeccionar si la tabla ya existe
        inspector = inspect(engine)
        if table_name in inspector.get_table_names():
            with engine.connect() as conn:
                conn.execute(text(f"DROP TABLE IF EXISTS {table_name}"))
                logging.info(f"Existing table '{table_name}' dropped.")

        # Cargar los datos con if_exists="append" ahora que no existe la tabla
        df.to_sql(
            table_name,
            con=engine,         # pasar engine directamente
            if_exists="append",  # ya nos encargamos de borrar antes
            index=False,
            method='multi'
        )

        logger.info("Data loaded successfully")
        return True

    except Exception as e:
        logger.error(f"Error loading data: {e}")
        raise

    finally:
        if engine:
            engine.dispose()

# # Cargar los datos usando raw_connection() para que pandas tenga un cursor DBAPI2
# +    raw_conn = engine.raw_connection()
# +    try:
# +        df.to_sql(
# +            name=table_name,
# +            con=raw_conn,      # <-- aquí pasamos la conexión psycopg2 pura
# +            if_exists="append",
# +            index=False,
# +            method='multi'
# +        )
# +        raw_conn.commit()
# +        logging.info("Data loaded successfully")
# +        return True
# +    finally:
# +        raw_conn.close()
 