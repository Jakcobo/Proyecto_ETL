import logging
from sqlalchemy import create_engine, text
import json

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

class ModelDimensional:
    def __init__(self, engine):
        self.engine = engine

    def migrate_to_dim_host(self):
        """Migra los datos a la tabla dim_host."""
        with self.engine.begin() as connection:
            connection.execute(text("""
                INSERT INTO dim_host (host_id, host_name, host_since, host_location, host_response_time, host_is_superhost)
                SELECT DISTINCT host_id, host_name, host_since, host_location, host_response_time, host_is_superhost
                FROM airbnb_EDA
                WHERE host_id IS NOT NULL;
            """))
            logging.info("Datos migrados a la tabla 'dim_host'.")

    def migrate_to_dim_property(self):
        """Migra los datos a la tabla dim_property."""
        with self.engine.begin() as connection:
            connection.execute(text("""
                INSERT INTO dim_property (property_id, name, neighbourhood_group, neighbourhood, room_type, price, minimum_nights)
                SELECT DISTINCT id AS property_id, name, neighbourhood_group, neighbourhood, room_type, price, minimum_nights
                FROM airbnb_EDA
                WHERE id IS NOT NULL;
            """))
            logging.info("Datos migrados a la tabla 'dim_property'.")

    def migrate_to_fact_reservations(self):
        """Migra los datos a la tabla fact_reservations."""
        with self.engine.begin() as connection:
            connection.execute(text("""
                INSERT INTO fact_reservations (property_id, host_id, number_of_reviews, last_review, reviews_per_month, availability_365)
                SELECT id AS property_id, host_id, number_of_reviews, last_review, reviews_per_month, availability_365
                FROM airbnb_EDA
                WHERE id IS NOT NULL AND host_id IS NOT NULL;
            """))
            logging.info("Datos migrados a la tabla 'fact_reservations'.")

    def execute_migration(self):
        """Ejecuta todas las migraciones a las tablas del modelo dimensional."""
        self.migrate_to_dim_host()
        self.migrate_to_dim_property()
        self.migrate_to_fact_reservations()
        logging.info("Migración a las tablas del modelo dimensional completada.")


if __name__ == "__main__":
    try:

        with open("../credentials.json") as f:
            creds = json.load(f)

        engine = create_engine(
            f'postgresql://{creds["user"]}:{creds["password"]}@{creds["host"]}:{creds["port"]}/{creds["database"]}'
        )

        model = ModelDimensional(engine)
        model.execute_migration()

    except Exception as e:
        logging.error(f"Error durante la migración: {e}")