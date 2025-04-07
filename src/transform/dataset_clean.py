import logging
import pandas as pd
from sqlalchemy import create_engine, text
from dotenv import load_dotenv
import os
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

class DatasetCleaner:
    def __init__(self, engine):
        self.engine = engine

    def create_eda_table(self):
        """Creates the airbnb_EDA table from airbnb_data."""
        with self.engine.begin() as connection:
            result = connection.execute(text("""
                SELECT COUNT(*) 
                FROM information_schema.tables 
                WHERE table_name = 'airbnb_data';
            """))
            if result.scalar() != 1:
                logging.error("The table 'airbnb_data' does not exist.")
                return False

            connection.execute(text("DROP TABLE IF EXISTS airbnb_EDA;"))
            connection.execute(text("""
                CREATE TABLE airbnb_EDA AS 
                SELECT * FROM airbnb_data;
            """))
            logging.info("Table 'airbnb_EDA' created successfully.")
            return True

    def rename_columns_with_spaces(self):
        """Renames columns with spaces in the airbnb_EDA table."""
        with self.engine.begin() as connection:
            query = text("""
                SELECT column_name 
                FROM information_schema.columns 
                WHERE table_name = 'airbnb_EDA' AND column_name LIKE '% %';
            """)
            result = connection.execute(query)
            columns_to_rename = [row[0] for row in result]

            for old_name in columns_to_rename:
                new_name = old_name.replace(' ', '_')
                connection.execute(text(f"""
                    ALTER TABLE airbnb_EDA RENAME COLUMN "{old_name}" TO {new_name};
                """))
                logging.info(f"Renamed column: '{old_name}' -> '{new_name}'")

    def drop_unnecessary_columns(self):
        """Drops unnecessary columns from the airbnb_EDA table."""
        with self.engine.begin() as connection:
            connection.execute(text("""
                ALTER TABLE airbnb_EDA
                DROP COLUMN IF EXISTS host_name,
                DROP COLUMN IF EXISTS country_code,
                DROP COLUMN IF EXISTS country,
                DROP COLUMN IF EXISTS house_rules;
            """))
            logging.info("Unnecessary columns dropped.")

    def transform_last_review(self):
        """Transforms the last_review column to INTEGER format (YYYYMMDD)."""
        with self.engine.begin() as connection:
            connection.execute(text("""
                ALTER TABLE airbnb_EDA ADD COLUMN IF NOT EXISTS temp_last_review DATE;
            """))
            connection.execute(text("""
                UPDATE airbnb_EDA
                SET temp_last_review = 
                    CASE
                        WHEN last_review ~ '^\d{4}-\d{2}-\d{2}$' THEN TO_DATE(last_review, 'YYYY-MM-DD')
                        WHEN last_review ~ '^\d{1,2}/\d{1,2}/\d{4}$' THEN TO_DATE(
                            LPAD(SPLIT_PART(last_review, '/', 1), 2, '0') || '/' ||
                            LPAD(SPLIT_PART(last_review, '/', 2), 2, '0') || '/' ||
                            SPLIT_PART(last_review, '/', 3), 'MM/DD/YYYY')
                        ELSE NULL
                    END;
            """))
            connection.execute(text("""
                UPDATE airbnb_EDA
                SET temp_last_review = '1900-01-01'
                WHERE temp_last_review IS NULL;
            """))
            connection.execute(text("""
                ALTER TABLE airbnb_EDA DROP COLUMN last_review;
                ALTER TABLE airbnb_EDA RENAME COLUMN temp_last_review TO last_review;
            """))
            connection.execute(text("""
                ALTER TABLE airbnb_EDA
                ALTER COLUMN last_review TYPE INTEGER
                USING (EXTRACT(YEAR FROM last_review) * 10000 +
                       EXTRACT(MONTH FROM last_review) * 100 +
                       EXTRACT(DAY FROM last_review));
            """))
            logging.info("Column 'last_review' transformed to INTEGER (AAAAMMDD).")

    def correct_neighbourhood_group(self):
        """Corrects values in the neighbourhood_group column."""
        with self.engine.begin() as connection:
            connection.execute(text("""
                UPDATE airbnb_EDA
                SET neighbourhood_group = CASE
                    WHEN neighbourhood_group = 'brookln' THEN 'Brooklyn'
                    WHEN neighbourhood_group = 'manhatan' THEN 'Manhattan'
                    ELSE neighbourhood_group
                END
                WHERE neighbourhood_group IN ('brookln', 'manhatan');
            """))
            logging.info("Values corrected in the 'neighbourhood_group' column.")

    def execute_transformations(self):
        """Executes all transformations on the airbnb_EDA table."""
        if self.create_eda_table():
            self.rename_columns_with_spaces()
            self.drop_unnecessary_columns()
            self.transform_last_review()
            self.correct_neighbourhood_group()
            logging.info("Transformations completed successfully.")



if __name__ == "__main__":
    try:
        # Cargar las variables de entorno desde el archivo .env
        load_dotenv(os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "env", ".env")))

        # Obtener las credenciales desde las variables de entorno
        user = os.getenv("POSTGRES_USER")
        password = os.getenv("POSTGRES_PASSWORD")
        host = os.getenv("POSTGRES_HOST")
        port = os.getenv("POSTGRES_PORT")
        database = os.getenv("POSTGRES_DATABASE")

        if not all([user, password, host, port, database]):
            raise ValueError("Faltan variables de entorno necesarias para la conexión a la base de datos.")

        engine = create_engine(f'postgresql://{user}:{password}@{host}:{port}/{database}')

        cleaner = DatasetCleaner(engine)
        cleaner.execute_transformations()

    except Exception as e:
        logging.error(f"Error executing transformations: {e}")