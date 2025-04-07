import os
import logging
from dotenv import load_dotenv
from sqlalchemy import (
    create_engine, MetaData, Table, Column, Integer, String, Boolean,
    Float, DECIMAL, Date, BIGINT, ForeignKeyConstraint, inspect
)
from sqlalchemy_utils import database_exists, create_database

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s", datefmt="%m/%d/%Y %I:%M:%S %p")

# --- Funciones de Configuración y Conexión ---
def load_environment():
    """Carga las variables de entorno desde un archivo .env."""
    try:
        # Intenta buscar el .env dos niveles arriba si __file__ está definido
        # o en el directorio actual/superior si se ejecuta directamente.
        try:
            env_path = os.path.join(os.path.dirname(__file__), '..', 'env', '.env')
            if not os.path.exists(env_path):
                 env_path = os.path.join(os.getcwd(), '..', 'env', '.env') # Intenta un nivel arriba del CWD
            if not os.path.exists(env_path):
                 env_path = os.path.join(os.getcwd(), '.env')
        except NameError:
             env_path = os.path.join(os.getcwd(), '.env')

        if os.path.exists(env_path):
            load_dotenv(dotenv_path=env_path, override=True)
            logging.info(f"Archivo .env cargado desde: {env_path}")
        else:
            logging.warning(f"Archivo .env no encontrado en las rutas buscadas. Usando variables de entorno existentes si están disponibles.")
            load_dotenv(override=True) # Intenta cargar desde variables de entorno existentes

    except Exception as e:
        logging.error(f"Error cargando el archivo de credenciales: {e}")
        raise

def get_db_connection_params():
    """Obtiene los parámetros de conexión a la base de datos desde las variables de entorno."""
    params = {
        'user':     os.getenv("USER"),
        'password': os.getenv("PASSWORD"),
        'host':     os.getenv("HOST"),
        'port':     os.getenv("PORT"),
        'database': os.getenv("DATABASE")
    }

    missing = [k for k, v in params.items() if v is None]
    if missing:
        logging.error(f"Faltan parámetros de base de datos en las variables de entorno: {missing}")
        raise ValueError(f"Missing required database parameters: {missing}")

    return params

def creating_engine(database=None):
    """Crea y retorna un engine de SQLAlchemy."""
    try:
        params = get_db_connection_params()
        # Permite sobreescribir la base de datos si se pasa como argumento
        db_name = database if database else params['database']
        if not db_name:
             raise ValueError("El nombre de la base de datos no puede estar vacío.")

        # URL de conexión sin el nombre de la base de datos para verificar/crear
        server_url = f"postgresql://{params['user']}:{params['password']}@{params['host']}:{params['port']}/"
        db_url = f"{server_url}{db_name}"

        # Crear la base de datos si no existe (conectándose al servidor sin especificar DB)
        temp_engine = create_engine(server_url)
        if not database_exists(db_url):
             with temp_engine.connect() as connection:
                 # PostgreSQL requiere que CREATE DATABASE no esté en una transacción
                 connection.execution_options(isolation_level="AUTOCOMMIT").execute(f"CREATE DATABASE {db_name}")
                 logging.info(f"Base de datos '{db_name}' creada.")
        temp_engine.dispose()

        # Crear el engine final conectado a la base de datos específica
        engine = create_engine(db_url)
        logging.info(f"Engine creado. Conectado a la base de datos '{db_name}'.")
        return engine

    except Exception as e:
        logging.error(f"Error creando el engine de base de datos: {e}")
        raise

def disposing_engine(engine):
    """Libera los recursos del engine."""
    if engine:
        engine.dispose()
        logging.info("Engine dispuesto (recursos liberados).")

# --- Funciones para Definir las Tablas del Modelo Dimensional ---

def define_dim_host(metadata):
    """Define la tabla dim_host en los metadatos."""
    logging.info("Definiendo tabla: dim_host")
    return Table('dim_host', metadata,
        Column('Host_Key', Integer, primary_key=True, comment="Clave subrogada para la dimensión host."),
        Column('host_id', BIGINT, comment="ID original del host."),
        Column('host_name', String(100), comment="Nombre del host."),
        Column('host_identity_verified', Boolean, comment="Indica si la identidad del host está verificada."),
        Column('calculated_host_listings_count', Integer, comment="Número de publicaciones calculadas para el host.")
    )

def define_dim_spot_location(metadata):
    """Define la tabla dim_spot_location en los metadatos."""
    logging.info("Definiendo tabla: dim_spot_location")
    return Table('dim_spot_location', metadata,
        Column('Location_Key', Integer, primary_key=True, comment="Clave subrogada para la dimensión de ubicación."),
        Column('neighbourhood_group', String(50), comment="Grupo de vecindario."),
        Column('neighbourhood', String(100), comment="Vecindario específico."),
        Column('lat', DECIMAL(9, 6), comment="Latitud de la ubicación."), # DECIMAL es más preciso para coordenadas que Float
        Column('long', DECIMAL(9, 6), comment="Longitud de la ubicación."),
        Column('country', String(100), comment="País."),
        Column('country_code', String(3), comment="Código de país (ej. 'ESP').")
    )

def define_dim_property(metadata):
    """Define la tabla dim_property en los metadatos."""
    logging.info("Definiendo tabla: dim_property")
    return Table('dim_property', metadata,
        Column('Property_Key', Integer, primary_key=True, comment="Clave subrogada para la dimensión de propiedad."),
        Column('name', String(100), comment="Nombre o descripción de la propiedad."),
        Column('instant_bookable', Boolean, comment="Indica si la propiedad se puede reservar instantáneamente."),
        Column('cancellation_policy', String(100), comment="Política de cancelación."),
        Column('room_type', String(50), comment="Tipo de habitación o propiedad."),
        Column('construction_year', Integer, comment="Año de construcción."),
        Column('house_rules', String(250), comment="Reglas de la casa."),
        Column('license', String(100), comment="Número de licencia, si aplica.")
    )

def define_dim_last_review(metadata):
    """Define la tabla dim_last_review en los metadatos."""
    logging.info("Definiendo tabla: dim_last_review")
    return Table('dim_last_review', metadata,
        Column('Date_Key', Integer, primary_key=True, comment="Clave subrogada para la dimensión de tiempo (basada en la última reseña)."),
        Column('full_date', Date, comment="Fecha completa de la última reseña."),
        Column('year', Integer, comment="Año de la última reseña."),
        Column('month', Integer, comment="Mes de la última reseña."),
        Column('day', Integer, comment="Día de la última reseña.")
    )

def define_fact_publication(metadata):
    """Define la tabla fact_publication en los metadatos, incluyendo FKs."""
    logging.info("Definiendo tabla: fact_publication")
    return Table('fact_publication', metadata,
        Column('Publication_ID', Integer, primary_key=True, comment="Clave primaria de la tabla de hechos (puede ser ID original o subrogada)."),
        Column('FK_Host', Integer, comment="Clave foránea a dim_host."),
        Column('FK_Location', Integer, comment="Clave foránea a dim_spot_location."),
        Column('FK_Property', Integer, comment="Clave foránea a dim_property."),
        Column('FK_Date', Integer, comment="Clave foránea a dim_last_review."),
        Column('price', Float, comment="Precio por noche."), # Float es común para precios, pero DECIMAL podría ser mejor si se requiere precisión exacta.
        Column('service_fee', DECIMAL(10, 2), comment="Tarifa de servicio."),
        Column('minimum_nights', Integer, comment="Mínimo de noches requeridas."),
        Column('number_of_reviews', Integer, comment="Número total de reseñas."),
        Column('reviews_per_month', Float, comment="Promedio de reseñas por mes."),
        Column('review_rate_number', Integer, comment="Puntuación de la reseña (escala?)."), # Asumiendo que es un número entero
        Column('availability_365', Integer, comment="Número de días disponibles en los próximos 365 días."),

        # Definición de las restricciones de clave foránea
        ForeignKeyConstraint(['FK_Host'], ['dim_host.Host_Key']),
        ForeignKeyConstraint(['FK_Location'], ['dim_spot_location.Location_Key']),
        ForeignKeyConstraint(['FK_Property'], ['dim_property.Property_Key']),
        ForeignKeyConstraint(['FK_Date'], ['dim_last_review.Date_Key'])
    )

def create_dimensional_model_tables(engine):
    """
    Define y crea todas las tablas del modelo dimensional si no existen.
    """
    metadata = MetaData()

    # 1. Definir todas las tablas asociándolas a los metadatos
    # El orden de definición no importa aquí, SQLAlchemy resolverá dependencias
    # al crear con metadata.create_all()
    define_dim_host(metadata)
    define_dim_spot_location(metadata)
    define_dim_property(metadata)
    define_dim_last_review(metadata)
    define_fact_publication(metadata) # Fact table definida después de las dimensiones

    # 2. Crear todas las tablas definidas en los metadatos
    try:
        logging.info("Intentando crear todas las tablas definidas en los metadatos...")
        # checkfirst=True evita errores si las tablas ya existen
        metadata.create_all(engine, checkfirst=True)
        logging.info("Verificación/Creación de tablas completada.")

        # Opcional: Verificar si las tablas fueron creadas
        inspector = inspect(engine)
        created_tables = inspector.get_table_names()
        logging.info(f"Tablas existentes en la base de datos: {created_tables}")
        for table_name in metadata.tables.keys():
            if table_name not in created_tables:
                 logging.warning(f"La tabla '{table_name}' no parece haber sido creada.")

    except Exception as e:
        logging.error(f"Error al crear las tablas del modelo dimensional: {e}")
        raise # Relanzar la excepción para detener la ejecución si es crítico

# --- Bloque Principal de Ejecución ---

if __name__ == "__main__":
    logging.info("Iniciando script para crear el modelo dimensional...")
    engine = None # Inicializar engine a None
    try:
        # Cargar variables de entorno (asegúrate de tener .env o variables exportadas)
        load_environment()

        # Crear engine para la base de datos especificada en .env
        # Puedes pasar un nombre de DB específico si quieres, ej: creating_engine("mi_dwh")
        engine = creating_engine()

        # Llamar a la función que define y crea las tablas
        create_dimensional_model_tables(engine)

        logging.info("Script finalizado exitosamente.")

    except ValueError as ve:
        logging.error(f"Error de configuración: {ve}")
    except Exception as e:
        logging.error(f"Ocurrió un error inesperado durante la ejecución: {e}")
    finally:
        # Asegurarse de liberar los recursos del engine
        if engine:
            disposing_engine(engine)