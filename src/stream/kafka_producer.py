import pandas as pd
import json
import time
import logging
from kafka import KafkaProducer
from kafka.errors import KafkaError
from sqlalchemy import text

import sys
import os
SRC_PATH = os.path.abspath(os.path.join(os.path.dirname(__file__), '..')) 
if SRC_PATH not in sys.path:
    sys.path.append(SRC_PATH)
DB_MODULE_PATH = os.path.abspath(os.path.join(os.path.dirname(__file__), 'database')) 
if DB_MODULE_PATH not in sys.path:
     sys.path.insert(0, DB_MODULE_PATH)

try:
    from database.db import get_db_engine
except ImportError:
    logger_fallback = logging.getLogger(__name__ + "_fallback_producer")
    logger_fallback.error("Error importando get_db_engine para Kafka Producer. Asegúrate que 'src' esté en PYTHONPATH.")


logger = logging.getLogger(__name__)


KAFKA_BROKERS = os.getenv('KAFKA_BROKERS', 'localhost:9092').split(',')
KAFKA_TOPIC = os.getenv('KAFKA_TOPIC_LISTINGS', 'airbnb_listings_stream')

def create_kafka_producer(brokers: list = None):
    """
    Crea y retorna un productor de Kafka.

    Args:
        brokers (list, optional): Lista de brokers de Kafka. Si es None, usa KAFKA_BROKERS.

    Returns:
        KafkaProducer: Instancia del productor de Kafka, o None si falla la conexión.
    """
    brokers_to_use = brokers if brokers else KAFKA_BROKERS
    try:
        producer = KafkaProducer(
            bootstrap_servers=brokers_to_use,
            value_serializer=lambda v: json.dumps(v).encode('utf-8'), # Serializar mensajes a JSON y luego a bytes
            acks='all',  # Esperar a que todos los brokers confirmen la recepción
            retries=5,   # Reintentar envíos fallidos
            # request_timeout_ms=30000 # Opcional: tiempo de espera para la solicitud
        )
        logger.info(f"Productor de Kafka conectado exitosamente a brokers: {brokers_to_use}")
        return producer
    except KafkaError as e:
        logger.error(f"Error al crear el productor de Kafka para {brokers_to_use}: {e}", exc_info=True)
        return None
    except Exception as e:
        logger.error(f"Error inesperado al inicializar el productor de Kafka: {e}", exc_info=True)
        return None


def fetch_data_for_streaming(engine, source_table: str, batch_size: int = 100, offset: int = 0) -> pd.DataFrame:
    """
    Obtiene un lote de datos de la tabla fuente para streaming.
    """
    query = text(f"SELECT * FROM {source_table} ORDER BY listing_poi_key LIMIT :limit OFFSET :offset") # Asumiendo listing_poi_key como PK
    try:
        with engine.connect() as connection:
            df = pd.read_sql_query(query, connection, params={'limit': batch_size, 'offset': offset})
        logger.info(f"Obtenidas {len(df)} filas de '{source_table}' con offset {offset}.")
        return df
    except Exception as e:
        logger.error(f"Error al obtener datos de '{source_table}': {e}", exc_info=True)
        return pd.DataFrame()


def stream_data_to_kafka(
    db_name: str = "airbnb",
    source_table_for_kafka: str = "fact_listing_pois",
    max_messages: int = 100,
    time_limit_seconds: int = 600, # 10 minutos
    batch_size: int = 50, # Cuántas filas leer de la BD a la vez
    delay_between_messages_ms: int = 100 # Pequeña pausa entre mensajes (milisegundos)
):
    """
    Lee datos de una tabla de PostgreSQL y los envía a un topic de Kafka.
    Se detiene después de enviar `max_messages` o después de `time_limit_seconds`.

    Args:
        db_name (str): Nombre de la base de datos de donde leer.
        source_table_for_kafka (str): Nombre de la tabla fuente.
        max_messages (int): Número máximo de mensajes a enviar.
        time_limit_seconds (int): Límite de tiempo en segundos para la ejecución.
        batch_size (int): Número de filas a obtener de la BD en cada consulta.
        delay_between_messages_ms (int): Pausa en milisegundos entre el envío de cada mensaje.
    """
    logger.info(f"Iniciando streaming de datos desde BD '{db_name}', tabla '{source_table_for_kafka}' a Kafka topic '{KAFKA_TOPIC}'.")
    logger.info(f"Límites: max_messages={max_messages}, time_limit_seconds={time_limit_seconds}s.")

    producer = create_kafka_producer()
    if not producer:
        logger.error("No se pudo crear el productor de Kafka. Abortando streaming.")
        return False # Indicar fallo

    db_engine = None
    messages_sent_count = 0
    start_time = time.time()
    current_offset = 0
    all_data_streamed = False

    try:
        db_engine = get_db_engine(db_name=db_name)
        if not db_engine:
            logger.error("No se pudo obtener el engine de la BD. Abortando.")
            return False

        while True:
            # Comprobar condiciones de parada
            elapsed_time = time.time() - start_time
            if messages_sent_count >= max_messages:
                logger.info(f"Límite de {max_messages} mensajes alcanzado.")
                break
            if elapsed_time >= time_limit_seconds:
                logger.info(f"Límite de tiempo de {time_limit_seconds}s alcanzado.")
                break
            if all_data_streamed:
                logger.info("Todos los datos disponibles han sido streameados.")
                break

            # Obtener un lote de datos
            df_batch = fetch_data_for_streaming(db_engine, source_table_for_kafka, batch_size, current_offset)

            if df_batch.empty:
                logger.info(f"No se obtuvieron más datos de '{source_table_for_kafka}' con offset {current_offset}. Asumiendo fin de datos.")
                all_data_streamed = True
                continue # Salta al inicio del while para comprobar condiciones de parada

            # Enviar cada fila del lote como un mensaje
            for index, row in df_batch.iterrows():
                if messages_sent_count >= max_messages or (time.time() - start_time) >= time_limit_seconds:
                    break # Salir del bucle de filas si se alcanza el límite

                message_payload = row.to_dict()
                try:
                    # Usar una clave para el mensaje puede ayudar con la partición en Kafka (opcional)
                    # message_key = str(row['listing_poi_key']).encode('utf-8') if 'listing_poi_key' in row else None
                    
                    future = producer.send(KAFKA_TOPIC, value=message_payload) #, key=message_key)
                    # Esperar a que el mensaje sea enviado (bloqueante, considera hacerlo asíncrono para alto rendimiento)
                    # Para este caso de "mantenerse activo", un poco de bloqueo está bien.
                    metadata = future.get(timeout=10) # Espera hasta 10s por confirmación
                    
                    messages_sent_count += 1
                    logger.debug(f"Mensaje #{messages_sent_count} enviado a Kafka (topic: {metadata.topic}, partition: {metadata.partition}, offset: {metadata.offset}).")

                    if delay_between_messages_ms > 0:
                        time.sleep(delay_between_messages_ms / 1000.0)

                except KafkaError as ke:
                    logger.error(f"Error de Kafka al enviar mensaje: {ke}. Payload: {message_payload}", exc_info=True)
                    # Podrías decidir reintentar o abortar aquí
                except Exception as e:
                    logger.error(f"Error al enviar mensaje a Kafka: {e}. Payload: {message_payload}", exc_info=True)
            
            current_offset += len(df_batch) # Actualizar offset para el siguiente lote

        logger.info(f"Streaming finalizado. Total de mensajes enviados: {messages_sent_count}.")
        return True

    except Exception as e:
        logger.error(f"Error general durante el proceso de streaming a Kafka: {e}", exc_info=True)
        return False
    finally:
        if producer:
            logger.info("Cerrando productor de Kafka...")
            producer.flush(timeout=10) # Esperar a que los mensajes en buffer se envíen
            producer.close(timeout=10)
            logger.info("Productor de Kafka cerrado.")
        if db_engine:
            db_engine.dispose()
            logger.info("Engine de base de datos (Kafka producer) dispuesto.")


if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO, # Cambia a DEBUG para ver logs de envío de mensajes individuales
                        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')

    logger.info("--- Iniciando prueba local de stream_data_to_kafka ---")
    # Asegúrate que tu servidor Kafka esté corriendo en localhost:9092 (o donde KAFKA_BROKERS apunte)
    # y que el topic KAFKA_TOPIC exista (o que Kafka esté configurado para auto-crear topics).
    # También asegúrate que la tabla 'fact_listing_pois' exista en la BD 'airbnb_test_model'
    # y tenga algunos datos (puedes ejecutar las pruebas de load_dimensional.py primero).

    # Para la prueba, reducimos los límites para que no tarde mucho
    test_db = "airbnb_test_model" # La BD donde cargaste el modelo dimensional
    test_table = "fact_listing_pois"
    test_max_messages = 20
    test_time_limit_seconds = 60 # 1 minuto
    test_batch_size = 5
    test_delay_ms = 200 # 200ms entre mensajes

    # Crear el topic manualmente si no existe (opcional, Kafka puede auto-crearlo)
    # from kafka.admin import KafkaAdminClient, NewTopic
    # try:
    #     admin_client = KafkaAdminClient(bootstrap_servers=KAFKA_BROKERS)
    #     topic_list = [NewTopic(name=KAFKA_TOPIC, num_partitions=1, replication_factor=1)]
    #     admin_client.create_topics(new_topics=topic_list, validate_only=False)
    #     logger.info(f"Topic '{KAFKA_TOPIC}' asegurado/creado.")
    # except Exception as e_topic:
    #     logger.warning(f"No se pudo crear el topic '{KAFKA_TOPIC}' (puede que ya exista o haya un problema de permisos/conexión): {e_topic}")


    success_streaming = stream_data_to_kafka(
        db_name=test_db,
        source_table_for_kafka=test_table,
        max_messages=test_max_messages,
        time_limit_seconds=test_time_limit_seconds,
        batch_size=test_batch_size,
        delay_between_messages_ms=test_delay_ms
    )

    if success_streaming:
        logger.info("Prueba de streaming a Kafka completada exitosamente (o según límites).")
    else:
        logger.error("La prueba de streaming a Kafka falló.")

    logger.info("--- Prueba local de stream_data_to_kafka finalizada ---")