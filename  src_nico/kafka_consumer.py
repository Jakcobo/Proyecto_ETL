# /home/nicolas/Escritorio/proyecto ETL/develop/src/kafka_consumer.py

import json
import logging
from kafka import KafkaConsumer
from kafka.errors import KafkaError
import os
import signal # Para manejar Ctrl+C

# Configurar logging básico para el consumidor
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger(__name__)

# --- Constantes de Configuración de Kafka (deben coincidir con el productor) ---
# Es mejor mover esto a variables de entorno si se comparten
KAFKA_BROKERS = os.getenv('KAFKA_BROKERS_CONSUMER', 'localhost:9092').split(',')
KAFKA_TOPIC = os.getenv('KAFKA_TOPIC_LISTINGS_CONSUMER', 'airbnb_listings_stream')
CONSUMER_GROUP_ID = os.getenv('KAFKA_CONSUMER_GROUP_ID', 'airbnb_listing_visualizer_group') # ID de grupo para el consumidor

# Variable global para controlar el bucle del consumidor
running = True

def signal_handler(sig, frame):
    """Manejador para señales de interrupción (Ctrl+C)."""
    global running
    logger.info("Señal de interrupción recibida. Deteniendo consumidor...")
    running = False

def consume_messages(
    brokers: list = None,
    topic: str = None,
    group_id: str = None,
    auto_offset_reset: str = 'earliest' # 'earliest' para leer desde el principio, 'latest' para nuevos mensajes
):
    """
    Consume mensajes de un topic de Kafka y los imprime en la consola.

    Args:
        brokers (list, optional): Lista de brokers de Kafka. Defaults a KAFKA_BROKERS.
        topic (str, optional): Nombre del topic. Defaults a KAFKA_TOPIC.
        group_id (str, optional): ID del grupo de consumidores. Defaults a CONSUMER_GROUP_ID.
        auto_offset_reset (str, optional): Política si no hay offset inicial o si el offset actual
                                           ya no existe. 'earliest' o 'latest'.
    """
    global running
    brokers_to_use = brokers if brokers else KAFKA_BROKERS
    topic_to_consume = topic if topic else KAFKA_TOPIC
    consumer_group = group_id if group_id else CONSUMER_GROUP_ID

    consumer = None
    try:
        logger.info(f"Intentando conectar consumidor a brokers: {brokers_to_use}, topic: {topic_to_consume}, group_id: {consumer_group}")
        consumer = KafkaConsumer(
            topic_to_consume,
            bootstrap_servers=brokers_to_use,
            auto_offset_reset=auto_offset_reset, # Comienza a leer desde el mensaje más antiguo si es un nuevo consumidor
            group_id=consumer_group,
            value_deserializer=lambda v: json.loads(v.decode('utf-8')), # Deserializar JSON de bytes a dict
            # consumer_timeout_ms=1000 # Descomentar para que el bucle no bloquee indefinidamente si no hay mensajes
                                     # y poder comprobar la variable 'running' más a menudo.
            # enable_auto_commit=True, # Por defecto es True, Kafka maneja los commits de offset
            # auto_commit_interval_ms=5000 # Frecuencia de auto-commit
        )
        logger.info("Consumidor de Kafka conectado y suscrito exitosamente.")
        logger.info(f"Esperando mensajes en el topic '{topic_to_consume}'. Presiona Ctrl+C para detener.")

        message_count = 0
        while running:
            # El método poll() o iterar sobre el consumidor son formas de obtener mensajes.
            # Iterar sobre el consumidor es bloqueante hasta que llega un mensaje o hay timeout.
            for message in consumer:
                if not running: # Comprobar de nuevo por si la señal llegó mientras se procesaba un mensaje
                    break
                
                message_count += 1
                logger.info(f"\n--- Mensaje #{message_count} Recibido ---")
                logger.info(f"Topic: {message.topic}")
                logger.info(f"Partition: {message.partition}")
                logger.info(f"Offset: {message.offset}")
                logger.info(f"Key: {message.key.decode('utf-8') if message.key else 'N/A'}") # Si enviaste claves
                logger.info(f"Timestamp: {message.timestamp} ({pd.to_datetime(message.timestamp, unit='ms') if message.timestamp else 'N/A'})")
                
                # Imprimir el valor del mensaje (payload)
                # El payload ya está deserializado a un diccionario Python por value_deserializer
                logger.info(f"Value (Payload):")
                print(json.dumps(message.value, indent=2)) # Imprimir JSON formateado

                # Si consumer_timeout_ms está configurado y no hay mensajes, el bucle for terminará
                # y el while running volverá a iterar. Si no está configurado, el for message in consumer
                # bloqueará hasta que llegue un mensaje o se interrumpa.
            
            if not running: # Salir del bucle while si se detuvo
                break
            
            # Si se usó consumer_timeout_ms y el bucle for terminó sin mensajes,
            # podemos añadir una pequeña pausa antes de volver a intentar.
            # time.sleep(0.1) # Pequeña pausa

    except KafkaError as ke:
        logger.error(f"Error de Kafka durante el consumo: {ke}", exc_info=True)
    except KeyboardInterrupt: # Manejado por signal_handler, pero por si acaso
        logger.info("Interrupción de teclado detectada en el bucle principal.")
    except Exception as e:
        logger.error(f"Error inesperado durante el consumo de mensajes: {e}", exc_info=True)
    finally:
        if consumer:
            logger.info("Cerrando consumidor de Kafka...")
            consumer.close()
            logger.info("Consumidor de Kafka cerrado.")
        logger.info("Proceso de consumo finalizado.")

if __name__ == '__main__':
    # Registrar el manejador de señales para Ctrl+C
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler) # Manejar también SIGTERM

    logger.info("Iniciando consumidor de Kafka local...")
    # Puedes pasar argumentos aquí si quieres sobrescribir los valores por defecto/variables de entorno
    consume_messages(
        # brokers=['otro_host:9093'], # Ejemplo
        # topic='otro_topic',        # Ejemplo
        # group_id='mi_grupo_local_test', # Ejemplo
        auto_offset_reset='latest' # Para pruebas, 'latest' es útil para ver solo nuevos mensajes
                                    # Cambia a 'earliest' si quieres ver todos los mensajes desde el principio del topic
                                    # si el grupo de consumidores es nuevo o su offset se perdió.
    )