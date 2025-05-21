import os
import json
import logging
import time
import requests
from kafka import KafkaConsumer

KAFKA_TOPIC = os.getenv('KAFKA_TOPIC', 'airbnb_stream')
BOOTSTRAP_SERVERS = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'localhost:9092').split(',')
POWERBI_PUSH_URL = os.getenv('POWERBI_PUSH_URL')  


logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def consume_and_forward():
    """
    Consume mensajes de Kafka y los envía al endpoint de Power BI en tiempo real.
    """
    if not POWERBI_PUSH_URL:
        logger.error("Variable POWERBI_PUSH_URL no configurada. Abortando consumidor.")
        return

    consumer = KafkaConsumer(
        KAFKA_TOPIC,
        bootstrap_servers=BOOTSTRAP_SERVERS,
        value_deserializer=lambda msg: json.loads(msg.decode('utf-8')),
        auto_offset_reset='earliest',
        enable_auto_commit=True,
        group_id='airbnb_consumer_group'
    )

    logger.info(f"Consumidor iniciado en topic='{KAFKA_TOPIC}' servers={BOOTSTRAP_SERVERS}")

    try:
        for message in consumer:
            record = message.value
            try:
                payload = [record]
                response = requests.post(
                    POWERBI_PUSH_URL,
                    headers={'Content-Type': 'application/json'},
                    data=json.dumps(payload)
                )
                response.raise_for_status()
                logger.info(f"Registro enviado a Power BI: key={message.key} topic_offset={message.offset}")
            except Exception as e:
                logger.error(f"Error al enviar a Power BI: {e}")

            time.sleep(0.05)
    except Exception as consumer_err:
        logger.error(f"Error en consumo de Kafka: {consumer_err}", exc_info=True)
    finally:
        consumer.close()
        logger.info("Consumidor detenido.")

if __name__ == '__main__':
    consume_and_forward()
