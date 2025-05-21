# src/stream/kafka_consumer.py
import json
import logging
import requests # Para hacer peticiones HTTP a Power BI
from kafka import KafkaConsumer
from kafka.errors import KafkaError
import time
from datetime import datetime # Para convertir la fecha

logger = logging.getLogger(__name__)

# --- CONFIGURACIÓN POWER BI ---
# Reemplaza esta URL con la tuya real si es diferente
POWERBI_STREAMING_URL = "https://api.powerbi.com/beta/693cbea0-4ef9-4254-8977-76e05cb5f556/datasets/6b4e2f65-3022-4590-adbc-5b7783074aec/rows?experience=power-bi&key=DSXqyJ5CA8adQiT5nUbGkQ2HFpLkxmOGfPe%2B6nxkYaVGnZVo6bTSqfMg92aVrDLwdH0MVuGeUq5FAuEHNEP3yQ%3D%3D"
POWERBI_BATCH_SIZE = 100  # Cuántos mensajes acumular antes de enviar (ajusta según necesidad y límites)
POWERBI_SEND_INTERVAL_S = 5 # O enviar cada X segundos (100 mensajes / 5s = 20 mensajes/s)

# --- FIN CONFIGURACIÓN POWER BI ---

def send_data_to_powerbi(data_payload: list, powerbi_url: str, retries: int = 3, backoff_factor: float = 0.5) -> bool:
    """
    Envía un lote de datos (payload) a un endpoint de streaming de Power BI.

    Args:
        data_payload (list): Lista de diccionarios, donde cada diccionario es una fila.
        powerbi_url (str): La URL completa del endpoint de streaming de Power BI.
        retries (int): Número de reintentos en caso de fallo.
        backoff_factor (float): Factor para el backoff exponencial.

    Returns:
        bool: True si el envío fue exitoso, False en caso contrario.
    """
    if not data_payload:
        logger.info("Payload para Power BI vacío, no se envía nada.")
        return True # Considerado éxito ya que no había nada que enviar

    headers = {
        "Content-Type": "application/json"
    }
    
    for attempt in range(retries):
        try:
            response = requests.post(powerbi_url, headers=headers, data=json.dumps(data_payload))
            response.raise_for_status()  # Lanza una excepción para códigos de error HTTP 4xx/5xx
            logger.debug(f"Datos enviados a Power BI exitosamente. Status: {response.status_code}")
            return True
        except requests.exceptions.HTTPError as http_err:
            logger.warning(f"Error HTTP enviando a Power BI (intento {attempt + 1}/{retries}): {http_err.response.status_code} - {http_err.response.text}")
            if response.status_code < 500 and response.status_code != 429: # Errores de cliente (4xx) no suelen resolverse reintentando, excepto 429 (Too Many Requests)
                break 
        except requests.exceptions.RequestException as req_err:
            logger.warning(f"Error de red/conexión enviando a Power BI (intento {attempt + 1}/{retries}): {req_err}")
        
        if attempt < retries - 1:
            sleep_time = backoff_factor * (2 ** attempt)
            logger.info(f"Reintentando envío a Power BI en {sleep_time:.2f} segundos...")
            time.sleep(sleep_time)
        else:
            logger.error(f"Falló el envío a Power BI después de {retries} intentos.")
            return False
    return False


def transform_for_powerbi(kafka_message_value: dict) -> dict:
    """
    Transforma un mensaje de Kafka para que coincida con el esquema esperado por Power BI.
    Principalmente maneja la conversión de fechas.
    """
    transformed_row = kafka_message_value.copy() # Trabajar con una copia

    # Convertir 'last_review' (YYYYMMDD int) a formato ISO 8601 string para Power BI
    if 'last_review' in transformed_row and transformed_row['last_review'] is not None:
        try:
            date_int = int(transformed_row['last_review'])
            # Si la fecha es la ficticia de 'sin revisión', enviarla como None o una fecha muy antigua
            if date_int == 22620411: # La fecha que usaste para nulos
                 transformed_row['last_review'] = None # O "1900-01-01T00:00:00Z" si Power BI necesita una fecha
            else:
                date_obj = datetime.strptime(str(date_int), '%Y%m%d')
                transformed_row['last_review'] = date_obj.isoformat() + "Z" # Formato ISO 8601 UTC
        except (ValueError, TypeError) as e:
            logger.warning(f"No se pudo convertir 'last_review' ({transformed_row['last_review']}): {e}. Se enviará como None.")
            transformed_row['last_review'] = None
    
    # Asegurar que los booleanos sean booleanos de Python (json.dumps los maneja bien)
    for bool_col in ['host_verification', 'instant_bookable_flag']:
        if bool_col in transformed_row:
            if isinstance(transformed_row[bool_col], str):
                transformed_row[bool_col] = transformed_row[bool_col].lower() == 'true'
            elif transformed_row[bool_col] is None: # pd.NA se convierte a None en la serialización
                transformed_row[bool_col] = None # O False si prefieres un default
            else:
                transformed_row[bool_col] = bool(transformed_row[bool_col])

    # Otros tipos de datos (números, texto) generalmente se mapean bien si los nombres de columna coinciden.
    # Si Power BI espera tipos estrictos (ej. entero vs decimal), asegúrate aquí.
    # Por ejemplo, si 'price' debe ser siempre float con dos decimales, aunque json.dumps lo maneja.
    
    return transformed_row


def consume_messages_and_send_to_powerbi(
    topic_name: str, 
    bootstrap_servers: str, 
    powerbi_url: str,
    batch_size: int,
    send_interval_s: float, # Intervalo para enviar si el lote no se llena
    group_id: str = 'powerbi_streaming_consumer_group', 
    auto_offset_reset: str = 'latest', # 'latest' es común para dashboards en tiempo real
    consume_timeout_ms: int = 1000 # Tiempo que el iterador consumer espera por más mensajes
):
    bootstrap_servers_list = [s.strip() for s in bootstrap_servers.split(',')]
    logger.info(f"Conectando consumidor al topic '{topic_name}' en {bootstrap_servers_list} con group_id '{group_id}' para Power BI...")

    message_batch = []
    last_send_time = time.time()
    total_messages_processed = 0
    total_batches_sent = 0
    total_messages_sent_to_pbi = 0

    try:
        consumer = KafkaConsumer(
            topic_name,
            bootstrap_servers=bootstrap_servers_list,
            group_id=group_id,
            auto_offset_reset=auto_offset_reset,
            consumer_timeout_ms=consume_timeout_ms, 
            value_deserializer=lambda v: json.loads(v.decode('utf-8')),
            key_deserializer=lambda k: k.decode('utf-8') if k else None,
        )
        logger.info(f"Consumidor conectado y suscrito a '{topic_name}'. Esperando mensajes para Power BI...")

        while True: # Bucle para seguir intentando consumir mientras el script corra
            batch_filled_by_timeout = False
            try:
                for message in consumer: # Este bucle interno se romperá si consumer_timeout_ms se alcanza
                    total_messages_processed += 1
                    logger.debug(f"Raw Kafka msg: Key={message.key}, Offset={message.offset}")
                    
                    # Transformar el mensaje para el esquema de Power BI
                    powerbi_row_data = transform_for_powerbi(message.value)
                    message_batch.append(powerbi_row_data)
                    logger.debug(f"Mensaje procesado y añadido al lote Power BI. Lote actual: {len(message_batch)}")

                    if len(message_batch) >= batch_size:
                        break # Salir del bucle for para enviar el lote

                # Después del bucle for, o porque se llenó el lote o porque hubo timeout
                current_time = time.time()
                if not message_batch and (current_time - last_send_time) < send_interval_s :
                    # No hay mensajes en el lote Y no ha pasado el intervalo de envío
                    # Si el consumer_timeout_ms fue alcanzado, el bucle for terminó.
                    # Si consumer_timeout_ms es bajo, esto puede pasar a menudo.
                    # Si no hubo mensajes, el bucle for no se ejecutó.
                    if consumer.assignment() and not any(consumer.position(tp) < consumer.highwater(tp) for tp in consumer.assignment()):
                         logger.debug(f"Sin mensajes nuevos en Kafka. Lote vacío. Esperando {send_interval_s - (current_time - last_send_time):.2f}s o nuevos mensajes.")
                    # Pequeña pausa para no hacer un bucle while True muy ajustado si no hay mensajes
                    time.sleep(0.1) 
                    continue


                if len(message_batch) >= batch_size or \
                   (message_batch and (current_time - last_send_time) >= send_interval_s):
                    
                    logger.info(f"Enviando lote de {len(message_batch)} mensajes a Power BI...")
                    success = send_data_to_powerbi(message_batch, powerbi_url)
                    if success:
                        logger.info(f"Lote enviado exitosamente a Power BI. Total mensajes PBI: {total_messages_sent_to_pbi + len(message_batch)}")
                        total_messages_sent_to_pbi += len(message_batch)
                        total_batches_sent += 1
                        message_batch = [] 
                    else:
                        logger.warning("Falló el envío del lote a Power BI. Los mensajes de este lote se perderán (o implementa dead-letter queue).")
                        message_batch = [] # Descartar lote fallido por ahora
                    last_send_time = time.time()
                
                # Si salimos del bucle for debido a consumer_timeout_ms y no hay nada en el lote,
                # el logger de arriba ("Sin mensajes nuevos...") lo manejará.
                # No necesitamos un 'else' explícito aquí para el caso de lote vacío post-timeout.

            except StopIteration: # Esto puede ocurrir si el topic es finito y se alcanza el final.
                logger.info("StopIteration: No más mensajes en el topic o final alcanzado.")
                break # Salir del while True si el topic realmente terminó.
            except KeyboardInterrupt:
                logger.info("Interrupción por teclado recibida. Saliendo...")
                break # Salir del while True

        logger.info(f"Bucle de consumo finalizado. Total mensajes Kafka procesados: {total_messages_processed}.")

    except KafkaError as e:
        logger.error(f"Error de Kafka durante el consumo para Power BI: {e}", exc_info=True)
    except Exception as e:
        logger.error(f"Error inesperado durante el consumo para Power BI: {e}", exc_info=True)
    finally:
        # Envío final de cualquier mensaje restante en el lote al cerrar
        if message_batch:
            logger.info(f"Enviando lote final de {len(message_batch)} mensajes a Power BI al cerrar...")
            send_data_to_powerbi(message_batch, powerbi_url)
        
        if 'consumer' in locals() and consumer:
            logger.info("Cerrando consumidor Kafka...")
            consumer.close()
            logger.info("Consumidor Kafka cerrado.")
        logger.info(f"Resumen: Total lotes enviados a PBI: {total_batches_sent}, Total mensajes en lotes exitosos a PBI: {total_messages_sent_to_pbi}")


if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO, # Cambia a logging.DEBUG para ver más detalle
                        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')

    TEST_BOOTSTRAP_SERVERS_CONSUMER = 'localhost:29092'
    # APUNTA ESTE AL TOPIC CORRECTO DONDE EL DAG ENVÍA LOS DATOS
    TEST_TOPIC_NAME_CONSUMER = 'airbnb_publications_enriched' 
    
    # Usa un group_id que recuerde su posición si quieres continuidad, 
    # o uno dinámico para empezar desde 'latest'/'earliest' cada vez.
    # consumer_group_id = f'powerbi_streamer_group_{int(time.time())}' 
    consumer_group_id = 'powerbi_streamer_group_static_1' # Para reanudar si se cae

    logger.info(f"--- Iniciando consumidor para Power BI ---")
    logger.info(f"Consumiendo del topic: {TEST_TOPIC_NAME_CONSUMER}")
    logger.info(f"Servidores Bootstrap: {TEST_BOOTSTRAP_SERVERS_CONSUMER}")
    logger.info(f"Group ID: {consumer_group_id}")
    logger.info(f"URL Power BI: {POWERBI_STREAMING_URL[:POWERBI_STREAMING_URL.find('key=') + 4]}...") # No loguear la key completa

    consume_messages_and_send_to_powerbi(
        topic_name=TEST_TOPIC_NAME_CONSUMER,
        bootstrap_servers=TEST_BOOTSTRAP_SERVERS_CONSUMER,
        powerbi_url=POWERBI_STREAMING_URL,
        batch_size=POWERBI_BATCH_SIZE,
        send_interval_s=POWERBI_SEND_INTERVAL_S,
        group_id=consumer_group_id,
        auto_offset_reset='latest', # 'latest' para no reenviar datos viejos si se reinicia
        consume_timeout_ms=1000  # Timeout corto para el iterador, el while True maneja la persistencia
    )

    logger.info("--- Consumidor para Power BI finalizado (o interrumpido) ---")