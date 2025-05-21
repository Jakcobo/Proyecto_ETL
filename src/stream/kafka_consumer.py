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
POWERBI_STREAMING_URL = "https://api.powerbi.com/beta/693cbea0-4ef9-4254-8977-76e05cb5f556/datasets/6b4e2f65-3022-4590-adbc-5b7783074aec/rows?experience=power-bi&key=DSXqyJ5CA8adQiT5nUbGkQ2HFpLkxmOGfPe%2B6nxkYaVGnZVo6bTSqfMg92aVrDLwdH0MVuGeUq5FAuEHNEP3yQ%3D%3D"
POWERBI_BATCH_SIZE = 10000  # Ajusta según sea necesario, 100 es un buen punto de partida
POWERBI_SEND_INTERVAL_S = 1 # Envía cada 5 segundos si el lote no se llena antes

# --- FIN CONFIGURACIÓN POWER BI ---

def send_data_to_powerbi(data_payload: list, powerbi_url: str, retries: int = 3, backoff_factor: float = 0.5) -> bool:
    if not data_payload:
        logger.debug("Payload para Power BI vacío, no se envía nada.")
        return True

    headers = {
        "Content-Type": "application/json"
    }
    
    if data_payload:
        logger.debug(f"Enviando a Power BI. Muestra del primer objeto en el payload: {json.dumps(data_payload[0], indent=2, default=str)}") # default=str para manejar tipos no serializables

    for attempt in range(retries):
        try:
            # Convertir el payload a una lista de diccionarios antes de json.dumps
            payload_to_send = [dict(row) for row in data_payload]
            response = requests.post(powerbi_url, headers=headers, data=json.dumps(payload_to_send, default=str)) # default=str
            
            logger.debug(f"Respuesta de Power BI (intento {attempt + 1}): Status={response.status_code}, Contenido='{response.text[:500]}...'")
            
            response.raise_for_status()
            logger.info(f"Lote de {len(data_payload)} mensajes enviado a Power BI exitosamente. Status: {response.status_code}")
            return True
        except requests.exceptions.HTTPError as http_err:
            error_message = f"Error HTTP enviando a Power BI (intento {attempt + 1}/{retries}): {http_err.response.status_code}"
            try:
                error_detail = http_err.response.json()
                error_message += f" - {error_detail}"
            except json.JSONDecodeError:
                error_message += f" - {http_err.response.text[:500]}"
            logger.warning(error_message)
            if http_err.response.status_code < 500 and http_err.response.status_code != 429:
                logger.error(f"Error de cliente ({http_err.response.status_code}) no recuperable por reintento. Deteniendo reintentos para este lote.")
                break 
        except requests.exceptions.RequestException as req_err:
            logger.warning(f"Error de red/conexión enviando a Power BI (intento {attempt + 1}/{retries}): {req_err}")
        except TypeError as type_err: # Capturar errores de serialización JSON
            logger.error(f"Error de serialización JSON al preparar payload para Power BI (intento {attempt + 1}): {type_err}")
            logger.debug(f"Payload problemático (primer elemento): {data_payload[0] if data_payload else 'Lote vacío'}")
            break # No reintentar si hay error de serialización
        
        if attempt < retries - 1:
            sleep_time = backoff_factor * (2 ** attempt)
            logger.info(f"Reintentando envío a Power BI en {sleep_time:.2f} segundos...")
            time.sleep(sleep_time)
        else:
            logger.error(f"Falló el envío a Power BI después de {retries} intentos para el lote actual.")
            return False
    return False


def transform_for_powerbi(kafka_message_value: dict) -> dict:
    if not isinstance(kafka_message_value, dict):
        logger.warning(f"Valor del mensaje Kafka no es un diccionario: {type(kafka_message_value)}. No se puede transformar.")
        return {}

    logger.debug(f"Transformando mensaje para Power BI. Datos originales: {kafka_message_value}")
    transformed_row = {}

    # **ACCIÓN IMPORTANTE: Revisa y ajusta los nombres de las claves aquí**
    # para que coincidan EXACTAMENTE con tu esquema de Power BI.
    key_map = {
        # "nombre_columna_en_kafka": "NombreColumnaEnPowerBI",
        "construction_year": "construction_year", # Ejemplo: Si en Power BI se llama 'ConstructionYear' -> "ConstructionYear"
        # Añade otros mapeos si son necesarios
    }

    for kafka_key, value in kafka_message_value.items():
        powerbi_key = key_map.get(kafka_key, kafka_key) # Usa el nombre mapeado o el original si no hay mapeo

        if kafka_key == 'last_review':
            if value is not None:
                try:
                    date_int = int(value)
                    if date_int == 22620411:
                        transformed_row[powerbi_key] = None
                    else:
                        date_obj = datetime.strptime(str(date_int), '%Y%m%d')
                        transformed_row[powerbi_key] = date_obj.isoformat() + "Z"
                except (ValueError, TypeError) as e:
                    logger.warning(f"No se pudo convertir '{kafka_key}' ({value}): {e}. Se enviará como None.")
                    transformed_row[powerbi_key] = None
            else:
                transformed_row[powerbi_key] = None
        elif kafka_key in ['host_verification', 'instant_bookable_flag']:
            if isinstance(value, str):
                transformed_row[powerbi_key] = value.lower() == 'true'
            elif value is None:
                transformed_row[powerbi_key] = None
            else:
                transformed_row[powerbi_key] = bool(value)
        else:
            transformed_row[powerbi_key] = value
            
    logger.debug(f"Mensaje transformado para Power BI: {transformed_row}")
    return transformed_row


def consume_messages_and_send_to_powerbi(
    topic_name: str, 
    bootstrap_servers: str, 
    powerbi_url: str,
    batch_size: int,
    send_interval_s: float,
    group_id: str = 'powerbi_streaming_consumer_group', 
    auto_offset_reset: str = 'earliest', # Cambiado a 'earliest' para la prueba
    consume_timeout_ms: int = 5000 # Aumentado un poco para dar más margen
):
    bootstrap_servers_list = [s.strip() for s in bootstrap_servers.split(',')]
    logger.info(f"Conectando consumidor al topic '{topic_name}' en {bootstrap_servers_list} con group_id '{group_id}' para Power BI (auto_offset_reset='{auto_offset_reset}')...")

    message_batch = []
    last_send_time = time.time()
    total_messages_processed_kafka = 0
    total_batches_sent_to_pbi_successfully = 0
    total_messages_in_successful_pbi_batches = 0

    consumer = None # Inicializar para el bloque finally
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
        logger.info(f"Consumidor conectado y suscrito a '{topic_name}'. Esperando mensajes...")

        running = True
        while running:
            try:
                messages_in_poll = 0
                for message in consumer: 
                    messages_in_poll +=1
                    total_messages_processed_kafka += 1
                    logger.info(f"Kafka msg #{total_messages_processed_kafka} RECIBIDO: Key={message.key}, Offset={message.offset}, Partition={message.partition}")
                    
                    powerbi_row_data = transform_for_powerbi(message.value)
                    if powerbi_row_data:
                        message_batch.append(powerbi_row_data)
                    
                    if len(message_batch) >= batch_size:
                        logger.debug(f"Lote lleno ({len(message_batch)} mensajes). Rompiendo bucle de fetch para enviar.")
                        break 

                if messages_in_poll == 0 and not message_batch:
                     logger.debug(f"No se recibieron nuevos mensajes de Kafka en este poll (timeout: {consume_timeout_ms}ms). El lote está vacío.")

                current_time = time.time()
                time_since_last_send = current_time - last_send_time
                
                should_send_batch = False
                if len(message_batch) >= batch_size:
                    logger.info(f"Preparando para enviar lote porque alcanzó el tamaño máximo ({len(message_batch)}/{batch_size}).")
                    should_send_batch = True
                elif message_batch and time_since_last_send >= send_interval_s:
                    logger.info(f"Preparando para enviar lote ({len(message_batch)} mensajes) porque ha pasado el intervalo de tiempo ({time_since_last_send:.2f}s >= {send_interval_s}s).")
                    should_send_batch = True
                
                if should_send_batch:
                    logger.info(f"Enviando lote de {len(message_batch)} mensajes a Power BI...")
                    success = send_data_to_powerbi(list(message_batch), powerbi_url) 
                    if success:
                        total_messages_in_successful_pbi_batches += len(message_batch)
                        total_batches_sent_to_pbi_successfully += 1
                        message_batch.clear() 
                    else:
                        logger.warning("Falló el envío del lote a Power BI. Descartando lote actual.")
                        message_batch.clear() 
                    last_send_time = time.time()
                
                # Si no hay mensajes de Kafka y no hay nada en el lote, hacer una pequeña pausa
                if messages_in_poll == 0 and not message_batch:
                    time.sleep(0.5) # Pausa para no consumir CPU si no hay actividad

            except StopIteration: 
                logger.info("StopIteration: No más mensajes en el topic o final alcanzado por el consumidor. Finalizando.")
                running = False
            except KeyboardInterrupt:
                logger.info("Interrupción por teclado recibida. Finalizando bucle de consumo...")
                running = False
            except Exception as e:
                logger.error(f"Error inesperado en el bucle de consumo: {e}", exc_info=True)
                time.sleep(1)

        logger.info(f"Bucle de consumo principal finalizado. Total mensajes Kafka procesados: {total_messages_processed_kafka}.")

    except KafkaError as e:
        logger.error(f"Error de Kafka (fuera del bucle de consumo) para Power BI: {e}", exc_info=True)
    except Exception as e:
        logger.error(f"Error inesperado (fuera del bucle de consumo) para Power BI: {e}", exc_info=True)
    finally:
        if message_batch:
            logger.info(f"Enviando lote final de {len(message_batch)} mensajes a Power BI al cerrar...")
            send_data_to_powerbi(list(message_batch), powerbi_url)
        
        if consumer:
            logger.info("Cerrando consumidor Kafka...")
            consumer.close(autocommit=False) 
            logger.info("Consumidor Kafka cerrado.")
        logger.info(f"Resumen Final: Lotes exitosos a PBI: {total_batches_sent_to_pbi_successfully}, Mensajes en lotes exitosos a PBI: {total_messages_in_successful_pbi_batches}")


if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO, # Puedes cambiar a logging.DEBUG para más detalle
                        format='%(asctime)s - %(name)s - %(levelname)s - %(module)s - %(funcName)s - %(lineno)d - %(message)s')

    TEST_BOOTSTRAP_SERVERS_CONSUMER = 'localhost:29092'
    TEST_TOPIC_NAME_CONSUMER = 'airbnb_publications_enriched' 
    
    # Para forzar la lectura desde el principio cada vez que ejecutas para pruebas:
    consumer_group_id = f'powerbi_debug_consumer_{int(time.time())}'
    effective_auto_offset_reset = 'earliest'
    
    # Si quisieras que recuerde dónde se quedó (ej. para un servicio continuo):
    # consumer_group_id = 'powerbi_streaming_service_group_1'
    # effective_auto_offset_reset = 'latest' # Para empezar con datos nuevos si el grupo ya existe

    logger.info(f"--- Iniciando consumidor para Power BI ---")
    logger.info(f"Consumiendo del topic: {TEST_TOPIC_NAME_CONSUMER}")
    logger.info(f"Servidores Bootstrap: {TEST_BOOTSTRAP_SERVERS_CONSUMER}")
    logger.info(f"Group ID: {consumer_group_id}")
    logger.info(f"Auto Offset Reset: {effective_auto_offset_reset}")
    logger.info(f"URL Power BI: {POWERBI_STREAMING_URL[:POWERBI_STREAMING_URL.find('key=') + 4]}...") # No loguear la key completa

    consume_messages_and_send_to_powerbi(
        topic_name=TEST_TOPIC_NAME_CONSUMER,
        bootstrap_servers=TEST_BOOTSTRAP_SERVERS_CONSUMER,
        powerbi_url=POWERBI_STREAMING_URL,
        batch_size=POWERBI_BATCH_SIZE,
        send_interval_s=POWERBI_SEND_INTERVAL_S,
        group_id=consumer_group_id,
        auto_offset_reset=effective_auto_offset_reset, 
        consume_timeout_ms=5000 # Espera 5 segundos por nuevos mensajes en cada poll
    )

    logger.info("--- Consumidor para Power BI finalizado (o interrumpido) ---")