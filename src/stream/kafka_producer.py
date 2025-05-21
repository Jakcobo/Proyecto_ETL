# src/stream/kafka_producer.py
import pandas as pd
import json
import logging
from kafka import KafkaProducer
from kafka.errors import KafkaError
import time

logger = logging.getLogger(__name__)

def create_kafka_producer(bootstrap_servers_list: list[str], retries: int = 5):
    """
    Crea y retorna una instancia de KafkaProducer.
    Maneja reintentos en caso de fallo de conexión inicial.
    """
    producer = None
    for attempt in range(retries):
        try:
            producer = KafkaProducer(
                bootstrap_servers=bootstrap_servers_list,
                value_serializer=lambda v: json.dumps(v).encode('utf-8'), # Serializar valores a JSON -> bytes
                key_serializer=lambda k: str(k).encode('utf-8') if k else None, # Serializar claves (opcional) a string -> bytes
                acks='all',  # Esperar a que todas las réplicas confirmen la recepción
                retries=3    # Reintentos internos del productor para enviar mensajes individuales
            )
            logger.info(f"Productor Kafka conectado exitosamente a {bootstrap_servers_list} en el intento {attempt + 1}.")
            return producer
        except KafkaError as e:
            logger.warning(f"Intento {attempt + 1} de conectar el productor Kafka falló: {e}")
            if attempt < retries - 1:
                time.sleep(5 * (attempt + 1)) # Espera exponencial creciente
            else:
                logger.error(f"No se pudo conectar el productor Kafka a {bootstrap_servers_list} después de {retries} intentos.")
                raise  # Relanzar la última excepción si todos los intentos fallan
    return None # Aunque con el raise anterior, esto no debería alcanzarse si falla.

def send_dataframe_to_kafka(df: pd.DataFrame, topic_name: str, bootstrap_servers: str, 
                            key_column: str = None) -> tuple[bool, int, int]:
    """
    Envía cada fila de un DataFrame como un mensaje JSON a un topic de Kafka.

    Args:
        df (pd.DataFrame): El DataFrame a enviar.
        topic_name (str): El nombre del topic de Kafka.
        bootstrap_servers (str): String de servidores Kafka (ej: 'localhost:29092').
                                 Puede ser una lista separada por comas.
        key_column (str, optional): Nombre de la columna a usar como clave del mensaje.
                                    Si es None, no se enviará clave.

    Returns:
        tuple[bool, int, int]: (éxito_general, mensajes_enviados_exitosamente, mensajes_fallidos)
    """
    if df.empty:
        logger.info(f"DataFrame vacío. No se enviarán mensajes al topic '{topic_name}'.")
        return True, 0, 0

    bootstrap_servers_list = [s.strip() for s in bootstrap_servers.split(',')]
    
    try:
        producer = create_kafka_producer(bootstrap_servers_list)
        if not producer:
            return False, 0, len(df) # Si no se pudo crear el productor
            
    except Exception as e: # Captura errores de create_kafka_producer
        logger.error(f"Error al crear el productor Kafka para el topic '{topic_name}': {e}", exc_info=True)
        return False, 0, len(df)

    sent_count = 0
    failed_count = 0
    
    logger.info(f"Iniciando envío de {len(df)} mensajes al topic '{topic_name}' en {bootstrap_servers_list}...")

    for index, row in df.iterrows():
        message_payload = row.to_dict()
        message_key = row[key_column] if key_column and key_column in row else None
        
        try:
            # Enviar mensaje
            future = producer.send(topic_name, key=message_key, value=message_payload)
            
            # Bloquear hasta que el mensaje sea enviado o falle (con timeout)
            # metadata = future.get(timeout=10) # Opcional: esperar confirmación por mensaje
            # logger.debug(f"Mensaje enviado al topic {metadata.topic} partición {metadata.partition} offset {metadata.offset}")
            sent_count += 1
        except KafkaError as e:
            logger.error(f"Error enviando mensaje (key: {message_key}) al topic '{topic_name}': {e}", exc_info=True)
            failed_count += 1
        except Exception as e:
            logger.error(f"Error inesperado enviando mensaje (key: {message_key}): {e}", exc_info=True)
            failed_count += 1

    if producer:
        try:
            producer.flush(timeout=30)  # Asegurar que todos los mensajes en buffer sean enviados
            logger.info("Productor Kafka hizo flush de mensajes pendientes.")
        except KafkaError as e:
            logger.error(f"Error durante el flush del productor Kafka: {e}", exc_info=True)
            # Los mensajes ya contados como 'sent' podrían no haberse enviado realmente.
            # Aquí podríamos ajustar failed_count, pero se vuelve complejo.
            # Es mejor asegurarse que el flush no falle.
        finally:
            producer.close(timeout=30)
            logger.info("Productor Kafka cerrado.")

    success_overall = failed_count == 0
    if success_overall:
        logger.info(f"Todos los {sent_count} mensajes enviados exitosamente al topic '{topic_name}'.")
    else:
        logger.warning(f"Envío al topic '{topic_name}' completado con {sent_count} éxitos y {failed_count} fallos.")
        
    return success_overall, sent_count, failed_count

if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO,
                        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')

    # --- Configuración para prueba local ---
    # Asegúrate de que Kafka esté corriendo según tu docker-compose.yml
    TEST_BOOTSTRAP_SERVERS = 'localhost:29092'
    TEST_TOPIC_NAME = 'airbnb_publications_test' # Usa un topic de prueba

    # Crear un DataFrame de ejemplo
    sample_data = {
        'publication_key': [101, 102, 103, 104, 105],
        'host_id': [1, 2, 1, 3, 2],
        'price': [100.0, 150.50, 90.0, 200.75, 120.0],
        'name': ['Cozy Apartment', 'Sunny Loft', 'Charming Studio', 'Spacious House', 'Modern Flat'],
        'reviews_per_month': [1.5, None, 3.1, 0.5, 2.2], # Incluir None para probar serialización
        'last_review_date': [20230115, 20230210, None, 20230301, 20221220] # Incluir None
    }
    test_df = pd.DataFrame(sample_data)
    # Convertir columnas que podrían ser Int64 o Float64 de Pandas a tipos nativos para JSON
    for col in ['reviews_per_month', 'last_review_date']: # Añade otras columnas si es necesario
        if col in test_df.columns:
             # Convertir Int64/Float64 a tipos nativos, manejando <NA>
            if pd.api.types.is_integer_dtype(test_df[col].dtype) and test_df[col].hasnans:
                test_df[col] = test_df[col].astype(object).where(pd.notna(test_df[col]), None)
            elif pd.api.types.is_float_dtype(test_df[col].dtype) and test_df[col].hasnans:
                test_df[col] = test_df[col].astype(object).where(pd.notna(test_df[col]), None)


    logger.info(f"--- Iniciando prueba local de send_dataframe_to_kafka ---")
    logger.info(f"DataFrame de prueba:\n{test_df.to_string()}")

    # Prueba 1: Enviar sin clave
    success, sent, failed = send_dataframe_to_kafka(
        df=test_df,
        topic_name=TEST_TOPIC_NAME,
        bootstrap_servers=TEST_BOOTSTRAP_SERVERS
    )
    logger.info(f"Prueba 1 (sin clave) - Éxito: {success}, Enviados: {sent}, Fallidos: {failed}")

    if not test_df.empty:
        # Prueba 2: Enviar con 'publication_key' como clave
        logger.info(f"\n--- Nueva prueba con clave 'publication_key' ---")
        success_k, sent_k, failed_k = send_dataframe_to_kafka(
            df=test_df,
            topic_name=TEST_TOPIC_NAME,
            bootstrap_servers=TEST_BOOTSTRAP_SERVERS,
            key_column='publication_key'
        )
        logger.info(f"Prueba 2 (con clave) - Éxito: {success_k}, Enviados: {sent_k}, Fallidos: {failed_k}")

        # Prueba 3: DataFrame vacío
        logger.info(f"\n--- Nueva prueba con DataFrame vacío ---")
        empty_df = pd.DataFrame()
        success_e, sent_e, failed_e = send_dataframe_to_kafka(
            df=empty_df,
            topic_name=TEST_TOPIC_NAME,
            bootstrap_servers=TEST_BOOTSTRAP_SERVERS
        )
        logger.info(f"Prueba 3 (vacío) - Éxito: {success_e}, Enviados: {sent_e}, Fallidos: {failed_e}")
    else:
        logger.info("DataFrame de prueba estaba vacío, omitiendo pruebas 2 y 3.")


    logger.info("--- Prueba local de send_dataframe_to_kafka finalizada ---")

    # Para verificar los mensajes, puedes usar kafka-console-consumer o un script de consumidor:
    # Ejemplo de comando para el consumidor de consola (ejecutar en otra terminal después de levantar Kafka con docker-compose):
    # docker exec -it kafka_broker kafka-console-consumer --bootstrap-server localhost:29092 --topic airbnb_publications_test --from-beginning --property print.key=true