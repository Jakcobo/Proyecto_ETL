# /home/nicolas/Escritorio/proyecto ETL/develop/src/database/merge.py

import pandas as pd
import numpy as np
from math import radians, sin, cos, sqrt, atan2
import logging
import os

logger = logging.getLogger(__name__)

# --- Constantes ---
EARTH_RADIUS_KM = 6371  # Radio de la Tierra en kilómetros

# --- Funciones Auxiliares ---

def haversine_distance(lat1, lon1, lat2_series, lon2_series):
    """
    Calcula la distancia Haversine entre un punto (lat1, lon1) y una serie de puntos (lat2_series, lon2_series).
    Optimizado para usarse con series de Pandas.

    Args:
        lat1 (float): Latitud del primer punto en grados.
        lon1 (float): Longitud del primer punto en grados.
        lat2_series (pd.Series): Serie de latitudes del segundo conjunto de puntos en grados.
        lon2_series (pd.Series): Serie de longitudes del segundo conjunto de puntos en grados.

    Returns:
        pd.Series: Serie de distancias en kilómetros.
    """
    lat1_rad, lon1_rad = map(radians, [lat1, lon1])
    lat2_rad = np.radians(lat2_series)
    lon2_rad = np.radians(lon2_series)

    dlon = lon2_rad - lon1_rad
    dlat = lat2_rad - lat1_rad

    a = np.sin(dlat / 2.0)**2 + np.cos(lat1_rad) * np.cos(lat2_rad) * np.sin(dlon / 2.0)**2
    c = 2 * np.arctan2(np.sqrt(a), np.sqrt(1 - a))
    distances = EARTH_RADIUS_KM * c
    return distances

# --- Función Principal de Merge ---

def merge_airbnb_api_data(
    df_airbnb: pd.DataFrame,
    df_api: pd.DataFrame,
    radius_km: float = 0.5  # Radio de búsqueda por defecto: 500 metros
) -> pd.DataFrame:
    """
    Combina datos de Airbnb con datos de API (Foursquare) basados en proximidad geográfica.
    Para cada listado de Airbnb, agrega información sobre los POIs de API cercanos,
    como el conteo de POIs por category_group.

    Args:
        df_airbnb (pd.DataFrame): DataFrame limpio de Airbnb.
                                  Debe contener 'id' (ID único de Airbnb), 'lat', 'long'.
        df_api (pd.DataFrame): DataFrame limpio de API/Foursquare.
                               Debe contener 'latitude', 'longitude', 'category_group'.
                               La columna 'id' de df_api (fsq_id) no se usa directamente en este merge de agregación.
        radius_km (float, optional): Radio en kilómetros para considerar POIs como "cercanos".
                                     Defaults to 0.5 km.

    Returns:
        pd.DataFrame: El DataFrame de Airbnb original con nuevas columnas agregadas
                      que resumen los POIs cercanos.
    """
    if not isinstance(df_airbnb, pd.DataFrame) or not isinstance(df_api, pd.DataFrame):
        logger.error("Ambas entradas deben ser DataFrames de Pandas.")
        raise TypeError("Entradas inválidas: se esperan DataFrames de Pandas.")

    if df_airbnb.empty:
        logger.warning("El DataFrame de Airbnb está vacío. Retornando DataFrame de Airbnb vacío.")
        return df_airbnb.copy()
    if df_api.empty:
        logger.warning("El DataFrame de API (Foursquare) está vacío. No se agregarán POIs. Retornando DataFrame de Airbnb sin cambios.")
        # Podríamos añadir columnas de conteo con ceros aquí si siempre se esperan
        return df_airbnb.copy()

    # Validar columnas necesarias
    required_airbnb_cols = ['id', 'lat', 'long'] # 'id' para asegurar que no lo perdamos
    required_api_cols = ['latitude', 'longitude', 'category_group']
    
    for col in required_airbnb_cols:
        if col not in df_airbnb.columns:
            logger.error(f"Columna requerida '{col}' ausente en el DataFrame de Airbnb.")
            raise ValueError(f"Columna '{col}' faltante en df_airbnb.")
    for col in required_api_cols:
        if col not in df_api.columns:
            logger.error(f"Columna requerida '{col}' ausente en el DataFrame de API.")
            raise ValueError(f"Columna '{col}' faltante en df_api.")

    # Asegurar tipos numéricos para coordenadas y limpiar NaNs
    df_airbnb['lat'] = pd.to_numeric(df_airbnb['lat'], errors='coerce')
    df_airbnb['long'] = pd.to_numeric(df_airbnb['long'], errors='coerce')
    df_api['latitude'] = pd.to_numeric(df_api['latitude'], errors='coerce')
    df_api['longitude'] = pd.to_numeric(df_api['longitude'], errors='coerce')

    # Eliminar filas con coordenadas NaN que impedirían el cálculo de distancia
    original_airbnb_rows = len(df_airbnb)
    original_api_rows = len(df_api)
    df_airbnb.dropna(subset=['lat', 'long'], inplace=True)
    df_api.dropna(subset=['latitude', 'longitude', 'category_group'], inplace=True) # category_group también es crucial

    if len(df_airbnb) < original_airbnb_rows:
        logger.warning(f"{original_airbnb_rows - len(df_airbnb)} filas eliminadas de Airbnb debido a lat/long nulas.")
    if len(df_api) < original_api_rows:
        logger.warning(f"{original_api_rows - len(df_api)} filas eliminadas de API debido a lat/long/category_group nulas.")

    if df_airbnb.empty or df_api.empty:
        logger.warning("Uno de los DataFrames quedó vacío después de eliminar NaNs en coordenadas. No se puede realizar el merge geoespacial.")
        # Devolver df_airbnb con columnas de conteo vacías/cero si es necesario
        # Por ahora, devolvemos el df_airbnb (posiblemente vacío)
        return df_airbnb.copy()


    logger.info(f"Iniciando merge geoespacial. Airbnb listings: {len(df_airbnb)}, API POIs: {len(df_api)}, Radio: {radius_km} km.")

    # Obtener todas las categorías únicas de API para crear columnas de conteo
    # Es importante que 'category_group' no tenga NaNs aquí.
    api_categories = df_api['category_group'].unique().tolist()
    poi_count_columns = {f'nearby_{cat.replace(" ", "_").replace("&", "and").lower()}_count': cat for cat in api_categories}
    
    # Inicializar nuevas columnas en el DataFrame de Airbnb
    for col_name in poi_count_columns.keys():
        df_airbnb[col_name] = 0
    df_airbnb['total_nearby_pois'] = 0
    # df_airbnb['distance_to_nearest_poi_km'] = np.nan # Opcional

    # Lista para almacenar los resultados agregados para cada Airbnb
    aggregated_features_list = []

    # Iterar sobre cada listado de Airbnb
    # Esto puede ser lento para datasets grandes. Considerar optimizaciones (KDTree, etc.) si el rendimiento es un problema.
    for index, airbnb_row in df_airbnb.iterrows():
        airbnb_lat = airbnb_row['lat']
        airbnb_lon = airbnb_row['long']

        # Calcular distancias a todos los POIs de API
        distances_to_api_pois = haversine_distance(airbnb_lat, airbnb_lon, df_api['latitude'], df_api['longitude'])

        # Filtrar POIs dentro del radio
        nearby_pois_mask = distances_to_api_pois <= radius_km
        df_nearby_api_pois = df_api[nearby_pois_mask].copy() # .copy() para evitar SettingWithCopyWarning
        
        # df_nearby_api_pois['distance_km'] = distances_to_api_pois[nearby_pois_mask] # Guardar distancias si se necesitan

        # Inicializar conteos para este Airbnb
        current_airbnb_poi_counts = {col_name: 0 for col_name in poi_count_columns.keys()}
        current_airbnb_poi_counts['total_nearby_pois'] = len(df_nearby_api_pois)
        # current_airbnb_poi_counts['distance_to_nearest_poi_km'] = df_nearby_api_pois['distance_km'].min() if not df_nearby_api_pois.empty else np.nan


        if not df_nearby_api_pois.empty:
            # Contar POIs por category_group
            category_counts = df_nearby_api_pois['category_group'].value_counts().to_dict()
            for new_col_name, original_cat_name in poi_count_columns.items():
                current_airbnb_poi_counts[new_col_name] = category_counts.get(original_cat_name, 0)
        
        # Añadir el ID del Airbnb para poder hacer merge luego (o actualizar directamente si se prefiere)
        current_airbnb_poi_counts['id'] = airbnb_row['id'] # Usar el ID de Airbnb como clave
        aggregated_features_list.append(current_airbnb_poi_counts)

    # Convertir la lista de diccionarios a DataFrame
    if aggregated_features_list:
        df_aggregated_features = pd.DataFrame(aggregated_features_list)
        
        # Hacer merge de las características agregadas de vuelta al DataFrame de Airbnb original
        # Asegurarse que 'id' es el índice o una columna en ambos para el merge.
        # df_airbnb.set_index('id', inplace=True) # Si 'id' no es índice
        # df_aggregated_features.set_index('id', inplace=True)
        
        # df_airbnb.update(df_aggregated_features) # .update() modifica en el lugar
        # df_airbnb.reset_index(inplace=True) # Si se usó set_index

        # Una forma más segura de hacer merge es usando pd.merge si 'id' es una columna
        # Primero, eliminamos las columnas de conteo inicializadas con 0 en df_airbnb
        cols_to_drop_before_merge = list(poi_count_columns.keys()) + ['total_nearby_pois'] # + ['distance_to_nearest_poi_km']
        df_airbnb_original_cols = df_airbnb.drop(columns=cols_to_drop_before_merge, errors='ignore')
        
        df_result = pd.merge(df_airbnb_original_cols, df_aggregated_features, on='id', how='left')
        # Rellenar con 0 los conteos para los Airbnb que no tuvieron POIs cercanos (y por tanto no están en df_aggregated_features o tienen NaNs)
        for col_name in poi_count_columns.keys():
            if col_name in df_result.columns:
                df_result[col_name] = df_result[col_name].fillna(0).astype(int)
        if 'total_nearby_pois' in df_result.columns:
            df_result['total_nearby_pois'] = df_result['total_nearby_pois'].fillna(0).astype(int)
        # if 'distance_to_nearest_poi_km' in df_result.columns:
        #     df_result['distance_to_nearest_poi_km'] = df_result['distance_to_nearest_poi_km'].fillna(np.nan)


    else: # No se encontraron POIs para ningún Airbnb o la lista está vacía
        logger.info("No se generaron características agregadas (lista vacía). Devolviendo df_airbnb con columnas de conteo en cero.")
        # Las columnas de conteo ya fueron inicializadas con 0, así que df_airbnb está listo.
        df_result = df_airbnb.copy()


    logger.info(f"Merge geoespacial completado. Shape final del DataFrame de Airbnb: {df_result.shape}")
    # logger.debug(f"Primeras filas del DataFrame mergeado:\n{df_result.head().to_markdown(index=False)}")
    return df_result


if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO,
                        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')

    logger.info("--- Iniciando prueba local de merge_airbnb_api_data ---")

    # Crear DataFrames de ejemplo (simplificados)
    airbnb_data = {
        'id': [1, 2, 3],
        'name_airbnb': ['Airbnb A', 'Airbnb B', 'Airbnb C'],
        'lat': [40.75, 40.76, 40.70], # NY lat
        'long': [-73.98, -73.99, -73.90], # NY lon
        'price': [100, 150, 200]
    }
    df_airbnb_test = pd.DataFrame(airbnb_data)

    api_data = {
        'id_api': ['fsq1', 'fsq2', 'fsq3', 'fsq4', 'fsq5'], # Asumimos que 'id' en API es fsq_id
        'name_poi': ['Restaurant Alpha', 'Park Beta', 'Cafe Gamma', 'Museum Delta', 'Bar Epsilon'],
        'latitude': [40.7501, 40.7502, 40.7601, 40.7001, 40.69],
        'longitude': [-73.9801, -73.9803, -73.9901, -73.9001, -73.95],
        'category_group': ['restaurants', 'parks_&_outdoor', 'restaurants', 'cultural', 'bars_&_clubs']
    }
    df_api_test = pd.DataFrame(api_data)

    try:
        logger.info(f"df_airbnb_test antes del merge:\n{df_airbnb_test.to_markdown(index=False)}")
        logger.info(f"df_api_test:\n{df_api_test.to_markdown(index=False)}")

        # Radio de 0.2 km para la prueba (200 metros)
        df_merged_result = merge_airbnb_api_data(df_airbnb_test, df_api_test, radius_km=0.2)
        
        logger.info(f"DataFrame mergeado (resultado):\n{df_merged_result.to_markdown(index=False)}")
        logger.info(f"Columnas del resultado: {df_merged_result.columns.tolist()}")

        # Prueba con radio más grande para asegurar más matches
        logger.info("\n--- Probando con radio de 1 km ---")
        df_merged_result_large_radius = merge_airbnb_api_data(df_airbnb_test, df_api_test, radius_km=1.0)
        logger.info(f"DataFrame mergeado (radio 1km):\n{df_merged_result_large_radius.to_markdown(index=False)}")
        
        # Prueba con df_api vacío
        logger.info("\n--- Probando con df_api vacío ---")
        df_merged_empty_api = merge_airbnb_api_data(df_airbnb_test.copy(), pd.DataFrame(columns=df_api_test.columns))
        logger.info(f"DataFrame mergeado (API vacío):\n{df_merged_empty_api.to_markdown(index=False)}")
        if all(col in df_merged_empty_api.columns for col in ['nearby_restaurants_count', 'total_nearby_pois']):
            logger.info("Columnas de conteo presentes (con ceros) cuando API es vacío.")


    except Exception as e:
        logger.error(f"Error durante la prueba local: {e}", exc_info=True)
    finally:
        logger.info("--- Prueba local de merge_airbnb_api_data finalizada ---")