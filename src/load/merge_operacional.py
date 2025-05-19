import pandas as pd
import numpy as np
from sklearn.neighbors import BallTree
import logging
import time

logger = logging.getLogger(__name__)

def merge_operacional(df_airbnb: pd.DataFrame, df_tiendas: pd.DataFrame, k: int = 5) -> pd.DataFrame:
    """
    Realiza un merge operacional entre publicaciones de Airbnb y puntos de interés cercanos
    (provenientes de la API), usando distancia geográfica con BallTree.

    Args:
        df_airbnb (pd.DataFrame): DataFrame limpio de publicaciones de Airbnb.
        df_tiendas (pd.DataFrame): DataFrame limpio de lugares de la API.
        k (int): Número de vecinos más cercanos a calcular. Por defecto es 5.

    Returns:
        pd.DataFrame: DataFrame con id_publicacion, id_tienda y distancia_km.
    """
    logger.info("Iniciando merge operacional con BallTree.")
    start_total = time.time()

    start = time.time()
    publicaciones_coords = np.radians(df_airbnb[['lat', 'long']].values)
    tiendas_coords = np.radians(df_tiendas[['latitude', 'longitude']].values)
    logger.info(f"Conversión a radianes completada en {time.time() - start:.2f} segundos.")

    start = time.time()
    tree = BallTree(tiendas_coords, metric='haversine')
    logger.info(f"BallTree construido en {time.time() - start:.2f} segundos.")

    start = time.time()
    distances, indices = tree.query(publicaciones_coords, k=k)
    distances_km = distances * 6371
    logger.info(f"Consulta de vecinos completada en {time.time() - start:.2f} segundos.")

    start = time.time()
    resultados = []
    for i, (row_idx, vecino_idxs, dists) in enumerate(zip(df_airbnb.index, indices, distances_km)):
        for tienda_idx, dist in zip(vecino_idxs, dists):
            resultados.append({
                'id_publicacion': df_airbnb.loc[row_idx, 'id'],
                'id_tienda': df_tiendas.iloc[tienda_idx]['fsq_id'],
                'distancia_km': round(dist, 4),
                'category_group': df_tiendas.iloc[tienda_idx]['category_group']
            })
    resultado_df = pd.DataFrame(resultados)
    logger.info(f"DataFrame de resultados ensamblado en {time.time() - start:.2f} segundos.")
    logger.info(f"Merge operacional finalizado en {time.time() - start_total:.2f} segundos.")
    
    return resultado_df
