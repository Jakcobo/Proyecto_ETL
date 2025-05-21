import pandas as pd
import logging

logger = logging.getLogger(__name__)

def data_merge(df_airbnb: pd.DataFrame, df_operations: pd.DataFrame) -> pd.DataFrame:
    """
    Realiza el merge entre publicaciones de Airbnb, puntos de interés cercanos
    (provenientes de la API) y las operaciones finales para la integración del 
    merge final, usando distancia geográfica con BallTree.

    Args:
        df_airbnb (pd.DataFrame): DataFrame limpio de publicaciones de Airbnb.
        df_tiendas (pd.DataFrame): DataFrame limpio de lugares de la API.
        df_operations (pd.DataFrame): DataFrame limpio la integración entre la 
        API y AirBnB.

    Returns:
        pd.DataFrame: DataFrame con id_publicacion, id_tienda y distancia_km.
    """
    df_data_airbnb = df_airbnb
    df_data_api = df_operations
    df_merged = pd.DataFrame()

    if not df_data_airbnb.empty or df_data_api.empty:
        logger.info("Verificando filas duplicadas en df_data_airbnb y df_data_api.")
        df_data_airbnb.rename(columns={'id': 'publication_key'}, inplace=True)
        num_duplicados_airbnb = df_data_airbnb.duplicated().sum()
        num_duplicados_api = df_data_api.duplicated().sum()
        logger.info(f"Número de filas duplicadas encontradas en df_data_airbnb: {num_duplicados_airbnb}")
        logger.info(f"Número de filas duplicadas encontradas en df_data_api: {num_duplicados_api}")
    else:
        if df_data_airbnb.empty:
            logger.critical("El DataFrame principal df_data_airbnb está vacío después de la carga. Terminando.")
        if df_data_api.empty:
            logger.critical("El DataFrame API df_data_api está vacío después de la carga. Terminando.")

    mapping_api_category = ['restaurants', 'parks_&_outdoor', 'retail_&_shopping', 'entertainment_&_leisure', 'landmarks', 'bars_&_clubs']
    logger.info(f"Categorías objetivo para el conteo: {mapping_api_category}")

    if not df_data_api.empty and 'id_publicacion' in df_data_api.columns and 'category_group' in df_data_api.columns:
        logger.info("Preparando los datos de API para el merge: contando categorías por id_publicacion.")

        try:
            poi_counts = df_data_api.pivot_table(
                index='id_publicacion',
                columns='category_group',
                values='id_tienda',
                aggfunc='count',
                fill_value=0
            )
            logger.info(f"Tabla pivote de conteo de POIs creada. Shape: {poi_counts.shape}")
        except Exception as e:
            logger.error(f"Error al crear la tabla pivote: {e}", exc_info=True)
            poi_counts = pd.DataFrame()
        
        if not poi_counts.empty:
            for cat in mapping_api_category:
                if cat not in poi_counts.columns:
                    poi_counts[cat] = 0
                    logger.info(f"Columna '{cat}' añadida a poi_counts con valor 0 (no presente originalmente).")
                    
            poi_counts = poi_counts.reindex(columns=mapping_api_category, fill_value=0)
            logger.info(f"Columnas de poi_counts reordenadas y aseguradas. Nuevas columnas: {poi_counts.columns.tolist()}")
            
            new_column_names = {}
            for cat in poi_counts.columns:
                clean_name = cat.replace('_&_', '_and_').replace(' ', '_')
                new_column_names[cat] = f"count_nearby_{clean_name}"
            
            poi_counts.rename(columns=new_column_names, inplace=True)
            logger.info(f"Columnas de conteo de POIs renombradas: {poi_counts.columns.tolist()}")

            if 'publication_key' in df_data_airbnb.columns:
                if 'last_review' in df_data_airbnb.columns:
                    try:
                        logger.info("Convirtiendo columna 'last_review' a formato entero AAAAMMDD.")
                        df_data_airbnb['last_review'] = pd.to_datetime(df_data_airbnb['last_review'], errors='coerce')
                        df_data_airbnb['last_review'] = df_data_airbnb['last_review'].dt.strftime('%Y%m%d').astype(float).astype('Int64')
                    except Exception as e:
                        logger.warning(f"No se pudo convertir 'last_review' a entero: {e}", exc_info=True)
                
                logger.info(f"Realizando left merge entre df_data_airbnb (on 'publication_key') y poi_counts (on index).")
                df_merged = pd.merge(
                    df_data_airbnb,
                    poi_counts,
                    left_on='publication_key',
                    right_index=True,
                    how='left'
                )
                logger.info(f"Merge completado. Shape de df_merged: {df_merged.shape}")
                
                count_cols_to_fill = poi_counts.columns.tolist()
                for col in count_cols_to_fill:
                    if col in df_merged.columns:
                        df_merged[col] = df_merged[col].fillna(0).astype(int)
                logger.info(f"Valores NaN en las nuevas columnas de conteo rellenados con 0 y convertidos a int.")

                logger.info("Primeras filas del DataFrame fusionado (df_merged):")
                logger.info(f"\n{df_merged.head().to_markdown(index=False)}")
                logger.info("Información del DataFrame fusionado:")

                df_merged.info()
                
            else:
                logger.error("La columna 'publication_key' no se encuentra en df_data_airbnb. No se puede realizar el merge.")
                df_merged = df_data_airbnb.copy()

        else:
            logger.warning("La tabla pivote 'poi_counts' está vacía. No se añadirán columnas de conteo.")
            df_merged = df_data_airbnb.copy()

    else:
        if df_data_api.empty:
            logger.warning("El DataFrame df_data_api está vacío. No se puede realizar el procesamiento de conteo de categorías.")
        else:
            logger.warning("Las columnas 'id_publicacion' o 'category_group' no existen en df_data_api. No se puede procesar.")
        df_merged = df_data_airbnb.copy()
    logger.info("Fin del script de merge de datos.")
    return df_merged