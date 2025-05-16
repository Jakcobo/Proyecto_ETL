# /home/nicolas/Escritorio/proyecto ETL/develop/src/transform/api_transform.py

import pandas as pd
from rapidfuzz import process # Asegúrate de que rapidfuzz esté en tus requirements.txt
import logging
import os

# Configurar logging para este módulo
logger = logging.getLogger(__name__)

# Podrías mover esta función a un módulo de utilidades si se usa en múltiples transformaciones.
def _map_category_to_group_api(category_name: str) -> str:
    """
    Mapea una categoría individual de Foursquare a un grupo de categorías definido.
    (Esta es una copia de la función en api_clean.py, adaptada o reutilizada)
    """
    category_mapping = {
        'cultural':                 ['Art Museum', 'Art Gallery', 'History Museum', 'Museum', 'Public Art', 'Cultural Center', 'Indie Movie Theater', 'Music Venue', 'Performing Arts Venue', 'Theater', 'Dance Studio', 'Science Museum', 'Sculpture Garden', 'Concert Hall'],
        'restaurants':              ['Italian Restaurant', 'Fast Food Restaurant', 'Korean Restaurant', 'Thai Restaurant', 'Wine Bar', 'Brazilian Restaurant', 'Pizzeria', 'American Restaurant', 'French Restaurant', 'Mexican Restaurant', 'Chinese Restaurant', 'Latin American Restaurant', 'Greek Restaurant', 'Burger Joint', 'Deli', 'Cocktail Bar', 'Wine Store', 'Dessert Shop', 'Sandwich Spot', 'Seafood Restaurant', 'Café', 'Gastropub', 'Fried Chicken Joint', 'Coffee Shop', 'Pub', 'Kosher Restaurant', 'Cantonese Restaurant', 'Asian Restaurant', 'German Restaurant', 'Wings Joint', 'Irish Pub'],
        'parks_&_outdoor':          ['Urban Park', 'Lake', 'Park', 'Playground', 'State or Provincial Park', 'Botanical Garden', 'Picnic Area', 'Beach', 'Soccer Field', 'National Park', 'Hiking Trail', 'Campground', 'Scenic Lookout', 'Dog Park', 'Zoo', 'Zoo Exhibit'],
        'retail_&_shopping':        ['Bakery', 'Grocery Store', 'Big Box Store', 'Gourmet Store', 'Electronics Store', 'Organic Grocery', 'Clothing Store', 'Toy Store', 'Department Store', 'Gift Store', 'Convenience Store', 'Shopping Mall', 'Supermarket', 'Retail', 'Shopping Plaza', 'Discount Store', 'Fruit and Vegetable Store', 'Furniture and Home Store', 'Video Games Store', 'Office Supply Store', 'Tobacco Store', 'Liquor Store', 'Beer Store', 'Warehouse or Wholesale Store'],
        'entertainment_&_leisure':  ['Movie Theater', 'Arcade', 'Bowling Alley', 'Amusement Park', 'Pool Hall', 'Event Space', 'Karaoke Bar', 'Sports Bar', 'Lounge', 'Beer Garden', 'Stadium', 'Rock Climbing Spot'],
        'landmarks':                ['Landmarks and Outdoors', 'Monument', 'Bridge'], # 'Landmarks and Outdoors' es un poco genérico, revisa tus categorías Foursquare
        'bars_&_clubs':             ['Bar', 'Gay Bar', 'Dive Bar', 'Cocktail Bar', 'Beer Bar', 'Hotel Bar', 'Beer Garden', 'Hookah Bar', 'Wine Store', 'Night Club', 'Rock Club', 'Comedy Club']
    }
    if not isinstance(category_name, str): # Manejar posibles NaNs o no strings
        return 'Other'
    for group, categories_in_group in category_mapping.items():
        # Podrías querer hacer esto insensible a mayúsculas/minúsculas para mayor robustez
        if category_name in categories_in_group:
            return group
    return 'Other'

def clean_api_data_updated(df_input: pd.DataFrame) -> pd.DataFrame:
    """
    Limpia y transforma el DataFrame de Foursquare (considerado como datos de API).
    Esta función es la principal para la tarea de transformación de API.

    Args:
        df_input (pd.DataFrame): DataFrame crudo de Foursquare.

    Returns:
        pd.DataFrame: DataFrame transformado y limpio.
    """
    if not isinstance(df_input, pd.DataFrame):
        logger.error("La entrada para clean_api_data_updated no es un DataFrame.")
        raise TypeError("La entrada debe ser un pandas DataFrame.")

    if df_input.empty:
        logger.warning("DataFrame de entrada para clean_api_data_updated está vacío. Retornando DataFrame vacío.")
        return df_input.copy() # Devolver una copia para evitar modificar el original vacío

    logger.info(f"Iniciando limpieza de datos de API (Foursquare). Shape entrada: {df_input.shape}")
    df = df_input.copy()

    # 1. Normalización de Nombres de Columnas (opcional, pero buena práctica para consistencia)
    # Si los nombres de columna del CSV ya son buenos, puedes omitir esto.
    # df.columns = df.columns.str.lower().str.replace(' ', '_', regex=False)
    # logger.info(f"Columnas normalizadas: {df.columns.tolist()}")

    # 2. Normalización de Categorías (Fuzzy Matching) - (Reutilizando lógica de api_clean.py)
    if 'category' in df.columns and not df['category'].isnull().all():
        # Define tus categorías estándar a las que quieres mapear.
        # Estas deberían ser más generales si luego vas a usar _map_category_to_group_api.
        # O, si _map_category_to_group_api es suficientemente bueno, puedes omitir este paso de fuzzy matching.
        # Por ahora, lo mantendré como en tu api_clean.py.
        standard_categories_for_fuzzy = ['Restaurant', 'Bar', 'Shopping Center', 'Cultural Place', 'Park', 'Museum', 'Hotel'] # Ajusta estas
        logger.info("Normalizando columna 'category' con fuzzy matching...")
        unique_categories = df['category'].dropna().unique()
        normalization_dict = {}
        for cat_name in unique_categories:
            if isinstance(cat_name, str): # Asegurar que sea string antes de procesar
                match = process.extractOne(cat_name, standard_categories_for_fuzzy, score_cutoff=75) # Ajusta score_cutoff
                if match:
                    normalization_dict[cat_name] = match[0] # match[0] es la cadena coincidente
        
        df['category_normalized'] = df['category'].map(normalization_dict) # Guardar en nueva columna o sobreescribir 'category'
        # Si sobreescribes, df['category'] = df['category'].map(normalization_dict).fillna(df['category'])
        # logger.info("Normalización de 'category' con fuzzy matching completada.")
        # logger.debug(f"Mapeos de normalización: {normalization_dict}")
        # Si decides usar category_normalized, entonces _map_category_to_group_api debería usarla.
        # Por ahora, asumiré que el mapeo a grupo usa la columna 'category' original.
    else:
        logger.warning("Columna 'category' no encontrada o completamente vacía. Saltando normalización con fuzzy matching.")

    # 3. Limpieza de Dirección (Reutilizando lógica de api_clean.py)
    if 'address' in df.columns:
        logger.info("Limpiando columna 'address'...")
        df['address_cleaned'] = df['address'].apply(
            lambda x: ''.join(str(x).split(', ')) if pd.notnull(x) and isinstance(x, str) else x
        ) # Guardar en nueva columna
        # df['address'] = ... si quieres sobreescribir
        logger.info("Limpieza de 'address' completada.")
    else:
        logger.warning("Columna 'address' no encontrada. Saltando limpieza de dirección.")

    # 4. Mapeo de Categorías a Grupos (Reutilizando lógica de api_clean.py)
    # Decide si usar 'category' o 'category_normalized' (si la creaste y la prefieres)
    category_col_for_grouping = 'category' # o 'category_normalized'
    if category_col_for_grouping in df.columns:
        logger.info(f"Mapeando '{category_col_for_grouping}' a 'category_group'...")
        df['category_group'] = df[category_col_for_grouping].apply(_map_category_to_group_api)
        logger.info("Mapeo a 'category_group' completado.")
    else:
        logger.warning(f"Columna '{category_col_for_grouping}' no encontrada. No se pudo crear 'category_group'.")
        df['category_group'] = 'Other' # O manejar de otra forma, como np.nan

    # 5. Selección y/o Eliminación de Columnas
    # Define las columnas que quieres MANTENER o las que quieres ELIMINAR.
    # Es más robusto seleccionar las que quieres mantener.
    columns_to_keep = [
        'fsq_id',  # Asumiendo que es el ID único
        'name',
        'latitude',
        'longitude',
        # 'address', # Original, si la quieres
        'address_cleaned', # La limpia, si la creaste y prefieres
        # 'category', # Original, si la quieres
        # 'category_normalized', # La normalizada, si la creaste y prefieres
        'category_group' # La agrupada
    ]
    
    # Asegurarse de que las columnas a mantener existen en el DataFrame
    final_columns = [col for col in columns_to_keep if col in df.columns]
    
    # Columnas a eliminar (alternativa a columns_to_keep)
    # columns_to_remove = ['price', 'rating', 'hours', 'popularity', 'website', 'phone', 'category']
    # existing_cols_to_remove = [col for col in columns_to_remove if col in df.columns]
    # if existing_cols_to_remove:
    #    logger.info(f"Eliminando columnas: {existing_cols_to_remove}")
    #    df = df.drop(columns=existing_cols_to_remove)

    df = df[final_columns].copy() # Seleccionar solo las columnas deseadas
    logger.info(f"Selección de columnas finalizada. Columnas resultantes: {df.columns.tolist()}")

    logger.info(f"Transformación de datos de API completada. Shape salida: {df.shape}")
    # logger.debug(f"Primeras filas del DataFrame transformado:\n{df.head().to_markdown(index=False)}")
    
    return df

if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO,
                        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')

    logger.info("--- Iniciando prueba local de clean_api_data_updated ---")

    # Crear un DataFrame de ejemplo similar al de api_clean.py para la prueba
    sample_data_api = {
        'fsq_id': ['1', '2', '3', '4', '5'],
        'name': ['Place A - Cafe', 'Place B - Art Gallery', 'Place C - Some Random Bar', 'Place D - Museum of History', 'Place E - Null Cat'],
        'category': ['Coffee Shop', 'Art Gallery', 'Divey Bar', 'History Museum', None], # 'Divey Bar' no está en _map_category_to_group_api
        'address': ['123 Main St, NY, USA', '456 Oak Ave, NY, USA', None, '789 Pine, CA, USA', 'Missing Address'],
        'latitude': [40.71, 40.72, 40.73, 40.74, 40.75],
        'longitude': [-74.00, -74.01, -74.02, -74.03, -74.04],
        'price': [2, 3, 1, 2, 1], # Será eliminada
        'rating': [4.5, 4.8, 3.9, 4.1, 3.0], # Será eliminada
        'hours': ['Mon-Fri', 'Tue-Sun', 'All week', 'Varies', '24/7'], # Será eliminada
        'popularity': [0.9, 0.8, 0.7, 0.85, 0.5], # Será eliminada
        'website': ['a.com', 'b.com', 'c.com', 'd.com', 'e.com'], # Será eliminada
        'phone': ['111', '222', '333', '444', '555'] # Será eliminada
    }
    test_df_api_raw = pd.DataFrame(sample_data_api)
    logger.info(f"DataFrame de prueba original:\n{test_df_api_raw.to_markdown(index=False)}")

    try:
        cleaned_df_api = clean_api_data_updated(test_df_api_raw)
        logger.info(f"DataFrame de API limpio:\n{cleaned_df_api.to_markdown(index=False)}")
        logger.info(f"Columnas del DataFrame limpio: {cleaned_df_api.columns.tolist()}")
        logger.info(f"Tipos de datos del DataFrame limpio:\n{cleaned_df_api.dtypes}")

        # Prueba con DataFrame vacío
        logger.info("\n--- Probando con DataFrame vacío ---")
        empty_df_cleaned = clean_api_data_updated(pd.DataFrame())
        logger.info(f"DataFrame vacío procesado. Shape: {empty_df_cleaned.shape}")
        if empty_df_cleaned.empty:
            logger.info("Procesamiento de DataFrame vacío exitoso.")

    except Exception as main_exception:
        logger.error(f"Error durante la prueba local: {main_exception}", exc_info=True)
    finally:
        logger.info("--- Prueba local de clean_api_data_updated finalizada ---")