# /home/nicolas/Escritorio/proyecto ETL/develop/src/transform/api_clean.py
import pandas as pd
from rapidfuzz import process
import logging

logger = logging.getLogger(__name__)

def _map_category_to_group(category_name: str) -> str:
    """Mapea una categoría individual a un grupo de categorías definido."""
    # Crear un diccionario de mapeo para agrupar las categorías
    category = {
        'cultural':                 ['Art Museum', 'Art Gallery', 'History Museum', 'Museum', 'Public Art', 'Cultural Center', 'Indie Movie Theater', 'Music Venue', 'Performing Arts Venue', 'Theater', 'Dance Studio', 'Science Museum', 'Sculpture Garden', 'Concert Hall'],
        'restaurants':              ['Italian Restaurant', 'Fast Food Restaurant', 'Korean Restaurant', 'Thai Restaurant', 'Wine Bar', 'Brazilian Restaurant', 'Pizzeria', 'American Restaurant', 'French Restaurant', 'Mexican Restaurant', 'Chinese Restaurant', 'Latin American Restaurant', 'Greek Restaurant', 'Burger Joint', 'Deli', 'Cocktail Bar', 'Wine Store', 'Dessert Shop', 'Sandwich Spot', 'Seafood Restaurant', 'Café', 'Gastropub', 'Fried Chicken Joint', 'Coffee Shop', 'Pub', 'Kosher Restaurant', 'Cantonese Restaurant', 'Asian Restaurant', 'German Restaurant', 'Wings Joint', 'Irish Pub'],
        'parks_&_outdoor':          ['Urban Park', 'Lake', 'Park', 'Playground', 'State or Provincial Park', 'Botanical Garden', 'Picnic Area', 'Beach', 'Soccer Field', 'National Park', 'Hiking Trail', 'Campground', 'Scenic Lookout', 'Dog Park', 'Zoo', 'Zoo Exhibit'],
        'retail_&_shopping':        ['Bakery', 'Grocery Store', 'Big Box Store', 'Gourmet Store', 'Electronics Store', 'Organic Grocery', 'Clothing Store', 'Toy Store', 'Department Store', 'Gift Store', 'Convenience Store', 'Shopping Mall', 'Supermarket', 'Retail', 'Shopping Plaza', 'Discount Store', 'Fruit and Vegetable Store', 'Furniture and Home Store', 'Video Games Store', 'Office Supply Store', 'Tobacco Store', 'Liquor Store', 'Beer Store', 'Warehouse or Wholesale Store'],
        'entertainment_&_leisure':  ['Movie Theater', 'Arcade', 'Bowling Alley', 'Amusement Park', 'Pool Hall', 'Event Space', 'Karaoke Bar', 'Sports Bar', 'Lounge', 'Beer Garden', 'Stadium', 'Rock Climbing Spot'],
        'landmarks':                ['Landmarks and Outdoors', 'Monument', 'Bridge'],
        'bars_&_clubs':             ['Bar', 'Gay Bar', 'Dive Bar', 'Cocktail Bar', 'Beer Bar', 'Hotel Bar', 'Beer Garden', 'Hookah Bar', 'Wine Store', 'Night Club', 'Rock Club', 'Comedy Club']
    }
    if not isinstance(category_name, str): # Manejar posibles NaNs o no strings
        return 'Other'
    for group, categories_in_group in category.items():
        if category_name in categories_in_group:
            return group
    return 'Other'

def clean_api_data(df_input: pd.DataFrame) -> pd.DataFrame:
    """
    Limpia y transforma el DataFrame de Foursquare (considerado como datos de API).
    """
    if df_input.empty:
        logger.warning("DataFrame de entrada está vacío. Retornando DataFrame vacío.")
        return df_input
        
    logger.info("Iniciando limpieza de datos de Foursquare.")
    df = df_input.copy()

    # 1. Normalización de Categorías (Fuzzy Matching)
    if 'category' in df.columns and not df['category'].isnull().all():
        standard_categories = ['Restaurant', 'Bar', 'Shopping Center', 'Cultural Place', 'Park']
        logger.info("Normalizando columna 'category'...")
        unique_categories = df['category'].dropna().unique()
        normalization_dict = {}
        for cat in unique_categories:
            match = process.extractOne(cat, standard_categories)
            if match:
                normalization_dict[cat] = match[0]
        
        df['category'] = df['category'].map(normalization_dict).fillna(df['category'])
        logger.info("Normalización de 'category' completada.")
    else:
        logger.warning("Columna 'category' no encontrada o completamente vacía. Saltando normalización.")

    # 2. Limpieza de Dirección
    if 'address' in df.columns:
        logger.info("Limpiando columna 'address'...")
        df['address'] = df['address'].apply(
            lambda x: ''.join(str(x).split(', ')) if pd.notnull(x) else x
        )
        logger.info("Limpieza de 'address' completada.")
    else:
        logger.warning("Columna 'address' no encontrada. Saltando limpieza de dirección.")

    # 3. Mapeo de Categorías a Grupos
    if 'category' in df.columns: # Basado en la categoría (posiblemente normalizada)
        logger.info("Mapeando 'category' a 'category_group'...")
        df['category_group'] = df['category'].apply(_map_category_to_group)
        logger.info("Mapeo a 'category_group' completado.")
    else:
        logger.warning("Columna 'category' no encontrada. No se pudo crear 'category_group'.")
        df['category_group'] = 'Other' # O manejar de otra forma

    # 4. Eliminación de Columnas
    columns_to_remove = ['price', 'rating', 'hours', 'popularity', 'website', 'phone', 'category']
    # Nota: Se elimina 'category' porque ahora usamos 'category_group'.
    #       Si necesitas 'category' original o normalizada en la BD, ajústalo.
    
    existing_columns_to_remove = [col for col in columns_to_remove if col in df.columns]
    if existing_columns_to_remove:
        logger.info(f"Eliminando columnas: {existing_columns_to_remove}")
        df = df.drop(columns=existing_columns_to_remove)
    
    logger.info(f"Transformación completada. Columnas resultantes: {df.columns.tolist()}")
    # logger.debug(f"Primeras filas del DataFrame transformado:\n{df.head()}") # Usar debug para info detallada
    
    return df

if __name__ == '__main__':
    # Configuración de logging para prueba local
    logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(name)s: %(message)s", datefmt="%Y-%m-%d %H:%M:%S")
    
    # Ejemplo de uso para prueba local
    # Crear un DataFrame de ejemplo
    sample_data = {
        'fsq_id': ['1', '2', '3', '4'],
        'name': ['Place A', 'Place B', 'Place C', 'Place D'],
        'category': ['Italian Restaurant', 'Art Museum', 'Some Bar', 'Unknown Category'],
        'address': ['123 Main St, NY, USA', '456 Oak Ave, NY, USA', None, '789 Pine, CA'],
        'latitude': [40.71, 40.72, 40.73, 40.74],
        'longitude': [-74.00, -74.01, -74.02, -74.03],
        'price': [2, 3, 1, 2],
        'rating': [4.5, 4.8, 3.9, 4.1],
        'hours': ['Mon-Fri', 'Tue-Sun', 'All week', 'Varies'],
        'popularity': [0.9, 0.8, 0.7, 0.85],
        'website': ['a.com', 'b.com', 'c.com', 'd.com'],
        'phone': ['111', '222', '333', '444']
    }
    test_df = pd.DataFrame(sample_data)
    logger.info("--- DataFrame de prueba original ---")
    logger.info(f"\n{test_df}")
    
    cleaned_df = clean_api_data(test_df)
    logger.info("--- DataFrame de prueba limpio ---")
    logger.info(f"\n{cleaned_df}")
    logger.info(f"Columnas: {cleaned_df.columns.tolist()}")