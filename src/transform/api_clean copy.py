# /home/nicolas/Escritorio/proyecto ETL/develop/src/transform/api_clean copy.py
import pandas as pd

def transform_data(input_file_path, output_file_path):
    # Cargar el archivo CSV
    df = pd.read_csv(input_file_path)

    # Eliminar las columnas no deseadas
    columns_to_remove = ['price', 'rating', 'hours', 'popularity', 'website', 'phone', 'address', 'category']
    df = df.drop(columns=columns_to_remove)

    # Crear un diccionario de mapeo para agrupar las categorías
    category_mapping = {
        'Cultural & Artistic Venues': ['Art Museum', 'Art Gallery', 'History Museum', 'Museum', 'Public Art', 'Cultural Center', 'Comedy Club', 'Indie Movie Theater', 'Music Venue', 'Performing Arts Venue', 'Theater', 'Rock Club', 'Dance Studio', 'Science Museum', 'Sculpture Garden', 'Concert Hall'],
        'Restaurants': ['Italian Restaurant', 'Fast Food Restaurant', 'Korean Restaurant', 'Thai Restaurant', 'Wine Bar', 'Brazilian Restaurant', 'Pizzeria', 'American Restaurant', 'French Restaurant', 'Mexican Restaurant', 'Chinese Restaurant', 'Latin American Restaurant', 'Greek Restaurant', 'Burger Joint', 'Deli', 'Cocktail Bar', 'Wine Store', 'Dessert Shop', 'Sandwich Spot', 'Seafood Restaurant', 'Café', 'Gastropub', 'Fried Chicken Joint', 'Coffee Shop', 'Pub', 'Kosher Restaurant', 'Cantonese Restaurant', 'Asian Restaurant', 'German Restaurant', 'Wings Joint', 'Irish Pub'],
        'Parks & Outdoor': ['Urban Park', 'Lake', 'Park', 'Playground', 'State or Provincial Park', 'Botanical Garden', 'Picnic Area', 'Beach', 'Soccer Field', 'National Park', 'Hiking Trail', 'Campground', 'Scenic Lookout', 'Dog Park', 'Zoo', 'Zoo Exhibit'],
        'Retail & Shopping': ['Bakery', 'Grocery Store', 'Big Box Store', 'Gourmet Store', 'Electronics Store', 'Organic Grocery', 'Clothing Store', 'Toy Store', 'Department Store', 'Gift Store', 'Convenience Store', 'Shopping Mall', 'Supermarket', 'Retail', 'Shopping Plaza', 'Discount Store', 'Fruit and Vegetable Store', 'Furniture and Home Store', 'Video Games Store', 'Office Supply Store', 'Tobacco Store', 'Liquor Store', 'Beer Store', 'Warehouse or Wholesale Store'],
        'Entertainment & Leisure': ['Night Club', 'Movie Theater', 'Arcade', 'Bowling Alley', 'Amusement Park', 'Pool Hall', 'Event Space', 'Karaoke Bar', 'Sports Bar', 'Lounge', 'Beer Garden', 'Beer Bar', 'Stadium', 'Rock Climbing Spot'],
        'Landmarks & Outdoor Activities': ['Landmarks and Outdoors', 'Monument', 'Bridge'],
        'Bars & Clubs': ['Bar', 'Gay Bar', 'Dive Bar', 'Cocktail Bar', 'Beer Bar', 'Hotel Bar', 'Beer Garden', 'Hookah Bar', 'Wine Store', 'Night Club']
    }

    # Función para mapear las categorías a los nuevos grupos
    def map_category(category):
        for group, categories in category_mapping.items():
            if category in categories:
                return group
        return 'Other'  # Si no está en ninguna de las categorías definidas

    # Mapear la columna 'category' a 'category_group'
    df['category_group'] = df['category'].apply(map_category)

    # Guardar el DataFrame transformado
    df.to_csv(output_file_path, index=False)
    print(f"Transformed data saved to {output_file_path}")
