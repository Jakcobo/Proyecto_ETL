# Proyecto_ETL/src/transform/api_transform.py
import pandas as pd
import logging
import os

logger = logging.getLogger(__name__)

def clean_api_data(df_input: pd.DataFrame) -> pd.DataFrame:
    """
    Limpia y transforma el DataFrame de Foursquare (considerado como datos de API).
    Esta función es la principal para la tarea de transformación de API.

    Args:
        df_input (pd.DataFrame): DataFrame crudo de Foursquare.

    Returns:
        pd.DataFrame: DataFrame transformado y limpio.
    """
    df_clean = df_input[df_input['region'].isin(['NJ', 'CT', 'PA'])]
    logger.info("Número de registros en df: %s", len(df_clean))


    category_mapping = {
        'cultural': [
            'Art Museum', 'Art Gallery', 'History Museum', 'Museum', 'Public Art',
            'Cultural Center', 'Indie Movie Theater', 'Music Venue', 'Performing Arts Venue', 'Theater', 'Dance Studio', 'Science Museum', 'Sculpture Garden', 'Concert Hall',
            'Exhibit', 'Opera House', 'Amphitheater', 'Memorial Site', 'Jazz and Blues Venue','Church', 'Attraction'
        ],

        'restaurants': [
            'Italian Restaurant', 'Fast Food Restaurant', 'Korean Restaurant', 'Thai Restaurant', 'Wine Bar', 'Brazilian Restaurant', 'Pizzeria', 'American Restaurant', 'French Restaurant', 'Mexican Restaurant', 
            'Chinese Restaurant', 'Latin American Restaurant', 'Greek Restaurant', 'Burger Joint', 'Deli', 'Cocktail Bar', 'Wine Store', 'Dessert Shop', 'Sandwich Spot', 'Seafood Restaurant', 'Café', 
            'Gastropub', 'Fried Chicken Joint', 'Coffee Shop', 'Pub', 'Kosher Restaurant', 'Cantonese Restaurant', 'Asian Restaurant', 'German Restaurant', 'Wings Joint', 'Irish Pub', 'Restaurant', 'Steakhouse', 
            'Bagel Shop', 'New American Restaurant', 'Japanese Restaurant', 'Tea Room', 'Hot Dog Joint', 'Taiwanese Restaurant', 'Salad Restaurant', 'Middle Eastern Restaurant', 'Buffet', 
            'South American Restaurant', 'Bubble Tea Shop', 'Peruvian Restaurant', 'Vietnamese Restaurant', 'Diner', 'BBQ Joint', 'Breakfast Spot', 'Caribbean Restaurant', 'Sushi Restaurant', 
            'Cha Chaan Teng', 'Taco Restaurant', 'Ice Cream Parlor', 'Donut Shop', 'Tapas Restaurant', 
            'Portuguese Restaurant', 'Japanese Curry Restaurant', 'Comfort Food Restaurant', 'Falafel Restaurant', 'Cuban Restaurant', 'Gelato Shop', 'Spanish Restaurant', 'Ramen Restaurant', 
            'Churrascaria', 'Food Truck', 'Tex-Mex Restaurant', 'Dining and Drinking', 'Pie Shop'
        ],

        'parks_&_outdoor': [
            'Urban Park', 'Lake', 'Park', 'Playground', 'State or Provincial Park', 'Botanical Garden', 'Picnic Area', 'Beach', 'Soccer Field', 'National Park', 'Hiking Trail', 'Campground', 
            'Scenic Lookout', 'Dog Park', 'Zoo', 'Zoo Exhibit', 'Garden', 'Farm', 'Bike Trail', 'Fountain', 'Harbor or Marina', 'Pier', 'Forest', 'River', 'Roof Deck', 'Surf Spot', 
            'Plaza', 'Other Great Outdoors', 'Nature Preserve', 'Bay', 'Bathing Area', 'Stable'
        ],

        'retail_&_shopping': [
            'Bakery', 'Grocery Store', 'Big Box Store', 'Gourmet Store', 'Electronics Store', 'Organic Grocery', 'Clothing Store', 'Toy Store', 'Department Store', 'Gift Store', 
            'Convenience Store', 'Shopping Mall', 'Supermarket', 'Retail', 'Shopping Plaza', 'Discount Store', 'Fruit and Vegetable Store', 'Furniture and Home Store', 
            'Video Games Store', 'Office Supply Store', 'Tobacco Store', 'Liquor Store', 'Beer Store', 'Warehouse or Wholesale Store', 'Candy Store', 'Eyecare Store', 'Drugstore','Hardware Store', 'Arts and Crafts Store', 'Bookstore', 'Garden Center', 
            'Computers and Electronics Retail', 'Shoe Store', 'Flower Store', 'Fish Market','Cheese Store', 'Jewelry Store', 'Vintage and Thrift Store', 'Miscellaneous Store', 
            "Women's Store", 'Sporting Goods Retail', 'Bicycle Store', 'Supplement Store','Mobile Phone Store', 'Print Store', 'Electrical Equipment Supplier', 'Textiles Store', 
            'Fashion Accessories Store', 'Framing Store', 'Music Store', 'Chocolate Store','Herbs and Spices Store', 'Mattress Store', "Children's Clothing Store", 'Butcher', 
            'Meat and Seafood Store', 'Food and Beverage Retail', 'Fuel Station'
        ],

        'entertainment_&_leisure': [
            'Movie Theater', 'Arcade', 'Bowling Alley', 'Amusement Park', 'Pool Hall', 'Event Space', 'Karaoke Bar', 'Sports Bar', 'Lounge', 'Beer Garden', 'Stadium', 
            'Rock Climbing Spot', 'Comedy Club', 'Casino', 'Skating Rink', 'Escape Room', 'Circus', 'Go Kart Track', 'Drive-in Theater', 'Bingo Center', 'Carnival', 
            'Gaming Cafe', 'Psychic and Astrologer', 'Resort', 'Motel', 'Lodging', 'Spa','Boat Rental', 'Bike Rental', 'Tour Provider', 'Travel Agency', 'Boarding House'
        ],

        'landmarks': [
            'Landmarks and Outdoors', 'Monument', 'Bridge', 'Historic and Protected Site','Parking', 'Metro Station', 'Rail Station', 'Transport Hub', 'Bus Station', 
            'Travel and Transportation', 'Office Building', 'Tunnel', 'Port', 'Platform',  'Airport Lounge', 'Airport', 'Rental Car Location', 'Airport Service', 
            'Airport Tram Station', 'Baggage Claim', 'Border Crossing', 'Airport Terminal', 'Transportation Service', 'Taxi Stand', 'Public Transportation', 'Office', 
            'Residential Building'
        ],

        'bars_&_clubs': [
            'Bar', 'Gay Bar', 'Dive Bar', 'Cocktail Bar', 'Beer Bar', 'Hotel Bar', 'Beer Garden', 'Hookah Bar', 'Wine Store', 'Night Club', 'Rock Club', 
            'Comedy Club', 'Speakeasy', 'Whisky Bar', 'Strip Club'
        ]
    }

    def map_category(category):
        for group, categories in category_mapping.items():
            if category in categories:
                return group
        return 'Other'  

    df_clean['category_group'] = df_clean['category_main_name'].apply(map_category)
    
    df_clean = df_clean.drop(columns=['price_tier', 'tips_count','popularity_score','address','category_main_id', 'category_main_name', 'all_categories','rating','region','area_searched', 'locality', 'name'])
    logger.info("Columnas eliminadas de df_clean")
    
    df_clean = df_clean.reset_index(drop=True)
    df_clean['fsq_id'] = df_clean.index + 1
    
    return df_clean