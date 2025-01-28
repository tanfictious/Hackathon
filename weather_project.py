import requests
import psycopg2
import os
from dotenv import load_dotenv
from datetime import datetime

# Load environment variables
load_dotenv()

# API key and endpoint setup
API_KEY = os.getenv('OPENWEATHER_API_KEY')
BASE_URL = "https://api.openweathermap.org/data/2.5/weather"

# Database connection
try:
    connection = psycopg2.connect(
        dbname=os.getenv('DB_NAME'),
        user=os.getenv('DB_USER'),
        password=os.getenv('DB_PASSWORD'),
        host=os.getenv('DB_HOST'),
        port=os.getenv('DB_PORT')
    )
    cursor = connection.cursor()
    print("Database connected successfully.")
except Exception as e:
    print(f"Error connecting to the database: {e}")
    exit()

# Function to fetch weather data
def fetch_weather(city_name):
    params = {
        "q": city_name,
        "appid": API_KEY,
        "units": "metric"
    }
    response = requests.get(BASE_URL, params=params)
    if response.status_code == 200:
        return response.json()
    else:
        print(f"Error fetching weather data: {response.status_code}")
        return None

# Function to save weather data to the database
def save_weather_data(data):
    try:
        # Create table if it doesn't exist
        cursor.execute("""
        CREATE TABLE IF NOT EXISTS weather_data (
            id SERIAL PRIMARY KEY,
            temperature DECIMAL,
            humidity DECIMAL,
            pressure DECIMAL,
            wind_speed DECIMAL,
            timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            description TEXT
        );
        """)
        
        query = """
        INSERT INTO weather_data (temperature, humidity, pressure, wind_speed, description)
        VALUES (%s, %s, %s, %s, %s);
        """
        values = (
            data['main']['temp'],
            data['main']['humidity'],
            data['main']['pressure'],
            data['wind']['speed'],
            data['weather'][0]['description']
        )
        cursor.execute(query, values)
        connection.commit()
        print(f"Weather data saved successfully.")
    except Exception as e:
        print(f"Error saving data to the database: {e}")

def main():
    try:
        while True:
            city = input("Enter a city name (or 'quit' to exit): ")
            if city.lower() == 'quit':
                break
                
            weather_data = fetch_weather(city)
            if weather_data:
                save_weather_data(weather_data)
                
    finally:
        if connection:
            cursor.close()
            connection.close()
            print("Database connection closed.")

if __name__ == "__main__":
    main()