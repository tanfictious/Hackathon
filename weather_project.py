import requests
import psycopg2

# API key and endpoint setup
API_KEY = "c20a298f91e5fb5f8dd6528574d09023"  # Your API key
BASE_URL = "https://api.openweathermap.org/data/2.5/weather"

# Database connection
try:
    connection = psycopg2.connect(
        dbname="weather_db",          # Database name
        user="tanmaykajrolkar",       # PostgreSQL username
        password="tanmay@iscool123",  # Your PostgreSQL password
        host="localhost",
        port="5432"
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
        # Ensure the table exists before inserting data
        cursor.execute("""
        CREATE TABLE IF NOT EXISTS weather (
            id SERIAL PRIMARY KEY,
            city VARCHAR(100),
            temperature DECIMAL,
            description TEXT
        );
        """)
        
        query = """
        INSERT INTO weather (city, temperature, description)
        VALUES (%s, %s, %s);
        """
        values = (
            data['name'],                 # City name
            data['main']['temp'],         # Temperature
            data['weather'][0]['description']  # Weather description
        )
        cursor.execute(query, values)
        connection.commit()
        print(f"Weather data for {data['name']} saved successfully.")
    except Exception as e:
        print(f"Error saving data to the database: {e}")

# Main program
if __name__ == "__main__":
    city = input("Enter a city name: ")
    weather_data = fetch_weather(city)
    if weather_data:
        save_weather_data(weather_data)

# Close database connection
if connection:
    cursor.close()
    connection.close()
    print("Database connection closed.")