import psycopg2
from psycopg2.extras import RealDictCursor
import openai
import os
from dotenv import load_dotenv
from twilio.rest import Client
import json
from datetime import datetime
import time

# Load environment variables
load_dotenv()

# Configure OpenAI API
openai.api_key = os.getenv('OPENAI_API_KEY')

# Configure Twilio
TWILIO_ACCOUNT_SID = os.getenv('TWILIO_ACCOUNT_SID')
TWILIO_AUTH_TOKEN = os.getenv('TWILIO_AUTH_TOKEN')
TWILIO_FROM_NUMBER = os.getenv('TWILIO_FROM_NUMBER')
ALERT_TO_NUMBER = os.getenv('ALERT_TO_NUMBER')

class DatabaseManager:
    def __init__(self):
        self.conn = psycopg2.connect(
            dbname=os.getenv('DB_NAME'),
            user=os.getenv('DB_USER'),
            password=os.getenv('DB_PASSWORD'),
            host=os.getenv('DB_HOST'),
            port=os.getenv('DB_PORT'),
            cursor_factory=RealDictCursor
        )
        self.cursor = self.conn.cursor()

    def fetch_weather_data(self):
        """Fetch the latest weather data from the database"""
        query = """
        SELECT temperature, humidity, pressure, wind_speed, timestamp 
        FROM weather_data 
        ORDER BY timestamp DESC 
        LIMIT 20
        """
        self.cursor.execute(query)
        return self.cursor.fetchall()

    def close(self):
        """Close database connection"""
        self.cursor.close()
        self.conn.close()

class AnomalyDetector:
    def __init__(self):
        self.model = "gpt-4"  # Using GPT-4 for better anomaly detection

    def detect_anomalies(self, weather_data):
        """Detect anomalies in weather data using LLM"""
        # Convert weather data to a more readable format
        formatted_data = json.dumps(weather_data, indent=2, default=str)
        
        prompt = f"""
        Analyze the following weather data for anomalies:
        {formatted_data}

        Consider the following aspects:
        1. Sudden temperature changes
        2. Unusual humidity levels
        3. Extreme pressure variations
        4. Abnormal wind speeds
        5. Any patterns that deviate significantly from normal weather behavior

        If you find any anomalies, respond with a JSON object in this format:
        {{
            "anomalies_detected": true/false,
            "anomalies": [list of anomaly descriptions],
            "severity": "low"/"medium"/"high",
            "explanation": "detailed explanation"
        }}

        If no anomalies are found, return {{"anomalies_detected": false}}
        """

        response = openai.ChatCompletion.create(
            model=self.model,
            messages=[
                {"role": "system", "content": "You are a weather anomaly detection expert. Analyze data and return JSON responses only."},
                {"role": "user", "content": prompt}
            ]
        )
        
        return json.loads(response.choices[0].message.content)

def alert(message, severity):
    """Send alert using Twilio"""
    try:
        client = Client(TWILIO_ACCOUNT_SID, TWILIO_AUTH_TOKEN)
        
        # Format the alert message
        alert_text = f"""
        ⚠️ WEATHER ANOMALY DETECTED ⚠️
        Severity: {severity.upper()}
        Time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}
        
        Details: {message}
        """
        
        # Send SMS
        message = client.messages.create(
            body=alert_text,
            from_=TWILIO_FROM_NUMBER,
            to=ALERT_TO_NUMBER
        )
        
        print(f"Alert sent successfully! SID: {message.sid}")
        
    except Exception as e:
        print(f"Failed to send alert: {str(e)}")

def main():
    try:
        # Initialize database connection
        db = DatabaseManager()
        
        # Initialize anomaly detector
        detector = AnomalyDetector()
        
        while True:  # Continuous monitoring
            # Fetch real-time weather data
            weather_data = db.fetch_weather_data()
            
            # Analyze for anomalies
            analysis = detector.detect_anomalies(weather_data)
            
            if analysis.get('anomalies_detected'):
                # Construct alert message
                alert_message = "\n".join(analysis['anomalies'])
                
                # Send alert
                alert(alert_message, analysis['severity'])
            
            # Wait for 5 minutes before next check
            time.sleep(300)
            
    except Exception as e:
        print(f"Error: {str(e)}")
        
    finally:
        db.close()

if __name__ == "__main__":
    main()



