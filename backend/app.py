import mysql.connector
from flask import Flask, jsonify
import requests
from urllib.parse import quote
from datetime import datetime
from flask_cors import CORS

app = Flask(__name__)
CORS(app)  # Allow cross-origin requests

API_KEY = "b36085dbd254423b05c7ef133ac094cc"


# --------------------------
# MySQL connection
# --------------------------
DB_CONFIG = {
    "host": "localhost",
    "user": "root",
    "password": "root",
    "database": "weather_sales",
    "port": 3308
}

# --------------------------
# Route: Home
# --------------------------
@app.route("/")
def home():
    return "Weather Sales API Running"

# --------------------------
# Route: Test backend
# --------------------------
@app.route("/api/test")
def test():
    return jsonify({"message": "Backend working!"})

# --------------------------
# Route: Get sales data
# --------------------------
@app.route("/api/sales")
def get_sales():
    try:
        db = mysql.connector.connect(**DB_CONFIG)  # new connection
        cursor = db.cursor(dictionary=True)
        cursor.execute("SELECT * FROM sales LIMIT 50")
        rows = cursor.fetchall()
        cursor.close()
        db.close()  # close after use
        return jsonify(rows)
    except mysql.connector.Error as e:
        return jsonify({"error": str(e)}), 500

# --------------------------
# Route: Fetch & insert weather data into DB
# --------------------------
# --------------------------
# Route: Fetch & insert weather data into DB (Forecast)
# --------------------------
@app.route("/api/load_weather/<city>")
def load_weather(city):
    try:
        city_encoded = quote(city)

        # Fetch 5-day forecast from OpenWeatherMap
        forecast_url = f"https://api.openweathermap.org/data/2.5/forecast?q={city_encoded}&units=metric&appid={API_KEY}"
        forecast_resp = requests.get(forecast_url, timeout=10).json()

        if forecast_resp.get("cod") != "200":
            return jsonify({"error": forecast_resp.get("message", "Forecast not available")}), 404

        # Aggregate 3-hour forecast into daily summaries
        daily_forecast = {}
        for entry in forecast_resp["list"]:
            date = datetime.utcfromtimestamp(entry["dt"]).strftime("%Y-%m-%d")
            temp = entry["main"]["temp"]
            humidity = entry["main"]["humidity"]
            wind_speed = entry["wind"]["speed"]
            weather_desc = entry["weather"][0]["description"]
            rainfall = entry.get("rain", {}).get("3h", 0)  # mm

            if date not in daily_forecast:
                daily_forecast[date] = {
                    "temps": [],
                    "descriptions": [],
                    "humidity": [],
                    "rainfall": [],
                    "wind_speed": []
                }

            daily_forecast[date]["temps"].append(temp)
            daily_forecast[date]["descriptions"].append(weather_desc)
            daily_forecast[date]["humidity"].append(humidity)
            daily_forecast[date]["rainfall"].append(rainfall)
            daily_forecast[date]["wind_speed"].append(wind_speed)

        # Insert aggregated data into DB
        cursor = db.cursor()
        for date, info in daily_forecast.items():
            avg_temp = round(sum(info["temps"]) / len(info["temps"]), 2)
            min_temp = round(min(info["temps"]), 2)
            max_temp = round(max(info["temps"]), 2)
            avg_humidity = int(sum(info["humidity"]) / len(info["humidity"]))
            avg_wind_speed = round(sum(info["wind_speed"]) / len(info["wind_speed"]), 2)
            total_rainfall = round(sum(info["rainfall"]), 2)
            desc = max(set(info["descriptions"]), key=info["descriptions"].count)

            insert_query = """
                INSERT INTO weather_data 
                    (city, date, avg_temp, min_temp, max_temp, description, rainfall, humidity, wind_speed, forecast_type)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON DUPLICATE KEY UPDATE
                    avg_temp=VALUES(avg_temp),
                    min_temp=VALUES(min_temp),
                    max_temp=VALUES(max_temp),
                    description=VALUES(description),
                    rainfall=VALUES(rainfall),
                    humidity=VALUES(humidity),
                    wind_speed=VALUES(wind_speed)
            """
            cursor.execute(insert_query, (
                forecast_resp["city"]["name"],
                date,
                avg_temp,
                min_temp,
                max_temp,
                desc,
                total_rainfall,
                avg_humidity,
                avg_wind_speed,
                'forecast'   # this marks it as future forecast
            ))

        db.commit()
        cursor.close()
        return jsonify({"message": f"Forecast weather data for {city} loaded successfully!"})

    except requests.exceptions.RequestException as e:
        return jsonify({"error": "Failed to reach OpenWeather API", "details": str(e)}), 500

    except Exception as e:
        return jsonify({"error": "Unexpected error", "details": str(e)}), 500

# --------------------------
# Route: Combined sales + weather data
# --------------------------
@app.route("/api/combined/<city>")
def combined_data(city):
    try:
        db = mysql.connector.connect(**DB_CONFIG)
        cursor = db.cursor(dictionary=True)

        query = """
            SELECT 
                s.sale_date,
                SUM(s.total) as total_sales,
                w.avg_temp,
                w.description,
                w.humidity,
                w.rainfall
            FROM sales s
            LEFT JOIN weather_data w
                ON s.sale_date = w.date AND w.city = %s
            GROUP BY s.sale_date, w.avg_temp, w.description, w.humidity, w.rainfall
            ORDER BY s.sale_date
        """

        cursor.execute(query, (city,))
        data = cursor.fetchall()
        cursor.close()
        db.close()
        return jsonify(data)
    
    except mysql.connector.Error as e:
        return jsonify({"error": str(e)}), 500
    
# --------------------------
# Route: Load forecast for all cities
# --------------------------
@app.route("/api/load_weather_forecast_all")
def load_weather_forecast_all():
    try:
        cursor = db.cursor()
        cursor.execute("SELECT DISTINCT city FROM sales")
        cities = [row[0] for row in cursor.fetchall()]

        for city in cities:
            city_encoded = quote(city)
            forecast_url = f"https://api.openweathermap.org/data/2.5/forecast?q={city_encoded}&units=metric&appid={API_KEY}"
            forecast_resp = requests.get(forecast_url, timeout=10).json()

            if forecast_resp.get("cod") != "200":
                print(f"Skipping {city}: {forecast_resp.get('message', 'Forecast not available')}")
                continue

            daily_forecast = {}
            for entry in forecast_resp["list"]:
                date = datetime.utcfromtimestamp(entry["dt"]).strftime("%Y-%m-%d")
                temp = entry["main"]["temp"]
                humidity = entry["main"]["humidity"]
                wind_speed = entry["wind"]["speed"]
                desc = entry["weather"][0]["description"]
                rainfall = entry.get("rain", {}).get("3h", 0)

                if date not in daily_forecast:
                    daily_forecast[date] = {"temps": [], "descriptions": [], "humidity": [], "rainfall": [], "wind_speed": []}

                daily_forecast[date]["temps"].append(temp)
                daily_forecast[date]["descriptions"].append(desc)
                daily_forecast[date]["humidity"].append(humidity)
                daily_forecast[date]["rainfall"].append(rainfall)
                daily_forecast[date]["wind_speed"].append(wind_speed)

            # Insert into DB
            for date, info in daily_forecast.items():
                avg_temp = round(sum(info["temps"]) / len(info["temps"]), 2)
                min_temp = round(min(info["temps"]), 2)
                max_temp = round(max(info["temps"]), 2)
                avg_humidity = int(sum(info["humidity"]) / len(info["humidity"]))
                avg_wind_speed = round(sum(info["wind_speed"]) / len(info["wind_speed"]), 2)
                total_rainfall = round(sum(info["rainfall"]), 2)
                desc = max(set(info["descriptions"]), key=info["descriptions"].count)

                insert_query = """
                    INSERT INTO weather_data 
                        (city, date, avg_temp, min_temp, max_temp, description, rainfall, humidity, wind_speed, forecast_type)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    ON DUPLICATE KEY UPDATE
                        avg_temp=VALUES(avg_temp),
                        min_temp=VALUES(min_temp),
                        max_temp=VALUES(max_temp),
                        description=VALUES(description),
                        rainfall=VALUES(rainfall),
                        humidity=VALUES(humidity),
                        wind_speed=VALUES(wind_speed)
                """
                cursor.execute(insert_query, (city, date, avg_temp, min_temp, max_temp, desc, total_rainfall, avg_humidity, avg_wind_speed, 'forecast'))

            print(f"Forecast loaded for {city}")

        db.commit()
        cursor.close()
        return jsonify({"message": "Forecast weather data loaded for all cities!"})

    except Exception as e:
        return jsonify({"error": "Failed to load forecast weather for all cities", "details": str(e)}), 500

# --------------------------
# Route: Load historical weather for all sales dates
# --------------------------
@app.route("/api/load_weather_historical_all")
def load_weather_historical_all():
    try:
        db = mysql.connector.connect(**DB_CONFIG)  # <-- create DB connection
        cursor = db.cursor()
        cursor.execute("SELECT DISTINCT city FROM sales")
        cities = [row[0] for row in cursor.fetchall()]

        for city in cities:
            cursor.execute("SELECT DISTINCT sale_date FROM sales WHERE city=%s", (city,))
            dates = [row[0] for row in cursor.fetchall()]

            for date in dates:
                # Dummy example weather data
                avg_temp = 25.0
                min_temp = 22.0
                max_temp = 28.0
                humidity = 60
                wind_speed = 3.5
                rainfall = 0
                desc = "clear"

                insert_query = """
                    INSERT INTO weather_data
                        (city, date, avg_temp, min_temp, max_temp, description, rainfall, humidity, wind_speed, forecast_type)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    ON DUPLICATE KEY UPDATE
                        avg_temp=VALUES(avg_temp),
                        min_temp=VALUES(min_temp),
                        max_temp=VALUES(max_temp),
                        description=VALUES(description),
                        rainfall=VALUES(rainfall),
                        humidity=VALUES(humidity),
                        wind_speed=VALUES(wind_speed)
                """
                cursor.execute(insert_query, (city, date, avg_temp, min_temp, max_temp, desc, rainfall, humidity, wind_speed, 'historical'))

            print(f"Historical weather loaded for {city}")

        db.commit()
        cursor.close()
        db.close()  # <-- close the connection
        return jsonify({"message": "Historical weather data loaded for all sales cities!"})

    except Exception as e:
        return jsonify({"error": "Failed to load historical weather", "details": str(e)}), 500
    
# --------------------------
# Run Flask app
# --------------------------
if __name__ == "__main__":
    app.run(debug=True, port=5050)