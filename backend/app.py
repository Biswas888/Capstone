import mysql.connector
from flask import Flask, jsonify, send_from_directory
import requests
from datetime import datetime
import os
from flask_cors import CORS

API_KEY = "b36085dbd254423b05c7ef133ac094cc"
CITY = "Akron"

DB_CONFIG = {
    "host": os.getenv("MYSQL_HOST", "localhost"),  # use container network name
    "user": os.getenv("MYSQL_USER", "root"),
    "password": os.getenv("MYSQL_PASSWORD", "root"),
    "database": os.getenv("MYSQL_DB", "weather_sales"),
    "port": 3306
}

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
# Absolute path to the 'frontend' directory
FRONTEND_FOLDER = os.path.join(BASE_DIR, 'frontend')

app = Flask(__name__)
CORS(app)

@app.route("/")
def root_index():
    # Look for index.html inside /app/frontend
    return send_from_directory(FRONTEND_FOLDER, "index.html")

@app.route("/frontend/<path:filename>")
def serve_static(filename):
    # Look for main.js or styles.css inside /app/frontend
    return send_from_directory(FRONTEND_FOLDER, filename)

# --------------------------
# 1️⃣ Get Sales Feature Dataset
# --------------------------
@app.route("/api/sales")
def get_sales():
    try:
        db = mysql.connector.connect(**DB_CONFIG)
        cursor = db.cursor(dictionary=True)

        cursor.execute("""
            SELECT *
            FROM sales_features
            ORDER BY date
        """)

        rows = cursor.fetchall()
        cursor.close()
        db.close()

        return jsonify(rows)

    except mysql.connector.Error as e:
        return jsonify({"error": str(e)}), 500


# --------------------------
# 2️⃣ Get Current Weather (Live API)
# --------------------------
@app.route("/api/weather/current")
def get_current_weather():
    try:
        url = f"https://api.openweathermap.org/data/2.5/weather?q={CITY}&units=metric&appid={API_KEY}"
        response = requests.get(url, timeout=10).json()

        if response.get("cod") != 200:
            return jsonify({"error": response.get("message")}), 400

        weather = {
            "date": datetime.utcnow().strftime("%Y-%m-%d"),
            "temperature": response["main"]["temp"],
            "humidity": response["main"]["humidity"],
            "wind_speed": response["wind"]["speed"],
            "description": response["weather"][0]["description"]
        }

        return jsonify(weather)

    except Exception as e:
        return jsonify({"error": str(e)}), 500


# --------------------------
# 3️⃣ Combined Sales + Weather (For Training / Analysis)
# --------------------------
@app.route("/api/combined")
def combined():
    try:
        db = mysql.connector.connect(**DB_CONFIG)
        cursor = db.cursor(dictionary=True)

        query = """
            SELECT 
                s.*,
                w.avg_temp,
                w.min_temp,
                w.max_temp,
                w.humidity,
                w.rainfall,
                w.wind_speed,
                w.description,
                w.forecast_type
            FROM sales_features s
            LEFT JOIN weather_data w
                ON s.date = w.date
                AND w.city = %s
            ORDER BY s.date
        """

        cursor.execute(query, (CITY,))
        rows = cursor.fetchall()

        cursor.close()
        db.close()

        return jsonify(rows)

    except mysql.connector.Error as e:
        return jsonify({"error": str(e)}), 500

@app.route("/api/simple_prediction")
def simple_prediction():
    try:
        db = mysql.connector.connect(**DB_CONFIG)
        cursor = db.cursor(dictionary=True)

        # Latest sales row
        cursor.execute("SELECT * FROM sales_features ORDER BY date DESC LIMIT 1")
        latest = cursor.fetchone()
        cursor.close()
        db.close()

        # Use latest avg_temp (dummy logic)
        predicted_sales = int(latest['total_quantity'] * 1.05)  # +5% as example

        return jsonify({
            "latest_date": latest['date'],
            "latest_total_quantity": latest['total_quantity'],
            "predicted_next_sales": predicted_sales
        })

    except Exception as e:
        return jsonify({"error": str(e)}), 500

# --------------------------
# 4️⃣ Prediction Input (Latest Row + Live Weather)
# --------------------------
@app.route("/api/prediction_input")
def prediction_input():
    try:
        db = mysql.connector.connect(**DB_CONFIG)
        cursor = db.cursor(dictionary=True)

        cursor.execute("""
            SELECT *
            FROM sales_features
            ORDER BY date DESC
            LIMIT 1
        """)

        latest_row = cursor.fetchone()

        cursor.close()
        db.close()

        # Fetch live weather
        url = f"https://api.openweathermap.org/data/2.5/weather?q={CITY}&units=metric&appid={API_KEY}"
        response = requests.get(url, timeout=10).json()

        weather = {
            "temperature": response["main"]["temp"],
            "humidity": response["main"]["humidity"],
            "wind_speed": response["wind"]["speed"]
        }

        return jsonify({
            "latest_sales_features": latest_row,
            "current_weather": weather
        })

    except Exception as e:
        return jsonify({"error": str(e)}), 500


# --------------------------
# Run App
# --------------------------
if __name__ == "__main__":
    app.run(debug=True, host="0.0.0.0", port=5050)