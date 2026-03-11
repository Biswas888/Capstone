import pandas as pd # <-- Make sure this is imported
import mysql.connector
from flask import Flask, jsonify, send_from_directory
import requests
from datetime import datetime
import os
from flask_cors import CORS

# --- Configuration ---
API_KEY = os.getenv("API_KEY")
CITY = "Akron"

DB_CONFIG = {
    "host": os.getenv("MYSQL_HOST", "mysql-db"), 
    "user": os.getenv("MYSQL_USER", "root"),
    "password": os.getenv("MYSQL_PASSWORD", "root"),
    "database": os.getenv("MYSQL_DB", "weather_sales"),
    "port": 3306
}

FRONTEND_FOLDER = "/app/frontend"
CSV_PATH = "/app/raw/forecast_ready_dataset.csv"

app = Flask(__name__)
CORS(app)

# --------------------------
# 📂 Static File Serving (FIXED)
# --------------------------

@app.route("/")
def root_index():
    return send_from_directory(FRONTEND_FOLDER, "index.html")

# This catches frontend
@app.route("/<path:filename>")
@app.route("/frontend/<path:filename>")
def serve_static(filename):
    return send_from_directory(FRONTEND_FOLDER, filename)

# --------------------------
#  Data Sync Logic (New!)
# --------------------------
def sync_csv_to_db():
    """Reads the CSV and updates MySQL."""
    if os.path.exists(CSV_PATH):
        try:
            df = pd.read_csv(CSV_PATH)
            db = mysql.connector.connect(**DB_CONFIG)
            cursor = db.cursor()
            for _, row in df.iterrows():
                # Adjust column names if your CSV is different
                sql = """
                    INSERT INTO sales_features (date, total_quantity, total_revenue, city)
                    VALUES (%s, %s, %s, %s)
                    ON DUPLICATE KEY UPDATE total_quantity=VALUES(total_quantity)
                """
                cursor.execute(sql, (row['date'], row['total_quantity'], row['total_revenue'], 'Akron'))
            db.commit()
            cursor.close()
            db.close()
            print("CSV synced successfully.")
        except Exception as e:
            print(f"CSV Sync Error: {e}")

# --------------------------
# API Endpoints
# --------------------------
@app.route("/api/combined")
def combined():
    # Sync the CSV data every time the button is clicked
    sync_csv_to_db()
    
    try:
        db = mysql.connector.connect(**DB_CONFIG)
        cursor = db.cursor(dictionary=True)
        query = """
            SELECT s.*, w.avg_temp, w.humidity, w.description
            FROM sales_features s
            LEFT JOIN weather_data w ON s.date = w.date
            ORDER BY s.date DESC
        """
        cursor.execute(query)
        rows = cursor.fetchall()
        cursor.close()
        db.close()
        return jsonify(rows)
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route("/api/prediction_input")
def prediction_input():
    try:
        db = mysql.connector.connect(**DB_CONFIG)
        cursor = db.cursor(dictionary=True)
        cursor.execute("SELECT date, total_quantity FROM sales_features ORDER BY date DESC LIMIT 1")
        latest_sales = cursor.fetchone() or {"date": "No Data", "total_quantity": 0}
        
        # Current weather fetch
        url = f"https://api.openweathermap.org/data/2.5/weather?q={CITY}&units=metric&appid={API_KEY}"
        weather_data = requests.get(url, timeout=5).json()
        current_weather = {
            "temperature": weather_data["main"]["temp"] if "main" in weather_data else 15,
            "humidity": weather_data["main"]["humidity"] if "main" in weather_data else 50
        }

        cursor.close()
        db.close()
        return jsonify({"latest_sales_features": latest_sales, "current_weather": current_weather})
    except Exception as e:
        return jsonify({"error": str(e)}), 500

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5050, debug=True)
