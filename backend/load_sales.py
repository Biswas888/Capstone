import mysql.connector
import pandas as pd
import requests
import os
from urllib.parse import quote
from dotenv import load_dotenv
from datetime import datetime, timedelta

# ---------------------------
# MySQL connection
# ---------------------------
DB_CONFIG = {
    "host": os.getenv("MYSQL_HOST", "mysql-db"), 
    "user": os.getenv("MYSQL_USER", "root"),
    "password": os.getenv("MYSQL_PASSWORD", "root"),
    "database": os.getenv("MYSQL_DB", "weather_sales"),
    "port": 3306  # Internal Docker port
}

API_KEY = os.getenv("API_KEY")
CITY = "Akron" 

# ---------------------------
# Load Raw Sales Data 
# ---------------------------
if os.path.exists("forecast_ready_dataset.csv"):
    raw_sales_df = pd.read_csv("forecast_ready_dataset.csv")
    
    # Push this raw data into MySQL so the 'SELECT' query in Step 1 actually finds something
    db = mysql.connector.connect(**DB_CONFIG)
    cursor = db.cursor()
    for _, row in raw_sales_df.iterrows():
        cursor.execute("""
            INSERT INTO sales_features (date, total_quantity, total_revenue, city)
            VALUES (%s, %s, %s, %s)
            ON DUPLICATE KEY UPDATE total_quantity=VALUES(total_quantity)
        """, (row['date'], row['total_quantity'], row['total_revenue'], 'Akron'))
    db.commit()
    cursor.close()
    db.close()
    print("Initial sales data pushed to database.")

# ---------------------------
# Function: Fetch forecast weather
# ---------------------------
def fetch_forecast_weather(city, api_key):
    city_encoded = quote(city)
    url = f"https://api.openweathermap.org/data/2.5/forecast?q={city_encoded}&units=metric&appid={api_key}"
    resp = requests.get(url, timeout=10).json()

    daily_forecast = {}
    if resp.get("cod") != "200":
        print(f"Forecast not available for {city}: {resp.get('message')}")
        return pd.DataFrame()

    for entry in resp["list"]:
        date = datetime.utcfromtimestamp(entry["dt"]).strftime("%Y-%m-%d")
        temp = entry["main"]["temp"]
        min_temp = entry["main"]["temp_min"]
        max_temp = entry["main"]["temp_max"]
        humidity = entry["main"]["humidity"]
        wind_speed = entry["wind"]["speed"]
        rainfall = entry.get("rain", {}).get("3h", 0)
        description = entry["weather"][0]["description"]

        if date not in daily_forecast:
            daily_forecast[date] = {
                "avg_temp": [],
                "min_temp": [],
                "max_temp": [],
                "humidity": [],
                "wind_speed": [],
                "rainfall": [],
                "description": []
            }

        daily_forecast[date]["avg_temp"].append(temp)
        daily_forecast[date]["min_temp"].append(min_temp)
        daily_forecast[date]["max_temp"].append(max_temp)
        daily_forecast[date]["humidity"].append(humidity)
        daily_forecast[date]["wind_speed"].append(wind_speed)
        daily_forecast[date]["rainfall"].append(rainfall)
        daily_forecast[date]["description"].append(description)

    # Aggregate daily data
    forecast_rows = []
    for date, values in daily_forecast.items():
        forecast_rows.append({
            "date": date,
            "avg_temp": round(sum(values["avg_temp"])/len(values["avg_temp"]), 2),
            "min_temp": round(min(values["min_temp"]), 2),
            "max_temp": round(max(values["max_temp"]), 2),
            "humidity": int(sum(values["humidity"])/len(values["humidity"])),
            "wind_speed": round(sum(values["wind_speed"])/len(values["wind_speed"]), 2),
            "rainfall": round(sum(values["rainfall"]), 2),
            "description": max(set(values["description"]), key=values["description"].count),
            "forecast_type": "forecast"
        })

    return pd.DataFrame(forecast_rows)

# ---------------------------
# Step 1: Connect to DB and get historical sales + weather
# ---------------------------
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
ORDER BY s.date
"""
cursor.execute(query)
data = cursor.fetchall()
cursor.close()
db.close()

df = pd.DataFrame(data)

# ---------------------------
# Step 2: Fill missing weather data with historical forward fill
# ---------------------------
df['avg_temp'] = df['avg_temp'].ffill()
df['humidity'] = df['humidity'].ffill()
df['wind_speed'] = df['wind_speed'].ffill()
df['rainfall'] = df['rainfall'].fillna(0)

# ---------------------------
# Step 3: Fetch forecast for next 5 days and append
# ---------------------------
forecast_df = fetch_forecast_weather(CITY, API_KEY)
if not forecast_df.empty:
    # Add empty sales features for forecast dates
    forecast_df_features = pd.DataFrame({
        "date": forecast_df["date"],
        "total_quantity": None,
        "total_revenue": None,
        "Baby": 0,
        "Baking/ Spices/ Condiments": 0,
        "Beverages": 0,
        "Cleaning": 0,
        "Dairy": 0,
        "Food": 0,
        "Fruits": 0,
        "Hygiene": 0,
        "Meat": 0,
        "Miscellaneous": 0,
        "Pet": 0,
        "School Supplies": 0,
        "Snacks": 0,
        "Vegetables": 0,
        "day_of_week": pd.to_datetime(forecast_df["date"]).dt.dayofweek,
        "month": pd.to_datetime(forecast_df["date"]).dt.month,
        "is_weekend": pd.to_datetime(forecast_df["date"]).dt.dayofweek.isin([5,6]).astype(int),
        "lag_1": None,
        "lag_3": None,
        "lag_7": None,
        "rolling_3": None,
        "rolling_7": None
    })

    forecast_combined = pd.merge(forecast_df_features, forecast_df, on="date", how="left")
    df = pd.concat([df, forecast_combined], ignore_index=True, sort=False)

# ---------------------------
# Step 4: Save final forecast-ready dataset
# ---------------------------
df.to_csv("forecast_ready_dataset.csv", index=False)
print("Forecast-ready dataset saved as 'forecast_ready_dataset.csv'")