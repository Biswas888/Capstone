import mysql.connector
import pandas as pd
import requests
import os
from urllib.parse import quote
from dotenv import load_dotenv
from datetime import datetime

load_dotenv()

# ---------------------------
# MySQL connection
# ---------------------------
DB_CONFIG = {
    "host": os.getenv("MYSQL_HOST", "mysql-db"),  # container hostname
    "user": os.getenv("MYSQL_USER", "root"),
    "password": os.getenv("MYSQL_PASSWORD", "root"),
    "database": os.getenv("MYSQL_DB", "weather_sales"),
    "port": 3306
}

API_KEY = os.getenv("API_KEY")
CITY = "Akron"  

# ---------------------------
# Load historical sales
# ---------------------------
sales_csv_path = "backend/forecast_ready_dataset.csv"  # adjust path for container

if os.path.exists(sales_csv_path):
    sales_df = pd.read_csv(sales_csv_path)
    print(f"Loaded sales CSV: {sales_csv_path}")
else:
    # CSV not found → generate dummy sales data for demo
    print(f"{sales_csv_path} not found. Generating dummy sales data.")
    sales_df = pd.DataFrame({
        "date": pd.date_range("2023-01-01", "2023-01-10"),  # adjust range as needed
        "total_quantity": [0]*10,
        "total_revenue": [0]*10
    })

# Push sales into MySQL
db = mysql.connector.connect(**DB_CONFIG)
cursor = db.cursor()
for _, row in sales_df.iterrows():
    total_quantity = 0 if pd.isna(row['total_quantity']) else int(row['total_quantity'])
    total_revenue = 0.0 if pd.isna(row['total_revenue']) else float(row['total_revenue'])
    
    cursor.execute("""
        INSERT INTO sales_features (date, total_quantity, total_revenue, city)
        VALUES (%s, %s, %s, %s)
        ON DUPLICATE KEY UPDATE total_quantity=VALUES(total_quantity), total_revenue=VALUES(total_revenue)
    """, (row['date'], total_quantity, total_revenue, 'Akron'))

db.commit()
cursor.close()
db.close()
print("Historical sales loaded into MySQL.")

# ---------------------------
# Fetch historical weather from Open-Meteo
# ---------------------------
def fetch_historical_weather(start_date, end_date, latitude=41.0814, longitude=-81.5190):
    url = f"https://archive-api.open-meteo.com/v1/era5?latitude={latitude}&longitude={longitude}&start_date={start_date}&end_date={end_date}&hourly=temperature_2m,humidity_2m,rain,wind_speed_10m"
    resp = requests.get(url, timeout=10).json()
    
    if 'hourly' not in resp:
        print(f"No historical weather data returned from Open-Meteo. Response:\n{resp}")
        return pd.DataFrame()  # return empty dataframe

    # process hourly data...
    df = pd.DataFrame({
        "date": pd.to_datetime(resp['hourly']['time']).date,
        "avg_temp": resp['hourly']['temperature_2m'],
        "humidity": resp['hourly']['humidity_2m'],
        "rainfall": resp['hourly'].get('rain', [0]*len(resp['hourly']['time'])),
        "wind_speed": resp['hourly']['wind_speed_10m']
    })
    
    # aggregate to daily
    df['date'] = pd.to_datetime(df['date'])
    daily_df = df.groupby('date').agg({
        'avg_temp': 'mean',
        'humidity': 'mean',
        'rainfall': 'sum',
        'wind_speed': 'mean'
    }).reset_index()
    
    return daily_df

# Determine date range from sales data
start_date = sales_df['date'].min()
end_date = sales_df['date'].max()
weather_df = fetch_historical_weather(start_date, end_date)

# Load weather into MySQL
db = mysql.connector.connect(**DB_CONFIG)
cursor = db.cursor()
for _, row in weather_df.iterrows():
    cursor.execute("""
        INSERT INTO weather_data (date, avg_temp, min_temp, max_temp, humidity, rainfall, wind_speed, description)
        VALUES (%s,%s,%s,%s,%s,%s,%s,%s)
        ON DUPLICATE KEY UPDATE avg_temp=VALUES(avg_temp), humidity=VALUES(humidity)
    """, (row['date'], row['avg_temp'], row['min_temp'], row['max_temp'],
          row['humidity'], row['rainfall'], row['wind_speed'], row['description']))
db.commit()
cursor.close()
db.close()
print("Historical weather loaded from Open-Meteo.")

# ---------------------------
# Fetch forecast weather (OpenWeatherMap)
# ---------------------------
def fetch_forecast_weather(city, api_key):
    city_encoded = quote(city)
    url = f"https://api.openweathermap.org/data/2.5/forecast?q={city_encoded}&units=metric&appid={api_key}"
    resp = requests.get(url, timeout=10).json()
    
    if resp.get("cod") != "200":
        print(f"Forecast not available: {resp.get('message')}")
        return pd.DataFrame()
    
    daily_forecast = {}
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
            daily_forecast[date] = {"avg_temp":[], "min_temp":[], "max_temp":[],
                                    "humidity":[], "wind_speed":[], "rainfall":[], "description":[]}
        daily_forecast[date]["avg_temp"].append(temp)
        daily_forecast[date]["min_temp"].append(min_temp)
        daily_forecast[date]["max_temp"].append(max_temp)
        daily_forecast[date]["humidity"].append(humidity)
        daily_forecast[date]["wind_speed"].append(wind_speed)
        daily_forecast[date]["rainfall"].append(rainfall)
        daily_forecast[date]["description"].append(description)
    
    rows = []
    for date, values in daily_forecast.items():
        rows.append({
            "date": date,
            "avg_temp": round(sum(values["avg_temp"])/len(values["avg_temp"]),2),
            "min_temp": round(min(values["min_temp"]),2),
            "max_temp": round(max(values["max_temp"]),2),
            "humidity": int(sum(values["humidity"])/len(values["humidity"])),
            "wind_speed": round(sum(values["wind_speed"])/len(values["wind_speed"]),2),
            "rainfall": round(sum(values["rainfall"]),2),
            "description": max(set(values["description"]), key=values["description"].count),
            "forecast_type":"forecast"
        })
    return pd.DataFrame(rows)

# ---------------------------
# Combine historical sales + weather
# ---------------------------
db = mysql.connector.connect(**DB_CONFIG)
cursor = db.cursor(dictionary=True)
query = """
SELECT s.*, w.avg_temp, w.min_temp, w.max_temp, w.humidity, w.rainfall, w.wind_speed, w.description, w.forecast_type
FROM sales_features s
LEFT JOIN weather_data w ON s.date = w.date
ORDER BY s.date
"""
cursor.execute(query)
data = cursor.fetchall()
cursor.close()
db.close()

df = pd.DataFrame(data)

# Forward-fill missing weather
for col in ['avg_temp','humidity','wind_speed','rainfall']:
    if col in df.columns:
        df[col] = df[col].ffill()

# ---------------------------
# Append forecast
# ---------------------------
forecast_df = fetch_forecast_weather(CITY, API_KEY)
if not forecast_df.empty:
    forecast_features = pd.DataFrame({
        "date": forecast_df["date"],
        "total_quantity": None, "total_revenue": None,
        "Baby": 0, "Baking/ Spices/ Condiments": 0, "Beverages":0,
        "Cleaning":0, "Dairy":0, "Food":0, "Fruits":0, "Hygiene":0,
        "Meat":0, "Miscellaneous":0, "Pet":0, "School Supplies":0,
        "Snacks":0, "Vegetables":0,
        "day_of_week": pd.to_datetime(forecast_df["date"]).dt.dayofweek,
        "month": pd.to_datetime(forecast_df["date"]).dt.month,
        "is_weekend": pd.to_datetime(forecast_df["date"]).dt.dayofweek.isin([5,6]).astype(int),
        "lag_1": None, "lag_3": None, "lag_7": None,
        "rolling_3": None, "rolling_7": None
    })
    df_forecast_combined = pd.merge(forecast_features, forecast_df, on="date", how="left")
    df = pd.concat([df, df_forecast_combined], ignore_index=True, sort=False)

# ---------------------------
# Save final forecast-ready dataset
# ---------------------------
df.to_csv("forecast_ready_dataset.csv", index=False)
print("Forecast-ready dataset saved.")