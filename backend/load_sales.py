import mysql.connector
import pandas as pd
import requests
import os
from urllib.parse import quote
from dotenv import load_dotenv
from datetime import date
from datetime import datetime, timedelta

load_dotenv()  # load .env

# ---------------------------
# MySQL connection
# ---------------------------
DB_CONFIG = {
    "host": os.getenv("MYSQL_HOST", "mysql-db"),
    "user": os.getenv("MYSQL_USER", "root"),
    "password": os.getenv("MYSQL_PASSWORD", "root"),
    "database": os.getenv("MYSQL_DB", "weather_sales"),
    "port": 3306
}

API_KEY = os.getenv("API_KEY")  # OpenWeatherMap
CITY = "Akron"
LATITUDE = 41.0814
LONGITUDE = -81.5190

# ---------------------------
# Load historical sales
# ---------------------------
sales_csv_path = "backend/forecast_ready_dataset.csv"
if os.path.exists(sales_csv_path):
    sales_df = pd.read_csv(sales_csv_path)
    print(f"Loaded sales CSV: {sales_csv_path}")
else:
    print(f"{sales_csv_path} not found. Generating dummy sales data.")
    sales_df = pd.DataFrame({
        "date": pd.date_range("2023-01-01", "2023-01-10"),
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
def fetch_historical_weather(start_date, end_date, lat=LATITUDE, lon=LONGITUDE):
    """
    Fetch hourly historical weather from Open-Meteo ERA5 API and aggregate to daily values.
    Returns a DataFrame with columns: date, avg_temp, rainfall, wind_speed
    """
    url = (
        f"https://archive-api.open-meteo.com/v1/era5?"
        f"latitude={lat}&longitude={lon}"
        f"&start_date={start_date}&end_date={end_date}"
        f"&hourly=temperature_2m,wind_speed_10m,rain,relativehumidity_2m"
    )
    
    resp = requests.get(url, timeout=10).json()
    
    if 'hourly' not in resp:
        print(f"No historical weather data returned. Response:\n{resp}")
        return pd.DataFrame()  # empty DF to avoid crashing
    
    hourly = resp['hourly']
    
    # Ensure rainfall exists
    rainfall = hourly.get('rain', [0]*len(hourly['time']))
    humidity = hourly.get('relativehumidity_2m', [0]*len(hourly['time']))
    
    df = pd.DataFrame({
        'datetime': pd.to_datetime(hourly['time']),
        'temperature': hourly['temperature_2m'],
        'rainfall': rainfall,
        'wind_speed': hourly['wind_speed_10m'],
        'humidity': humidity
    })
    
    df['date'] = df['datetime'].dt.date

    # Aggregate hourly → daily for ML
    daily = df.groupby('date').agg({
        'temperature': 'mean',    
        'rainfall': 'sum',        
        'wind_speed': 'mean',  
        'humidity': 'mean'        
    }).reset_index()
    
    # Rename to match your DB schema
    daily.rename(columns={'temperature': 'avg_temp'}, inplace=True)
    
    # Optional: round floats for readability
    daily[['avg_temp','rainfall','wind_speed','humidity']] = daily[['avg_temp','rainfall','wind_speed','humidity']].round(2)
    
    # Add forecast_type for clarity
    daily['forecast_type'] = 'historical'
    
    return daily

# Determine historical date range from sales
start_date = sales_df['date'].min()
end_date_raw = sales_df['date'].max()

# Clamp to Open-Meteo allowed range
min_allowed = "1940-01-01"
max_allowed = date.today().isoformat() 

# Ensure dates are within allowed range
start_date = max(str(start_date), min_allowed)  
end_date = min(str(end_date_raw), max_allowed) 
print(f"Fetching historical weather from {start_date} to {end_date}")
historical_weather = fetch_historical_weather(start_date, end_date)

if not historical_weather.empty:
    # --- FILL MISSING VALUES & ADD HUMIDITY ---
    historical_weather['avg_temp'] = historical_weather['avg_temp'].fillna(0)
    historical_weather['rainfall'] = historical_weather['rainfall'].fillna(0)
    historical_weather['wind_speed'] = historical_weather['wind_speed'].fillna(0)
    
    # fill NaNs with a default
    if 'humidity' not in historical_weather.columns:
        historical_weather['humidity'] = 50
    else:
        historical_weather['humidity'] = historical_weather['humidity'].fillna(50).astype(int)

    # --- INSERT INTO MYSQL ---
    db = mysql.connector.connect(**DB_CONFIG)
    cursor = db.cursor()
    for _, row in historical_weather.iterrows():
        cursor.execute("""
            INSERT INTO weather_data (date, avg_temp, rainfall, wind_speed, humidity)
            VALUES (%s, %s, %s, %s, %s)
            ON DUPLICATE KEY UPDATE
                avg_temp=VALUES(avg_temp),
                rainfall=VALUES(rainfall),
                wind_speed=VALUES(wind_speed),
                humidity=VALUES(humidity)
        """, (row['date'], row['avg_temp'], row['rainfall'], row['wind_speed'], row['humidity']))
    db.commit()
    cursor.close()
    db.close()
    print("Historical weather loaded from Open-Meteo.")
else:
    print("No historical weather data to load.")

# ---------------------------
# Fetch forecast weather from OpenWeatherMap
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
            daily_forecast[date] = {"temps": [], "humidity": [], "wind_speed": [], "rainfall": [], "description": []}
        daily_forecast[date]["temps"].append(temp)
        daily_forecast[date]["humidity"].append(humidity)
        daily_forecast[date]["wind_speed"].append(wind_speed)
        daily_forecast[date]["rainfall"].append(rainfall)
        daily_forecast[date]["description"].append(description)
    
    forecast_rows = []
    for date, vals in daily_forecast.items():
        forecast_rows.append({
            "date": date,
            "avg_temp": round(sum(vals["temps"])/len(vals["temps"]), 2),
            "humidity": int(sum(vals["humidity"])/len(vals["humidity"])),
            "wind_speed": round(sum(vals["wind_speed"])/len(vals["wind_speed"]), 2),
            "rainfall": round(sum(vals["rainfall"]), 2),
            "description": max(set(vals["description"]), key=vals["description"].count),
            "forecast_type": "forecast"
        })
    return pd.DataFrame(forecast_rows)

forecast_df = fetch_forecast_weather(CITY, API_KEY)
if not forecast_df.empty:
    # Add empty sales features for forecast dates
    forecast_df_features = pd.DataFrame({
        "date": forecast_df["date"],
        "total_quantity": 0,
        "total_revenue": 0,
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
        "lag_1": 0,
        "lag_3": 0,
        "lag_7": 0,
        "rolling_3": 0,
        "rolling_7": 0
    })
    
    df_forecast_combined = pd.merge(forecast_df_features, forecast_df, on="date", how="left")
else:
    df_forecast_combined = pd.DataFrame()

# ---------------------------
# Combine sales + historical + forecast into final CSV
# ---------------------------
# Start with sales_df
df = sales_df.copy()
if not historical_weather.empty:
    df = df.merge(historical_weather, left_on='date', right_on='date', how='left')

# Append forecast
if not df_forecast_combined.empty:
    df = pd.concat([df, df_forecast_combined], ignore_index=True, sort=False)

df.to_csv("backend/forecast_ready_dataset.csv", index=False)
print("Forecast-ready dataset saved at 'backend/forecast_ready_dataset.csv'.")