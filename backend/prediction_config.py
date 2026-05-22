import os
import json
import joblib
import pandas as pd
import requests

BASE_DIR = os.path.dirname(os.path.abspath(__file__))

# ITEM MODEL
ITEM_MODEL_PATH = os.path.join(BASE_DIR, "model_artifacts", "xgb_sales_model.pkl")
ITEM_FEATURES_PATH = os.path.join(BASE_DIR, "model_artifacts", "feature_columns.pkl")
ITEM_ENCODERS_PATH = os.path.join(BASE_DIR, "model_artifacts", "item_model_encoders.json")

item_model = joblib.load(ITEM_MODEL_PATH)
item_feature_cols = joblib.load(ITEM_FEATURES_PATH)

with open(ITEM_ENCODERS_PATH, "r") as f:
    item_encoders = json.load(f)

# CATEGORY MODEL
CATEGORY_MODEL_PATH = os.path.join(BASE_DIR, "model_artifacts", "xgb_category_model.pkl")
CATEGORY_FEATURES_PATH = os.path.join(BASE_DIR, "model_artifacts", "category_feature_columns.pkl")
CATEGORY_ENCODERS_PATH = os.path.join(BASE_DIR, "model_artifacts", "category_model_encoders.json")

category_model = joblib.load(CATEGORY_MODEL_PATH)
category_feature_cols = joblib.load(CATEGORY_FEATURES_PATH)

with open(CATEGORY_ENCODERS_PATH, "r") as f:
    category_encoders = json.load(f)


def encode_with_default(col, value, encoder_dict):
    mapping = encoder_dict.get(col, {})
    return mapping.get(str(value), -1)


def add_calendar_features(row_date):
    dt = pd.to_datetime(row_date)
    return {
        "weekday": dt.weekday() + 1,
        "month": dt.month,
        "year": dt.year,
        "day_of_month": dt.day,
        "day_of_year": dt.dayofyear,
        "week_of_year": int(dt.isocalendar().week),
        "quarter": dt.quarter,
        "is_weekend": 1 if dt.weekday() >= 5 else 0
    }


def geocode_city(city):
    url = "https://geocoding-api.open-meteo.com/v1/search"
    params = {
        "name": city,
        "count": 1,
        "format": "json"
    }

    try:
        r = requests.get(url, params=params, timeout=15)
        r.raise_for_status()
        data = r.json()
    except requests.RequestException as e:
        raise ValueError(f"Failed to geocode city: {e}")

    if not data.get("results"):
        raise ValueError(f"City not found: {city}")

    loc = data["results"][0]
    return loc["latitude"], loc["longitude"], loc["name"]


def fetch_forecast(lat, lon):
    url = "https://api.open-meteo.com/v1/forecast"
    params = {
        "latitude": lat,
        "longitude": lon,
        "daily": (
            "weather_code,"
            "temperature_2m_max,"
            "temperature_2m_min,"
            "precipitation_sum,"
            "rain_sum,"
            "snowfall_sum,"
            "wind_speed_10m_max"
        ),
        "timezone": "auto",
        "forecast_days": 15
    }

    try:
        r = requests.get(url, params=params, timeout=20)
        r.raise_for_status()
        data = r.json()
    except requests.RequestException as e:
        raise ValueError(f"Failed to fetch forecast: {e}")

    if "daily" not in data:
        raise ValueError("Forecast response missing 'daily' data.")

    daily = data["daily"]

    required_keys = [
        "time",
        "weather_code",
        "temperature_2m_max",
        "temperature_2m_min",
        "precipitation_sum",
        "rain_sum",
        "snowfall_sum",
        "wind_speed_10m_max"
    ]

    for key in required_keys:
        if key not in daily:
            raise ValueError(f"Forecast response missing daily field: {key}")

    df = pd.DataFrame({
        "date": daily["time"],
        "weather_code": daily["weather_code"],
        "temperature_2m_max": daily["temperature_2m_max"],
        "temperature_2m_min": daily["temperature_2m_min"],
        "precipitation_sum": daily["precipitation_sum"],
        "rain_sum": daily["rain_sum"],
        "snowfall_sum": daily["snowfall_sum"],
        "wind_speed_10m_max": daily["wind_speed_10m_max"]
    })

    df["date"] = pd.to_datetime(df["date"], errors="coerce")
    df = df.dropna(subset=["date"]).copy()

    df["temperature_2m_mean"] = (
        df["temperature_2m_max"] + df["temperature_2m_min"]
    ) / 2.0

    df["apparent_temperature_max"] = df["temperature_2m_max"]
    df["apparent_temperature_min"] = df["temperature_2m_min"]

    df["is_rainy"] = (df["rain_sum"] > 0).astype(int)
    df["is_snowy"] = (df["snowfall_sum"] > 0).astype(int)
    df["is_hot"] = (df["temperature_2m_mean"] >= 27).astype(int)
    df["is_cold"] = (df["temperature_2m_mean"] <= 5).astype(int)

    return df