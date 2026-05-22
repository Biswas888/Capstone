import pandas as pd
import numpy as np

from prediction_config import (
    geocode_city,
    fetch_forecast,
    encode_with_default,
    add_calendar_features,
    item_model,
    item_feature_cols,
    item_encoders,
    category_model,
    category_feature_cols,
    category_encoders,
)

# validate request and load CSV
def validate_prediction_request(req):
    if "file" not in req.files:
        raise ValueError("No file uploaded. Use form-data key 'file'.")

    file = req.files["file"]
    city = req.form.get("city", "").strip()

    if not city:
        raise ValueError("City is required.")

    if not file or file.filename == "":
        raise ValueError("No selected file.")

    if not file.filename.lower().endswith(".csv"):
        raise ValueError("Please upload a CSV file.")

    input_df = pd.read_csv(file)

    if input_df.empty:
        raise ValueError("Uploaded CSV is empty.")

    required_cols = ["date", "sales"]
    missing_cols = [col for col in required_cols if col not in input_df.columns]
    if missing_cols:
        raise ValueError(f"Uploaded CSV must contain columns: {missing_cols}")

    input_df["date"] = pd.to_datetime(input_df["date"], errors="coerce")
    input_df["sales"] = pd.to_numeric(input_df["sales"], errors="coerce")

    input_df = input_df.dropna(subset=["date", "sales"]).copy()

    if input_df.empty:
        raise ValueError("No valid date/sales rows found in uploaded CSV.")

    input_df["date"] = input_df["date"].dt.normalize()
    input_df["city"] = city

    optional_defaults = {
        "category": None,
        "item_id": None,
        "store_id": None,
        "sell_price": np.nan,
        "snap": 0,
        "has_event_1": 0,
        "has_event_2": 0,
        "has_any_event": 0,
        "dept_id": None,
        "cat_id": None
    }

    for col, default_val in optional_defaults.items():
        if col not in input_df.columns:
            input_df[col] = default_val

    # normalize optional numeric columns
    numeric_optional_cols = ["sell_price", "snap", "has_event_1", "has_event_2", "has_any_event"]
    for col in numeric_optional_cols:
        input_df[col] = pd.to_numeric(input_df[col], errors="coerce")

    # require at least one grouping field
    grouping_fields = ["item_id", "store_id", "category"]
    has_grouping_data = any(
        col in input_df.columns and input_df[col].notna().any() and (input_df[col].astype(str).str.strip() != "").any()
        for col in grouping_fields
    )
    if not has_grouping_data:
        raise ValueError("Uploaded CSV must contain at least one grouping field with values: item_id, store_id, or category.")

    input_df = input_df.sort_values("date").reset_index(drop=True)

    return input_df, city

# build history map from uploaded CSV
def build_uploaded_history_map(input_df):
    history_map = {}

    input_df = input_df.copy()
    input_df["date"] = pd.to_datetime(input_df["date"], errors="coerce")
    input_df["sales"] = pd.to_numeric(input_df["sales"], errors="coerce")

    input_df = input_df.dropna(subset=["date", "sales"]).copy()
    input_df["date"] = input_df["date"].dt.normalize()
    input_df = input_df.sort_values("date").reset_index(drop=True)

    for _, row in input_df.iterrows():
        category = row.get("category")

        if pd.isna(category) or str(category).strip() in ["", "nan", "None"]:
            category = row.get("cat_id")

        item_id = row.get("item_id")

        store_id = row.get("store_id") if "store_id" in input_df.columns else None

        history_key = get_history_key(category, item_id, store_id)

        if history_key not in history_map:
            history_map[history_key] = {
                "category": category if pd.notna(category) else None,
                "item_id": item_id if pd.notna(item_id) else None,
                "store_id": store_id if pd.notna(store_id) else None,
                "dept_id": row.get("dept_id") if pd.notna(row.get("dept_id")) else None,
                "cat_id": row.get("cat_id") if pd.notna(row.get("cat_id")) else (
                    category if pd.notna(category) else None
                ),
                "sell_price": float(pd.to_numeric(row.get("sell_price"), errors="coerce"))
                if pd.notna(row.get("sell_price")) else 0.0,
                "snap": int(pd.to_numeric(row.get("snap"), errors="coerce"))
                if pd.notna(row.get("snap")) else 0,
                "has_event_1": int(pd.to_numeric(row.get("has_event_1"), errors="coerce"))
                if pd.notna(row.get("has_event_1")) else 0,
                "has_event_2": int(pd.to_numeric(row.get("has_event_2"), errors="coerce"))
                if pd.notna(row.get("has_event_2")) else 0,
                "has_any_event": int(pd.to_numeric(row.get("has_any_event"), errors="coerce"))
                if pd.notna(row.get("has_any_event")) else 0,
                "history_dates": [],
                "sales_history": []
            }

        history_map[history_key]["history_dates"].append(row["date"])
        history_map[history_key]["sales_history"].append(float(row["sales"]))

        # keep latest non-null metadata values
        if pd.notna(row.get("sell_price")):
            history_map[history_key]["sell_price"] = float(
                pd.to_numeric(row.get("sell_price"), errors="coerce") or 0
            )

        if pd.notna(row.get("snap")):
            history_map[history_key]["snap"] = int(
                pd.to_numeric(row.get("snap"), errors="coerce") or 0
            )

        if pd.notna(row.get("dept_id")):
            history_map[history_key]["dept_id"] = row.get("dept_id")

        if pd.notna(row.get("cat_id")):
            history_map[history_key]["cat_id"] = row.get("cat_id")

        if pd.notna(row.get("has_event_1")):
            history_map[history_key]["has_event_1"] = int(
                pd.to_numeric(row.get("has_event_1"), errors="coerce") or 0
            )

        if pd.notna(row.get("has_event_2")):
            history_map[history_key]["has_event_2"] = int(
                pd.to_numeric(row.get("has_event_2"), errors="coerce") or 0
            )

        if pd.notna(row.get("has_any_event")):
            history_map[history_key]["has_any_event"] = int(
                pd.to_numeric(row.get("has_any_event"), errors="coerce") or 0
            )

    return history_map

# load forecast for city
def load_forecast_for_city(city):
    geo = geocode_city(city)
    if not geo or len(geo) < 2:
        raise ValueError(f"Could not geocode city '{city}'.")

    lat, lon = geo[:2]
    forecast_df = fetch_forecast(lat, lon)

    if forecast_df is None or forecast_df.empty:
        raise ValueError(f"No forecast data found for city '{city}'.")

    if "date" not in forecast_df.columns:
        raise ValueError("Forecast data is missing the 'date' column.")

    forecast_df = forecast_df.copy()
    forecast_df["date"] = pd.to_datetime(forecast_df["date"], errors="coerce").dt.date
    forecast_df = forecast_df.dropna(subset=["date"])

    if forecast_df.empty:
        raise ValueError(f"Forecast data for city '{city}' contains no valid dates.")

    return forecast_df

# history key
def get_history_key(category, item_id, store_id):
    if pd.notna(item_id) and str(item_id).strip() not in ["", "nan", "None"]:
        return f"item::{item_id}"

    if pd.notna(category) and str(category).strip() not in ["", "nan", "None"]:
        return f"category::{category}"

    return "default"

# Demand suggestion
def get_demand_suggestion(pred):
    if pred >= 20:
        return "High demand expected. Restock heavily."
    if pred >= 10:
        return "Moderate to high demand. Restock soon."
    if pred >= 5:
        return "Moderate demand expected. Monitor inventory."
    return "Low demand expected. Current stock may be sufficient."

#Build feature row for item-level prediction
def build_item_feature_row(forecast_date, weather_row, sales_history, meta):
    recent = sales_history[-7:] if len(sales_history) > 0 else [0.0]
    lag_1 = float(sales_history[-1]) if len(sales_history) >= 1 else 0.0

    store_id = meta.get("store_id")
    dept_id = meta.get("dept_id")
    cat_id = meta.get("cat_id")
    sell_price = float(meta.get("sell_price", 0.0) or 0.0)
    snap = int(meta.get("snap", 0) or 0)

    calendar_feats = add_calendar_features(pd.to_datetime(forecast_date))
    is_weekend = int(calendar_feats.get("is_weekend", 0))
    month = int(calendar_feats.get("month", 0))

    encoded_store = (
        encode_with_default("store_id", store_id, item_encoders)
        if store_id is not None and str(store_id).strip() != ""
        else -1
    )
    encoded_dept = (
        encode_with_default("dept_id", dept_id, item_encoders)
        if dept_id is not None and str(dept_id).strip() != ""
        else -1
    )
    encoded_cat = (
        encode_with_default("cat_id", cat_id, item_encoders)
        if cat_id is not None and str(cat_id).strip() != ""
        else -1
    )

    # Use raw weather values to match training-time scale
    temp_max = float(weather_row.get("temperature_2m_max", 0) or 0)
    temp_min = float(weather_row.get("temperature_2m_min", 0) or 0)
    temp_mean = float(weather_row.get("temperature_2m_mean", 0) or 0)
    app_temp_max = float(
        weather_row.get("apparent_temperature_max", weather_row.get("temperature_2m_max", 0)) or 0
    )
    app_temp_min = float(
        weather_row.get("apparent_temperature_min", weather_row.get("temperature_2m_min", 0)) or 0
    )
    precipitation_sum = float(weather_row.get("precipitation_sum", 0) or 0)
    rain_sum = float(weather_row.get("rain_sum", 0) or 0)
    snowfall_sum = float(weather_row.get("snowfall_sum", 0) or 0)
    wind_speed_10m_max = float(weather_row.get("wind_speed_10m_max", 0) or 0)

    feature = {
        # item_id was dropped during training
        "store_id": encoded_store,
        "dept_id": encoded_dept,
        "cat_id": encoded_cat,

        "sell_price": sell_price,
        "snap": snap,

        "weekday": int(calendar_feats.get("weekday", 0)),
        "month": month,
        "year": int(calendar_feats.get("year", 0)),
        "quarter": int(calendar_feats.get("quarter", 0)),
        "day_of_month": int(calendar_feats.get("day_of_month", 0)),
        "day_of_year": int(calendar_feats.get("day_of_year", 0)),
        "week_of_year": int(calendar_feats.get("week_of_year", 0)),
        "is_weekend": is_weekend,

        "temperature_2m_max": temp_max,
        "temperature_2m_min": temp_min,
        "temperature_2m_mean": temp_mean,
        "apparent_temperature_max": app_temp_max,
        "apparent_temperature_min": app_temp_min,
        "precipitation_sum": precipitation_sum,
        "rain_sum": rain_sum,
        "snowfall_sum": snowfall_sum,
        "wind_speed_10m_max": wind_speed_10m_max,
        "weather_code": int(weather_row.get("weather_code", 0) or 0),

        "is_rainy": int(weather_row.get("is_rainy", 0) or 0),
        "is_snowy": int(weather_row.get("is_snowy", 0) or 0),
        "is_hot": int(weather_row.get("is_hot", 0) or 0),
        "is_cold": int(weather_row.get("is_cold", 0) or 0),

        "lag_1": lag_1,
        "rolling_mean_7": float(np.mean(recent)) if recent else 0.0,

        # kept only in case item_feature_cols expects it
        "cat_avg_sales": float(np.mean(recent)) if recent else 0.0,

        "price_change_1": 0.0,
        "price_change_7": 0.0,
        "price_pct_change_1": 0.0,
        "lag_zero_1": 1 if lag_1 == 0 else 0,

        "has_event_1": int(meta.get("has_event_1", 0) or 0),
        "has_event_2": int(meta.get("has_event_2", 0) or 0),
        "has_any_event": int(meta.get("has_any_event", 0) or 0),

        # interaction features used in training
        "temp_x_cat": temp_mean * encoded_cat if encoded_cat != -1 else 0.0,
        "rain_x_cat": rain_sum * encoded_cat if encoded_cat != -1 else 0.0,
        "temp_x_weekend": temp_mean * is_weekend,
        "rain_x_weekend": rain_sum * is_weekend,
        "temp_x_month": temp_mean * month,
        "temp_x_rain": temp_mean * rain_sum,
    }

    feature_df = pd.DataFrame([feature])

    for col in item_feature_cols:
        if col not in feature_df.columns:
            feature_df[col] = 0

    return feature_df[item_feature_cols], sell_price

#Build feature row for category-level prediction
def build_category_feature_row(forecast_date, weather_row, sales_history, meta):
    lag_1 = float(sales_history[-1]) if len(sales_history) >= 1 else 0.0
    recent = sales_history[-7:] if len(sales_history) > 0 else [0.0]

    store_id = meta.get("store_id")
    category = meta.get("category")
    cat_value = meta.get("cat_id") if meta.get("cat_id") is not None else category
    sell_price = float(meta.get("sell_price", 0.0) or 0.0)
    snap = int(meta.get("snap", 0) or 0)

    calendar_feats = add_calendar_features(pd.to_datetime(forecast_date))
    is_weekend = int(calendar_feats.get("is_weekend", 0))
    month = int(calendar_feats.get("month", 0))

    encoded_store = (
        encode_with_default("store_id", store_id, category_encoders)
        if store_id is not None and str(store_id).strip() != ""
        else -1
    )

    encoded_cat = (
        encode_with_default("cat_id", cat_value, category_encoders)
        if cat_value is not None and str(cat_value).strip() != ""
        else -1
    )

    # Use raw weather values to match training-time scale
    temp_max = float(weather_row.get("temperature_2m_max", 0) or 0)
    temp_min = float(weather_row.get("temperature_2m_min", 0) or 0)
    temp_mean = float(weather_row.get("temperature_2m_mean", 0) or 0)
    app_temp_max = float(
        weather_row.get("apparent_temperature_max", weather_row.get("temperature_2m_max", 0)) or 0
    )
    app_temp_min = float(
        weather_row.get("apparent_temperature_min", weather_row.get("temperature_2m_min", 0)) or 0
    )
    precipitation_sum = float(weather_row.get("precipitation_sum", 0) or 0)
    rain_sum = float(weather_row.get("rain_sum", 0) or 0)
    snowfall_sum = float(weather_row.get("snowfall_sum", 0) or 0)
    wind_speed_10m_max = float(weather_row.get("wind_speed_10m_max", 0) or 0)

    feature = {
        "cat_id": encoded_cat,
        "store_id": encoded_store,

        "sell_price": sell_price,
        "snap": snap,

        "is_weekend": is_weekend,
        "month": month,
        "year": int(calendar_feats.get("year", 0)),
        "quarter": int(calendar_feats.get("quarter", 0)),
        "day_of_month": int(calendar_feats.get("day_of_month", 0)),
        "day_of_year": int(calendar_feats.get("day_of_year", 0)),
        "week_of_year": int(calendar_feats.get("week_of_year", 0)),
        "weekday": int(calendar_feats.get("weekday", 0)),

        "temperature_2m_max": temp_max,
        "temperature_2m_min": temp_min,
        "temperature_2m_mean": temp_mean,
        "apparent_temperature_max": app_temp_max,
        "apparent_temperature_min": app_temp_min,
        "precipitation_sum": precipitation_sum,
        "rain_sum": rain_sum,
        "snowfall_sum": snowfall_sum,
        "wind_speed_10m_max": wind_speed_10m_max,
        "weather_code": int(weather_row.get("weather_code", 0) or 0),

        "is_rainy": int(weather_row.get("is_rainy", 0) or 0),
        "is_snowy": int(weather_row.get("is_snowy", 0) or 0),
        "is_hot": int(weather_row.get("is_hot", 0) or 0),
        "is_cold": int(weather_row.get("is_cold", 0) or 0),

        "has_event_1": int(meta.get("has_event_1", 0) or 0),
        "has_event_2": int(meta.get("has_event_2", 0) or 0),
        "has_any_event": int(meta.get("has_any_event", 0) or 0),

        "lag_1": lag_1,
        "rolling_mean_7": float(np.mean(recent)) if recent else 0.0,

        # interaction features required by category model
        "temp_x_weekend": temp_mean * is_weekend,
        "rain_x_weekend": rain_sum * is_weekend,
        "temp_x_cat": temp_mean * encoded_cat if encoded_cat != -1 else 0.0,
        "rain_x_cat": rain_sum * encoded_cat if encoded_cat != -1 else 0.0,
        "temp_x_month": temp_mean * month,
        "temp_x_rain": temp_mean * rain_sum,
    }

    feature_df = pd.DataFrame([feature])

    for col in category_feature_cols:
        if col not in feature_df.columns:
            feature_df[col] = 0

    return feature_df[category_feature_cols], sell_price

def determine_prediction_level(item_id, category, sales_history, item_encoders, category_encoders):
    history_len = len(sales_history) if sales_history is not None else 0

    known_item = (
        item_id is not None
        and str(item_id).strip() != ""
        and "item_id" in item_encoders
        and str(item_id) in item_encoders["item_id"]
    )

    known_category = (
        category is not None
        and str(category).strip() != ""
        and (
            ("cat_id" in category_encoders and str(category) in category_encoders["cat_id"])
            or ("category" in category_encoders and str(category) in category_encoders["category"])
        )
    )

    if known_item and history_len >= 10:
        return "item"

    if known_category and history_len >= 7:
        return "category"

    return "category"

def build_prediction_results(input_df, city, forecast_df):
    results = []

    history_map = build_uploaded_history_map(input_df)

    forecast_df = forecast_df.copy()
    forecast_df["date"] = pd.to_datetime(forecast_df["date"], errors="coerce")
    forecast_df = forecast_df.dropna(subset=["date"]).copy()

    forecast_df["date"] = forecast_df["date"].dt.normalize()

    today = pd.Timestamp.today().normalize()

    forecast_df = (
        forecast_df[forecast_df["date"] >= today]
        .drop_duplicates(subset=["date"], keep="first")
        .sort_values("date")
        .head(15)
        .reset_index(drop=True)
    )

    print("Forecast dates used:", forecast_df["date"].dt.date.tolist())
    print("Forecast day count:", len(forecast_df))


    for history_key, meta in history_map.items():
        sales_history = [float(x) for x in meta.get("sales_history", []) if pd.notna(x)]
        history_dates = [pd.to_datetime(d) for d in meta.get("history_dates", []) if pd.notna(d)]

        if not history_dates or not sales_history:
            continue

        if len(sales_history) < 7:
            continue

        # Use only the cleaned daily forecast dates
        future_forecast_df = forecast_df.copy()

        if future_forecast_df.empty:
            continue

        for _, weather_row in future_forecast_df.iterrows():
            forecast_date = pd.to_datetime(weather_row["date"]).normalize()

            level = determine_prediction_level(
                item_id=meta.get("item_id"),
                category=meta.get("category"),
                sales_history=sales_history,
                item_encoders=item_encoders,
                category_encoders=category_encoders
            )

            # Base prediction
            if level == "item":
                feature_df, sell_price = build_item_feature_row(
                    forecast_date=forecast_date,
                    weather_row=weather_row,
                    sales_history=sales_history,
                    meta=meta
                )
                pred = float(item_model.predict(feature_df)[0])

            else:
                feature_df, sell_price = build_category_feature_row(
                    forecast_date=forecast_date,
                    weather_row=weather_row,
                    sales_history=sales_history,
                    meta=meta
                )
                pred = float(category_model.predict(feature_df)[0])

            pred = max(pred, 0.0)

            # Controlled weather adjustment
            temp = float(weather_row.get("temperature_2m_mean", 0) or 0)
            rain = float(weather_row.get("rain_sum", 0) or 0)
            snowfall = float(weather_row.get("snowfall_sum", 0) or 0)

            item_id_str = str(meta.get("item_id", "")).upper()
            category_str = str(meta.get("category", "")).upper()

            weather_factor = 1.0

            # Frozen / hot-weather foods
            if "ICECREAM" in item_id_str:
                if temp >= 28:
                    weather_factor += 0.15
                elif temp >= 20:
                    weather_factor += 0.10
                elif temp >= 15:
                    weather_factor += 0.05
                elif temp <= 8:
                    weather_factor -= 0.10

            # Beverages
            if category_str == "BEVERAGES":
                if "COFFEE" in item_id_str:
                    if temp >= 30:
                        weather_factor -= 0.28
                    elif temp >= 25:
                        weather_factor -= 0.20
                    elif temp >= 20:
                        weather_factor -= 0.12
                    elif temp <= 3:
                        weather_factor += 0.18
                    elif temp <= 8:
                        weather_factor += 0.10

                    if snowfall >= 2:
                        weather_factor += 0.05
                    if rain >= 5:
                        weather_factor += 0.03

                # Cold drinks = more demand in heat, less in cold/snow
                elif "WATER" in item_id_str or "SODA" in item_id_str:
                    if temp >= 28:
                        weather_factor += 0.12
                    elif temp >= 20:
                        weather_factor += 0.07
                    elif temp <= 5:
                        weather_factor -= 0.05

                    if snowfall >= 2:
                        weather_factor -= 0.04

                # Fallback for other beverages
                else:
                    if temp >= 28:
                        weather_factor += 0.08
                    elif temp >= 20:
                        weather_factor += 0.04
                    elif temp <= 5:
                        weather_factor -= 0.03

            # Warm foods / comfort foods
            if "SOUP" in item_id_str:
                if temp <= 5:
                    weather_factor += 0.12
                elif temp <= 10:
                    weather_factor += 0.06
                elif temp >= 25:
                    weather_factor -= 0.05

            if "SALAD" in item_id_str:
                if temp >= 22:
                    weather_factor += 0.06
                elif temp <= 8:
                    weather_factor -= 0.05

            # -----------------------------
            # Household / cleaning
            # -----------------------------
            if "CLEANER" in item_id_str:
                if rain >= 5:
                    weather_factor += 0.08
                elif rain >= 2:
                    weather_factor += 0.04

                if snowfall >= 2:
                    weather_factor += 0.03

            elif category_str == "HOUSEHOLD":
                if rain >= 5:
                    weather_factor += 0.04
                elif rain >= 2:
                    weather_factor += 0.02

                if snowfall >= 2:
                    weather_factor += 0.03

            # Indoor / outdoor hobbies
            if "BOARDGAME" in item_id_str or "PUZZLE" in item_id_str:
                if rain >= 5:
                    weather_factor += 0.06
                elif rain >= 2:
                    weather_factor += 0.03

                if snowfall >= 2:
                    weather_factor += 0.05

            if "OUTDOORBALL" in item_id_str:
                if rain >= 5:
                    weather_factor -= 0.12
                elif rain >= 2:
                    weather_factor -= 0.06

                if snowfall >= 2:
                    weather_factor -= 0.10
                elif temp <= 5:
                    weather_factor -= 0.06

            # General snow drag for most items
            if snowfall >= 3 and "ICECREAM" not in item_id_str and "BOARDGAME" not in item_id_str and "PUZZLE" not in item_id_str:
                weather_factor -= 0.03

            pred = max(pred * weather_factor, 0.0)

            # Stabilization
            recent_window = sales_history[-7:] if len(sales_history) >= 7 else sales_history
            recent_mean = float(np.mean(recent_window)) if recent_window else 0.0
            recent_std = float(np.std(recent_window)) if recent_window else 0.0

            lower_bound = max(0.0, recent_mean - 2.5 * max(recent_std, 1.25))
            upper_bound = recent_mean + 2.5 * max(recent_std, 1.25)

            pred = min(pred, upper_bound)
            pred = max(pred, lower_bound)

            # Recursive update
            if sales_history:
                blended = 0.7 * pred + 0.3 * sales_history[-1]
            else:
                blended = pred

            sales_history.append(blended)

            if len(sales_history) > 28:
                sales_history = sales_history[-28:]

            raw_category = meta.get("category")
            if raw_category is None or str(raw_category).strip() in ["", "-", "nan", "None"]:
                raw_category = meta.get("cat_id")

            category_value = str(raw_category).strip() if raw_category is not None else "-"
            if category_value in ["", "nan", "None"]:
                category_value = "-"

            results.append({
                "date": str(forecast_date.date()),
                "city": city,
                "category": category_value,
                "item_id": meta.get("item_id"),
                "store_id": meta.get("store_id"),
                "sell_price": round(float(sell_price), 2),
                "temperature_2m_mean": round(temp, 2),
                "rain_sum": round(rain, 2),
                "snowfall_sum": round(snowfall, 2),
                "predicted_sales": round(pred, 2),
                "suggestion": get_demand_suggestion(pred),
                "prediction_level": level,
                "confidence": "high" if level == "item" else "medium",
            })

    return pd.DataFrame(results)


def compute_inventory_metrics(group_df):
    predicted = pd.to_numeric(group_df["predicted_sales"], errors="coerce").fillna(0)
    avg_daily = predicted.mean()
    total_predicted = predicted.sum()
    forecast_days = group_df["date"].nunique() if "date" in group_df.columns else len(group_df)

    historical_avg_daily = None

    # 1) Use actual current stock if provided
    if "current_stock" in group_df.columns and group_df["current_stock"].notna().any():
        estimated_stock = float(
            pd.to_numeric(group_df["current_stock"], errors="coerce").fillna(0).iloc[0]
        )

    # 2) Otherwise estimate from historical average
    elif "historical_avg_daily_sales" in group_df.columns and group_df["historical_avg_daily_sales"].notna().any():
        historical_avg_daily = float(
            pd.to_numeric(group_df["historical_avg_daily_sales"], errors="coerce").fillna(0).iloc[0]
        )
        stock_cover_days = 7
        estimated_stock = historical_avg_daily * stock_cover_days

    # 3) Final fallback
    else:
        stock_cover_days = 7
        estimated_stock = float(avg_daily * stock_cover_days)

    # stockout days based on predicted demand
    if avg_daily > 0:
        days_until_stockout = estimated_stock / avg_daily
    else:
        days_until_stockout = float("inf")

    # reorder matched to forecast horizon
    recommended_reorder = max(0, total_predicted - estimated_stock)
    print("Forecast days:", forecast_days)
    # urgency = stockout risk only
    if days_until_stockout < 2:
        urgency = "Very High"
    elif days_until_stockout < 4:
        urgency = "High"
    elif days_until_stockout < 7:
        urgency = "Medium"
    elif days_until_stockout < 10:
        urgency = "Low"
    else:
        urgency = "Very Low"

    return {
        "estimated_stock": round(float(estimated_stock), 2),
        "historical_avg_daily_sales": round(historical_avg_daily, 2) if historical_avg_daily is not None else "-",
        "predicted_avg_daily_demand": round(float(avg_daily), 2),
        "total_predicted_demand": round(float(total_predicted), 2),
        "forecast_days": int(forecast_days),
        "days_until_stockout": round(float(days_until_stockout), 2) if np.isfinite(days_until_stockout) else "∞",
        "recommended_reorder": round(float(recommended_reorder), 2),
        "urgency": urgency
    }

def build_prediction_response_payload(result_df, input_df=None):
    result_df = result_df.copy()

    result_df["predicted_sales"] = pd.to_numeric(result_df["predicted_sales"], errors="coerce")
    result_df["sell_price"] = pd.to_numeric(result_df["sell_price"], errors="coerce")
    result_df["predicted_revenue"] = result_df["predicted_sales"] * result_df["sell_price"]

    if input_df is not None and "item_id" in input_df.columns and "sales" in input_df.columns:
        hist_df = input_df.copy()
        hist_df["sales"] = pd.to_numeric(hist_df["sales"], errors="coerce")

        historical_item_avg = (
            hist_df.groupby("item_id", as_index=False)["sales"]
            .mean()
            .rename(columns={"sales": "historical_avg_daily_sales"})
        )

        result_df = result_df.merge(historical_item_avg, on="item_id", how="left")

    valid_df = result_df[result_df["predicted_sales"].notna()].copy()

    summary = {
        "total_rows": int(len(result_df)),
        "rows_with_predictions": int(len(valid_df)),
        "total_predicted_sales": 0.0,
        "average_predicted_sales": 0.0,
        "total_predicted_revenue": 0.0,
        "average_predicted_revenue": 0.0,
        "peak_date": None,
        "peak_sales": 0.0,
        "peak_revenue_date": None,
        "peak_revenue": 0.0,
        "lowest_date": None,
        "lowest_sales": 0.0,
        "high_demand_days": 0,
        "moderate_demand_days": 0,
        "low_demand_days": 0
    }

    insights = []

    if not valid_df.empty:
        total_predicted_sales = float(valid_df["predicted_sales"].sum())
        avg_predicted_sales = float(valid_df["predicted_sales"].mean())
        total_predicted_revenue = float(valid_df["predicted_revenue"].fillna(0).sum())
        avg_predicted_revenue = float(valid_df["predicted_revenue"].fillna(0).mean())

        daily_sales_df = (
            valid_df.groupby("date", as_index=False)
            .agg(
                total_predicted_sales=("predicted_sales", "sum"),
                total_predicted_revenue=("predicted_revenue", "sum")
            )
            .sort_values("date")
        )

        peak_date = None
        peak_sales = 0.0
        lowest_date = None
        lowest_sales = 0.0
        peak_revenue_date = None
        peak_revenue_value = 0.0

        if not daily_sales_df.empty:
            peak_sales_row = daily_sales_df.loc[daily_sales_df["total_predicted_sales"].idxmax()]
            low_sales_row = daily_sales_df.loc[daily_sales_df["total_predicted_sales"].idxmin()]
            peak_revenue_row = daily_sales_df.loc[daily_sales_df["total_predicted_revenue"].idxmax()]

            peak_date = str(peak_sales_row["date"])
            peak_sales = round(float(peak_sales_row["total_predicted_sales"]), 2)

            lowest_date = str(low_sales_row["date"])
            lowest_sales = round(float(low_sales_row["total_predicted_sales"]), 2)

            peak_revenue_date = str(peak_revenue_row["date"])
            peak_revenue_value = round(float(peak_revenue_row["total_predicted_revenue"]), 2)

        high_demand_days = int((daily_sales_df["total_predicted_sales"] >= 20).sum())
        moderate_demand_days = int(
            ((daily_sales_df["total_predicted_sales"] >= 5) &
             (daily_sales_df["total_predicted_sales"] < 20)).sum()
        )
        low_demand_days = int((daily_sales_df["total_predicted_sales"] < 5).sum())

        summary.update({
            "total_predicted_sales": round(total_predicted_sales, 2),
            "average_predicted_sales": round(avg_predicted_sales, 2),
            "total_predicted_revenue": round(total_predicted_revenue, 2),
            "average_predicted_revenue": round(avg_predicted_revenue, 2),
            "peak_date": peak_date,
            "peak_sales": peak_sales,
            "peak_revenue_date": peak_revenue_date,
            "peak_revenue": peak_revenue_value,
            "lowest_date": lowest_date,
            "lowest_sales": lowest_sales,
            "high_demand_days": high_demand_days,
            "moderate_demand_days": moderate_demand_days,
            "low_demand_days": low_demand_days
        })

        if peak_date is not None:
            insights.append(
                f"Highest total forecast demand is expected on {peak_date} with predicted sales of {peak_sales}."
            )

        if peak_revenue_date is not None:
            insights.append(
                f"Highest predicted revenue is expected on {peak_revenue_date} with estimated revenue of {peak_revenue_value}."
            )

        if high_demand_days > 0:
            insights.append(
                f"{high_demand_days} forecast day(s) show high total demand and may require early restocking."
            )

        if summary["average_predicted_sales"] >= 20:
            insights.append("Overall forecast suggests strong demand across the forecast horizon.")
        elif summary["average_predicted_sales"] >= 10:
            insights.append("Overall forecast suggests moderate demand with some stronger-demand periods.")
        else:
            insights.append("Overall forecast suggests relatively low demand across the forecast horizon.")

        if "rain_sum" in valid_df.columns:
            rainy_days = int(
                (pd.to_numeric(valid_df["rain_sum"], errors="coerce").fillna(0) > 0)
                .groupby(valid_df["date"]).any().sum()
            )
            if rainy_days > 0:
                insights.append(f"Rain is expected on {rainy_days} forecast day(s).")

        if "snowfall_sum" in valid_df.columns:
            snowy_days = int(
                (pd.to_numeric(valid_df["snowfall_sum"], errors="coerce").fillna(0) > 0)
                .groupby(valid_df["date"]).any().sum()
            )
            if snowy_days > 0:
                insights.append(f"Snowfall is expected on {snowy_days} forecast day(s).")

        insights.append(
            f"Predictions were generated for {summary['rows_with_predictions']} future forecast row(s)."
        )

    else:
        insights.append(
            "No predictions could be generated. This may happen if no future forecast data was available or if uploaded groups did not have enough valid history."
        )

    category_summary = []
    if not valid_df.empty and "category" in valid_df.columns:
        category_group = (
            valid_df[valid_df["category"].notna()]
            .groupby("category", as_index=False)
            .agg(
                total_predicted_sales=("predicted_sales", "sum"),
                average_predicted_sales=("predicted_sales", "mean"),
                total_predicted_revenue=("predicted_revenue", "sum"),
                item_count=("item_id", "nunique")
            )
            .sort_values("total_predicted_sales", ascending=False)
        )

        category_summary = [
            {
                "category": str(r["category"]),
                "total_predicted_sales": round(float(r["total_predicted_sales"]), 2),
                "average_predicted_sales": round(float(r["average_predicted_sales"]), 2),
                "total_predicted_revenue": round(float(r["total_predicted_revenue"]), 2),
                "item_count": int(r["item_count"]) if pd.notna(r["item_count"]) else 0
            }
            for _, r in category_group.iterrows()
        ]

    item_summary = []
    if not valid_df.empty and "item_id" in valid_df.columns:
        group_cols = ["item_id"]
        if "category" in valid_df.columns:
            group_cols.append("category")

        item_group = (
            valid_df[valid_df["item_id"].notna()]
            .groupby(group_cols, as_index=False)
            .agg(
                total_predicted_sales=("predicted_sales", "sum"),
                average_predicted_sales=("predicted_sales", "mean"),
                total_predicted_revenue=("predicted_revenue", "sum"),
                avg_sell_price=("sell_price", "mean")
            )
            .sort_values("total_predicted_sales", ascending=False)
            .head(20)
        )

        item_summary = [
            {
                "item_id": str(r["item_id"]),
                "category": str(r["category"]) if "category" in item_group.columns and pd.notna(r["category"]) else None,
                "total_predicted_sales": round(float(r["total_predicted_sales"]), 2),
                "average_predicted_sales": round(float(r["average_predicted_sales"]), 2),
                "total_predicted_revenue": round(float(r["total_predicted_revenue"]), 2),
                "avg_sell_price": round(float(r["avg_sell_price"]), 2) if pd.notna(r["avg_sell_price"]) else None
            }
            for _, r in item_group.iterrows()
        ]

    top_items = item_summary[:5] if item_summary else []

    inventory_summary = []

    for item_id, group in result_df.groupby("item_id"):
        metrics = compute_inventory_metrics(group)

        inventory_summary.append({
            "item_id": item_id,
            "category": (
                group["category"].iloc[0] if "category" in group.columns
                else group["cat_id"].iloc[0] if "cat_id" in group.columns
                else "-"
            ),
            **metrics
        })

    sales_over_time = []
    revenue_over_time = []

    if not valid_df.empty:
        time_group = (
            valid_df.groupby("date", as_index=False)
            .agg(
                total_predicted_sales=("predicted_sales", "sum"),
                total_predicted_revenue=("predicted_revenue", "sum")
            )
            .sort_values("date")
        )

        sales_over_time = [
            {
                "date": str(r["date"]),
                "total_predicted_sales": round(float(r["total_predicted_sales"]), 2)
            }
            for _, r in time_group.iterrows()
        ]

        revenue_over_time = [
            {
                "date": str(r["date"]),
                "total_predicted_revenue": round(float(r["total_predicted_revenue"]), 2)
            }
            for _, r in time_group.iterrows()
        ]

    preview = valid_df.replace({np.nan: None}).head(500).to_dict(orient="records")

    chart_data = []
    if not valid_df.empty:
        chart_data = [
            {
                "date": str(r["date"]),
                "predicted_sales": round(float(r["predicted_sales"]), 2),
                "predicted_revenue": round(float(r["predicted_revenue"]), 2) if pd.notna(r["predicted_revenue"]) else 0.0,
                "temperature_2m_mean": round(float(r["temperature_2m_mean"]), 2) if "temperature_2m_mean" in valid_df.columns and pd.notna(r["temperature_2m_mean"]) else None,
                "rain_sum": float(r["rain_sum"]) if "rain_sum" in valid_df.columns and pd.notna(r["rain_sum"]) else 0
            }
            for _, r in valid_df.iterrows()
        ]

    print("result_df shape:", result_df.shape)
    print("result_df columns:", result_df.columns.tolist())
    print("valid_df shape:", valid_df.shape)
    print("inventory_summary count:", len(inventory_summary))
    print("inventory_summary sample:", inventory_summary[:3])

    return {
        "message": "Prediction completed successfully using uploaded sales history and future weather forecast.",
        "rows_processed": int(len(result_df)),
        "summary": summary,
        "insights": insights,
        "top_items": top_items,
        "category_summary": category_summary,
        "item_summary": item_summary,
        "sales_over_time": sales_over_time,
        "revenue_over_time": revenue_over_time,
        "preview": preview,
        "chart_data": chart_data,
        "inventory_summary": inventory_summary
    }