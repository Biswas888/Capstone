import os
import time
import json
import joblib
import jwt
import numpy as np
import pandas as pd
import mysql.connector
import requests

from datetime import datetime
from flask import Flask, jsonify, send_from_directory, request
from flask_cors import CORS
from metabase import save_predictions_to_db

from helpers import (
    validate_prediction_request,
    load_forecast_for_city,
    build_prediction_results,
    build_prediction_response_payload,
    build_uploaded_history_map
)

# ITEM MODEL
ITEM_MODEL_PATH = "model_artifacts/xgb_sales_model.pkl"
ITEM_FEATURES_PATH = "model_artifacts/feature_columns.pkl"
ITEM_ENCODERS_PATH = "model_artifacts/item_model_encoders.json"

item_model = joblib.load(ITEM_MODEL_PATH)
item_feature_cols = joblib.load(ITEM_FEATURES_PATH)

with open(ITEM_ENCODERS_PATH, "r") as f:
    item_encoders = json.load(f)

# CATEGORY MODEL
CATEGORY_MODEL_PATH = "model_artifacts/xgb_category_model.pkl"
CATEGORY_FEATURES_PATH = "model_artifacts/category_feature_columns.pkl"
CATEGORY_ENCODERS_PATH = "model_artifacts/category_model_encoders.json"

category_model = joblib.load(CATEGORY_MODEL_PATH)
category_feature_cols = joblib.load(CATEGORY_FEATURES_PATH)

with open(CATEGORY_ENCODERS_PATH, "r") as f:
    category_encoders = json.load(f)

# ENV / CONFIG
API_KEY = os.getenv("API_KEY")
CITY = "Akron"

SECRET_KEY = os.getenv("METABASE_SECRET_KEY")
DASHBOARD_ID = int(os.getenv("METABASE_DASHBOARD_ID", 2))

DB_CONFIG = {
    "host": os.getenv("MYSQL_HOST", "db"),
    "user": os.getenv("MYSQL_USER", "root"),
    "password": os.getenv("MYSQL_PASSWORD", "root"),
    "database": os.getenv("MYSQL_DATABASE", "weather_sales"),
    "port": int(os.getenv("MYSQL_PORT", 3306))
}

FRONTEND_FOLDER = "/app/frontend"
CSV_PATH = "/app/raw/forecast_ready_dataset.csv"
PREDICTION_OUTPUT_FOLDER = "/app/prediction_outputs"

os.makedirs(PREDICTION_OUTPUT_FOLDER, exist_ok=True)

# APP SETUP
app = Flask(__name__)
CORS(app, resources={r"/api/*": {"origins": "*"}})

# FRONTEND ROUTES
@app.route("/")
def root_index():
    return send_from_directory(FRONTEND_FOLDER, "index.html")

@app.route("/<path:filename>")
@app.route("/frontend/<path:filename>")
def serve_static(filename):
    return send_from_directory(FRONTEND_FOLDER, filename)

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
FRONTEND_FOLDER = os.path.join(BASE_DIR, "frontend")

print("Frontend folder:", FRONTEND_FOLDER)
print("Index exists:", os.path.exists(os.path.join(FRONTEND_FOLDER, "index.html")))


def generate_suggestions(df):
    df = df.copy()
    suggestions = []

    for _, row in df.iterrows():
        predicted_sales = float(row.get("predicted_sales", 0))
        snowfall = float(row.get("snowfall_sum", 0) or 0)
        rain = float(row.get("rain_sum", 0) or 0)
        has_event = int(row.get("has_any_event", 0) or 0)

        if predicted_sales >= 20:
            msg = "High expected demand. Restock heavily."
        elif predicted_sales >= 10:
            msg = "Moderate to high demand. Restock soon."
        elif predicted_sales >= 5:
            msg = "Moderate demand expected. Monitor inventory."
        else:
            msg = "Low demand expected. Current stock may be sufficient."

        if snowfall > 0:
            msg += " Snow may affect demand and delivery timing."
        elif rain > 0:
            msg += " Rain may slightly reduce store traffic."

        if has_event == 1:
            msg += " Event activity may increase demand."

        suggestions.append(msg)

    df["suggestion"] = suggestions
    return df


def sync_csv_to_db():
    """Reads a simple CSV and updates MySQL."""
    if os.path.exists(CSV_PATH):
        try:
            df = pd.read_csv(CSV_PATH)
            db = mysql.connector.connect(**DB_CONFIG)
            cursor = db.cursor()

            for _, row in df.iterrows():
                sql = """
                    INSERT INTO sales_features (date, total_quantity, total_revenue, city)
                    VALUES (%s, %s, %s, %s)
                    ON DUPLICATE KEY UPDATE
                        total_quantity = VALUES(total_quantity),
                        total_revenue = VALUES(total_revenue)
                """
                cursor.execute(
                    sql,
                    (
                        row["date"],
                        row["total_quantity"],
                        row["total_revenue"],
                        "Akron"
                    )
                )

            db.commit()
            cursor.close()
            db.close()
            print("CSV synced successfully.")

        except Exception as e:
            print(f"CSV Sync Error: {e}")



# EXISTING API ENDPOINTS
@app.route("/api/combined")
def combined():
    sync_csv_to_db()

    try:
        db = mysql.connector.connect(**DB_CONFIG)
        cursor = db.cursor(dictionary=True)

        query = """
            SELECT s.*, w.avg_temp, w.humidity, w.rainfall, w.wind_speed, w.forecast_type, w.description
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

        cursor.execute("""
            SELECT date, total_quantity
            FROM sales_features
            ORDER BY date DESC
            LIMIT 1
        """)
        latest_sales = cursor.fetchone() or {"date": "No Data", "total_quantity": 0}

        cursor.execute("""
            SELECT date, avg_temp, humidity
            FROM weather_data
            WHERE forecast_type = 'forecast' AND date > CURDATE()
            ORDER BY date ASC
            LIMIT 6
        """)
        forecast_weather = cursor.fetchall()

        cursor.close()
        db.close()

        return jsonify({
            "latest_sales_features": latest_sales,
            "forecast_weather": forecast_weather
        })

    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route("/api/get-metabase-token")
def get_token():
    payload = {
        "resource": {"dashboard": DASHBOARD_ID},
        "params": {},
        "exp": round(time.time()) + (60 * 10)
    }

    token = jwt.encode(payload, SECRET_KEY, algorithm="HS256")
    return jsonify({"token": token})

@app.route("/api/predict-csv-download", methods=["POST"])
def predict_csv_download():
    """
    Upload a CSV with model-ready feature columns and save/download predictions.
    """
    try:
        if "file" not in request.files:
            return jsonify({"error": "No file uploaded. Use form-data with key 'file'."}), 400

        file = request.files["file"]

        if file.filename == "":
            return jsonify({"error": "No selected file."}), 400

        if not file.filename.lower().endswith(".csv"):
            return jsonify({"error": "Please upload a CSV file."}), 400

        df = pd.read_csv(file)

        df = preprocess_input(df)

        missing_cols = [col for col in feature_cols if col not in df.columns]
        if missing_cols:
            return jsonify({
                "error": "Uploaded CSV is missing required feature columns.",
                "missing_columns": missing_cols
            }), 400

        X = df[feature_cols].copy()

        preds = model.predict(X)
        preds = np.clip(preds, 0, None)

        result_df = df.copy()
        result_df["predicted_sales"] = preds
        result_df = generate_suggestions(result_df)

        output_filename = f"predictions_{int(time.time())}.csv"
        output_path = os.path.join(PREDICTION_OUTPUT_FOLDER, output_filename)

        result_df.to_csv(output_path, index=False)

        return jsonify({
            "message": "Prediction file generated successfully.",
            "download_url": f"/api/download-prediction/{output_filename}",
            "rows_processed": len(result_df)
        })

    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route("/api/download-prediction/<filename>")
def download_prediction(filename):
    return send_from_directory(PREDICTION_OUTPUT_FOLDER, filename, as_attachment=True)

# HEALTH CHECK
@app.route("/api/health")
def health():
    return jsonify({
        "status": "ok",
        "item_model_loaded": os.path.exists(ITEM_MODEL_PATH),
        "item_features_loaded": os.path.exists(ITEM_FEATURES_PATH),
        "item_encoders_loaded": os.path.exists(ITEM_ENCODERS_PATH),
        "category_model_loaded": os.path.exists(CATEGORY_MODEL_PATH),
        "category_features_loaded": os.path.exists(CATEGORY_FEATURES_PATH),
        "category_encoders_loaded": os.path.exists(CATEGORY_ENCODERS_PATH)
    })

@app.route("/api/predict-csv-with-forecast", methods=["POST"])
def predict_csv_with_forecast():
    try:
        input_df, city = validate_prediction_request(request)
        forecast_df = load_forecast_for_city(city)

        result_df = build_prediction_results(input_df, city, forecast_df)

        save_predictions_to_db(result_df)

        payload = build_prediction_response_payload(result_df, input_df=input_df)
        return jsonify(payload), 200

    except ValueError as e:
        print("VALUE ERROR:", str(e))
        return jsonify({"error": str(e)}), 400

    except Exception as e:
        import traceback
        traceback.print_exc()
        print("PREDICTION ROUTE ERROR:", str(e))
        return jsonify({"error": str(e)}), 500

# MAIN
if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5050, debug=True, use_reloader=False)