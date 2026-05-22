import os
import json
import joblib
import numpy as np
import pandas as pd

from sklearn.metrics import mean_absolute_error, mean_squared_error, r2_score
from xgboost import XGBRegressor

# CONFIG
DATA_PATH = os.getenv("DATA_PATH", "strong_weather_retail_training_dataset.csv")
STATE_FILTER = os.getenv("STATE_FILTER", "") 
SPLIT_DATE = os.getenv("SPLIT_DATE", "2025-10-01")
OUTPUT_DIR = "model_artifacts"

# LOAD DATA
def load_data():
    if not os.path.exists(DATA_PATH):
        raise FileNotFoundError(f"Dataset not found: {DATA_PATH}")

    print(f"Loading dataset from: {DATA_PATH}")
    df = pd.read_csv(DATA_PATH)

    if STATE_FILTER:
        df = df[df["state_id"] == STATE_FILTER].copy()
        print(f"Applied STATE_FILTER={STATE_FILTER}, rows left: {len(df):,}")

    print(f"Successfully loaded {len(df):,} rows into memory.")
    return df

# PREPROCESS ITEM-LEVEL DATA
def preprocess_item_data(df):
    df = df.copy()

    # Basic conversions
    df["date"] = pd.to_datetime(df["date"], errors="coerce")
    df["sales"] = pd.to_numeric(df["sales"], errors="coerce")

    # Drop bad rows
    df = df.dropna(subset=["date", "sales"]).copy()

    # Sort for time-series consistency
    sort_cols = [c for c in ["item_id", "store_id", "date"] if c in df.columns]
    if sort_cols:
        df = df.sort_values(sort_cols).reset_index(drop=True)

    # Convert weekday names to numeric if needed
    if "weekday" in df.columns and df["weekday"].dtype == "object":
        weekday_map = {
            "Monday": 1,
            "Tuesday": 2,
            "Wednesday": 3,
            "Thursday": 4,
            "Friday": 5,
            "Saturday": 6,
            "Sunday": 7
        }
        df["weekday"] = df["weekday"].map(weekday_map)

    # Save encoders for prediction time
    encoders = {}
    id_cols = ["item_id", "store_id", "dept_id", "cat_id"]

    for col in id_cols:
        if col in df.columns:
            cat_series = df[col].astype("category")
            encoders[col] = {
                str(category): int(code)
                for code, category in enumerate(cat_series.cat.categories)
            }
            df[col] = cat_series.cat.codes

    # Convert bool columns
    bool_cols = df.select_dtypes(include=["bool"]).columns
    for col in bool_cols:
        df[col] = df[col].astype(int)

    # Fill numeric NaNs
    numeric_cols = df.select_dtypes(include=[np.number]).columns
    df[numeric_cols] = df[numeric_cols].fillna(0)

    # Interaction features
    if "temperature_2m_mean" in df.columns and "cat_id" in df.columns:
        df["temp_x_cat"] = df["temperature_2m_mean"] * df["cat_id"]

    if "rain_sum" in df.columns and "cat_id" in df.columns:
        df["rain_x_cat"] = df["rain_sum"] * df["cat_id"]

    if "is_weekend" in df.columns and "temperature_2m_mean" in df.columns:
        df["temp_x_weekend"] = df["temperature_2m_mean"] * df["is_weekend"]

    if "is_weekend" in df.columns and "rain_sum" in df.columns:
        df["rain_x_weekend"] = df["rain_sum"] * df["is_weekend"]

    if "month" in df.columns and "temperature_2m_mean" in df.columns:
        df["temp_x_month"] = df["temperature_2m_mean"] * df["month"]

    if "temperature_2m_mean" in df.columns and "rain_sum" in df.columns:
        df["temp_x_rain"] = df["temperature_2m_mean"] * df["rain_sum"]

    return df, encoders


# BUILD CATEGORY-LEVEL DATASET
def build_category_level_dataset(df):
    df = df.copy()

    required_cols = ["cat_id", "store_id", "date", "sales"]
    for col in required_cols:
        if col not in df.columns:
            raise ValueError(f"Missing required column for category model: {col}")

    df["date"] = pd.to_datetime(df["date"], errors="coerce")
    df["sales"] = pd.to_numeric(df["sales"], errors="coerce")
    df = df.dropna(subset=["date", "sales"]).copy()

    agg_map = {
        "sales": "sum",
        "sell_price": "mean",
        "snap": "max",
        "is_weekend": "max",
        "month": "first",
        "year": "first",
        "quarter": "first",
        "day_of_month": "first",
        "day_of_year": "first",
        "week_of_year": "first",
        "weekday": "first",
        "temperature_2m_max": "mean",
        "temperature_2m_min": "mean",
        "temperature_2m_mean": "mean",
        "apparent_temperature_max": "mean",
        "apparent_temperature_min": "mean",
        "precipitation_sum": "mean",
        "rain_sum": "mean",
        "snowfall_sum": "mean",
        "wind_speed_10m_max": "mean",
        "weather_code": "first",
        "is_rainy": "max",
        "is_snowy": "max",
        "is_hot": "max",
        "is_cold": "max",
        "has_event_1": "max",
        "has_event_2": "max",
        "has_any_event": "max",
    }

    available_agg = {k: v for k, v in agg_map.items() if k in df.columns}

    cat_df = (
        df.groupby(["cat_id", "store_id", "date"], as_index=False)
        .agg(available_agg)
        .sort_values(["cat_id", "store_id", "date"])
        .reset_index(drop=True)
    )

    group_cols = ["cat_id", "store_id"]

    cat_df["lag_1"] = cat_df.groupby(group_cols)["sales"].shift(1)
    cat_df["lag_7"] = cat_df.groupby(group_cols)["sales"].shift(7)

    cat_df["rolling_mean_7"] = (
        cat_df.groupby(group_cols)["sales"]
        .transform(lambda s: s.shift(1).rolling(7, min_periods=1).mean())
    )

    cat_df["rolling_std_7"] = (
        cat_df.groupby(group_cols)["sales"]
        .transform(lambda s: s.shift(1).rolling(7, min_periods=1).std())
    )

    cat_df["rolling_std_7"] = cat_df["rolling_std_7"].fillna(0)
    cat_df = cat_df.dropna(subset=["date"]).copy()

    return cat_df

# ENCODE CATEGORY-LEVEL DATA
def encode_category_level_data(cat_df):
    cat_df = cat_df.copy()
    encoders = {}

    for col in ["cat_id", "store_id"]:
        if col in cat_df.columns:
            cat_series = cat_df[col].astype("category")
            encoders[col] = {
                str(category): int(code)
                for code, category in enumerate(cat_series.cat.categories)
            }
            cat_df[col] = cat_series.cat.codes

    if "weekday" in cat_df.columns and cat_df["weekday"].dtype == "object":
        weekday_map = {
            "Monday": 1,
            "Tuesday": 2,
            "Wednesday": 3,
            "Thursday": 4,
            "Friday": 5,
            "Saturday": 6,
            "Sunday": 7
        }
        cat_df["weekday"] = cat_df["weekday"].map(weekday_map)

    # Interaction features
    if "temperature_2m_mean" in cat_df.columns and "cat_id" in cat_df.columns:
        cat_df["temp_x_cat"] = cat_df["temperature_2m_mean"] * cat_df["cat_id"]

    if "rain_sum" in cat_df.columns and "cat_id" in cat_df.columns:
        cat_df["rain_x_cat"] = cat_df["rain_sum"] * cat_df["cat_id"]

    if "temperature_2m_mean" in cat_df.columns and "is_weekend" in cat_df.columns:
        cat_df["temp_x_weekend"] = cat_df["temperature_2m_mean"] * cat_df["is_weekend"]

    if "rain_sum" in cat_df.columns and "is_weekend" in cat_df.columns:
        cat_df["rain_x_weekend"] = cat_df["rain_sum"] * cat_df["is_weekend"]

    if "temperature_2m_mean" in cat_df.columns and "month" in cat_df.columns:
        cat_df["temp_x_month"] = cat_df["temperature_2m_mean"] * cat_df["month"]

    if "temperature_2m_mean" in cat_df.columns and "rain_sum" in cat_df.columns:
        cat_df["temp_x_rain"] = cat_df["temperature_2m_mean"] * cat_df["rain_sum"]

    bool_cols = cat_df.select_dtypes(include=["bool"]).columns
    for col in bool_cols:
        cat_df[col] = cat_df[col].astype(int)

    numeric_cols = cat_df.select_dtypes(include=[np.number]).columns
    cat_df[numeric_cols] = cat_df[numeric_cols].fillna(0)

    return cat_df, encoders

# FEATURE SELECTION
def build_item_feature_list(df):
    drop_cols = [
        "sales",
        "date",
        "state_id",
        "item_id",
        "dept_id",
        "cat_id",

        "cat_avg_sales",

        # raw text columns
        "event_name_1",
        "event_type_1",
        "event_name_2",
        "event_type_2",

        # leakage / target-derived
        "daily_total_sales",
        "cat_daily_sales",
        "dept_daily_sales",
        "item_daily_share",
        "sales_diff_1",
        "sales_diff_7",
        "sales_pct_change_1",
        "is_zero_sales",
        "expected_daily_demand",
        "safety_stock",
        "reorder_point_7d",

        "lag_7",
        "rolling_std_7",
    ]

    feature_cols = [c for c in df.columns if c not in drop_cols]

    print("\nITEM MODEL feature columns:")
    for col in feature_cols:
        print(f" - {col}")

    return feature_cols


def build_category_feature_list(df):
    drop_cols = [
        "sales",
        "date",
        "lag_7",
        "rolling_std_7",
    ]

    feature_cols = [c for c in df.columns if c not in drop_cols]

    print("\nCATEGORY MODEL feature columns:")
    for col in feature_cols:
        print(f" - {col}")

    return feature_cols

# SPLIT
def time_based_split(df, feature_cols, split_date):
    df = df.copy()
    df["date"] = pd.to_datetime(df["date"], errors="coerce")
    df = df.dropna(subset=["date"]).copy()

    split_date = pd.to_datetime(split_date)

    train_full = df[df["date"] < split_date].copy()
    test_df = df[df["date"] >= split_date].copy()

    if train_full.empty or test_df.empty:
        raise ValueError(
            f"Train/test split failed. Check SPLIT_DATE={split_date.date()} and your data range."
        )

    val_cutoff = train_full["date"].quantile(0.85)

    train_df = train_full[train_full["date"] < val_cutoff].copy()
    val_df = train_full[train_full["date"] >= val_cutoff].copy()

    if train_df.empty or val_df.empty:
        raise ValueError("Train/validation split failed. Check your date coverage.")

    X_train = train_df[feature_cols]
    y_train = train_df["sales"]

    X_val = val_df[feature_cols]
    y_val = val_df["sales"]

    X_test = test_df[feature_cols]
    y_test = test_df["sales"]

    print(f"\nTrain rows: {len(train_df):,}")
    print(f"Validation rows: {len(val_df):,}")
    print(f"Test rows: {len(test_df):,}")

    print(f"Train range: {train_df['date'].min().date()} to {train_df['date'].max().date()}")
    print(f"Val range  : {val_df['date'].min().date()} to {val_df['date'].max().date()}")
    print(f"Test range : {test_df['date'].min().date()} to {test_df['date'].max().date()}")

    return X_train, y_train, X_val, y_val, X_test, y_test, test_df

# BASELINE
def evaluate_baseline(X_test, y_test):
    if "lag_1" not in X_test.columns:
        print("\nBaseline skipped because 'lag_1' is not available.")
        return None

    baseline_pred = X_test["lag_1"].values
    mae = mean_absolute_error(y_test, baseline_pred)
    rmse = np.sqrt(mean_squared_error(y_test, baseline_pred))
    r2 = r2_score(y_test, baseline_pred)

    print("\nBaseline (predict sales = lag_1)")
    print(f"MAE  : {mae:.4f}")
    print(f"RMSE : {rmse:.4f}")
    print(f"R2   : {r2:.4f}")

    return baseline_pred

# TRAIN MODEL
def train_model(X_train, y_train, X_val, y_val):
    model = XGBRegressor(
        n_estimators=450,
        learning_rate=0.05,
        max_depth=5,
        min_child_weight=6,
        subsample=0.8,
        colsample_bytree=0.8,
        reg_alpha=0.25,
        reg_lambda=1.8,
        objective="reg:squarederror",
        random_state=42,
        n_jobs=-1,
        tree_method="hist",
        early_stopping_rounds=30
    )

    model.fit(
        X_train,
        y_train,
        eval_set=[(X_val, y_val)],
        verbose=50
    )

    return model

# EVALUATE MODEL
def evaluate_model(model, X_test, y_test):
    y_pred = model.predict(X_test)
    y_pred = np.clip(y_pred, 0, None)

    mae = mean_absolute_error(y_test, y_pred)
    rmse = np.sqrt(mean_squared_error(y_test, y_pred))
    r2 = r2_score(y_test, y_pred)

    print("\nModel Evaluation")
    print(f"MAE  : {mae:.4f}")
    print(f"RMSE : {rmse:.4f}")
    print(f"R2   : {r2:.4f}")

    return y_pred

# FEATURE IMPORTANCE
def show_feature_importance(model, feature_cols, top_n=25):
    importance_df = pd.DataFrame({
        "feature": feature_cols,
        "importance": model.feature_importances_
    }).sort_values("importance", ascending=False)

    print(f"\nTop {top_n} Features")
    print(importance_df.head(top_n).to_string(index=False))

    return importance_df

# SAVE ARTIFACTS
def save_item_artifacts(model, feature_cols, encoders, test_df, y_pred, importance_df):
    os.makedirs(OUTPUT_DIR, exist_ok=True)

    model_path = os.path.join(OUTPUT_DIR, "xgb_sales_model.pkl")
    features_path = os.path.join(OUTPUT_DIR, "feature_columns.pkl")
    encoders_path = os.path.join(OUTPUT_DIR, "item_model_encoders.json")
    preds_path = os.path.join(OUTPUT_DIR, "test_predictions.csv")
    importance_path = os.path.join(OUTPUT_DIR, "feature_importance.csv")

    joblib.dump(model, model_path)
    joblib.dump(feature_cols, features_path)

    with open(encoders_path, "w") as f:
        json.dump(encoders, f, indent=2)

    results = test_df.copy()
    results["predicted_sales"] = y_pred
    results["prediction_error"] = results["sales"] - results["predicted_sales"]
    results.to_csv(preds_path, index=False)

    importance_df.to_csv(importance_path, index=False)

    print("\nSaved item-level artifacts:")
    print(f" - {model_path}")
    print(f" - {features_path}")
    print(f" - {encoders_path}")
    print(f" - {preds_path}")
    print(f" - {importance_path}")


def save_category_artifacts(model, feature_cols, encoders, test_df, y_pred, importance_df):
    os.makedirs(OUTPUT_DIR, exist_ok=True)

    model_path = os.path.join(OUTPUT_DIR, "xgb_category_model.pkl")
    features_path = os.path.join(OUTPUT_DIR, "category_feature_columns.pkl")
    encoders_path = os.path.join(OUTPUT_DIR, "category_model_encoders.json")
    preds_path = os.path.join(OUTPUT_DIR, "category_test_predictions.csv")
    importance_path = os.path.join(OUTPUT_DIR, "category_feature_importance.csv")

    joblib.dump(model, model_path)
    joblib.dump(feature_cols, features_path)

    with open(encoders_path, "w") as f:
        json.dump(encoders, f, indent=2)

    results = test_df.copy()
    results["predicted_sales"] = y_pred
    results["prediction_error"] = results["sales"] - results["predicted_sales"]
    results.to_csv(preds_path, index=False)

    importance_df.to_csv(importance_path, index=False)

    print("\nSaved category-level artifacts:")
    print(f" - {model_path}")
    print(f" - {features_path}")
    print(f" - {encoders_path}")
    print(f" - {preds_path}")
    print(f" - {importance_path}")

# MAIN
def main():
    print("Loading data from CSV...")
    df_raw = load_data()

    # ITEM MODEL
    print("Preprocessing item-level data...")
    df_item, item_encoders = preprocess_item_data(df_raw)

    print("Selecting item-level features...")
    item_feature_cols = build_item_feature_list(df_item)

    print("Splitting item-level train/validation/test...")
    X_train, y_train, X_val, y_val, X_test, y_test, test_df = time_based_split(
        df_item, item_feature_cols, SPLIT_DATE
    )

    print("Evaluating item-level baseline...")
    evaluate_baseline(X_test, y_test)

    print("Training item-level XGBoost model...")
    item_model = train_model(X_train, y_train, X_val, y_val)

    print("Evaluating item-level model...")
    y_pred = evaluate_model(item_model, X_test, y_test)

    print("Checking item-level feature importance...")
    importance_df = show_feature_importance(item_model, item_feature_cols, top_n=20)

    print("Saving item-level artifacts...")
    save_item_artifacts(item_model, item_feature_cols, item_encoders, test_df, y_pred, importance_df)

    # CATEGORY MODEL
    print("\n=========================")
    print("Building category-level dataset...")
    cat_df_raw = build_category_level_dataset(df_raw)

    print("Encoding category-level data...")
    cat_df, cat_encoders = encode_category_level_data(cat_df_raw)

    print("Selecting category-level features...")
    cat_feature_cols = build_category_feature_list(cat_df)

    print("Splitting category-level train/validation/test...")
    X_train_c, y_train_c, X_val_c, y_val_c, X_test_c, y_test_c, test_df_c = time_based_split(
        cat_df, cat_feature_cols, SPLIT_DATE
    )

    print("Evaluating category-level baseline...")
    evaluate_baseline(X_test_c, y_test_c)

    print("Training category-level XGBoost model...")
    category_model = train_model(X_train_c, y_train_c, X_val_c, y_val_c)

    print("Evaluating category-level model...")
    y_pred_c = evaluate_model(category_model, X_test_c, y_test_c)

    print("Checking category-level feature importance...")
    importance_df_c = show_feature_importance(category_model, cat_feature_cols, top_n=20)

    print("Saving category-level artifacts...")
    save_category_artifacts(category_model, cat_feature_cols, cat_encoders, test_df_c, y_pred_c, importance_df_c)

    print("\nBoth item-level and category-level training complete.")


if __name__ == "__main__":
    main()