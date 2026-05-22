import mysql.connector
import os

DB_CONFIG = {
    "host": os.getenv("MYSQL_HOST", "db"),
    "user": os.getenv("MYSQL_USER", "root"),
    "password": os.getenv("MYSQL_PASSWORD", "root"),
    "database": os.getenv("MYSQL_DATABASE", "weather_sales"),
    "port": int(os.getenv("MYSQL_PORT", 3306))
}

def save_predictions_to_db(df):
    print(" save_predictions_to_db called")

    if df is None:
        print(" df is None")
        return

    if df.empty:
        print(" df is empty")
        return

    print(" rows to insert:", len(df))
    print(df.head())

    db = mysql.connector.connect(**DB_CONFIG)
    cursor = db.cursor()

    cursor.execute("""
        CREATE TABLE IF NOT EXISTS prediction_results (
            id INT AUTO_INCREMENT PRIMARY KEY,
            date DATE,
            city VARCHAR(50),
            category VARCHAR(100),
            item_id VARCHAR(100),
            store_id VARCHAR(100),
            sell_price FLOAT,
            temperature FLOAT,
            rain FLOAT,
            snowfall FLOAT,
            predicted_sales FLOAT,
            suggestion VARCHAR(255),
            prediction_level VARCHAR(50),
            confidence VARCHAR(50)
        )
    """)

    cursor.execute("DELETE FROM prediction_results")

    insert_sql = """
        INSERT INTO prediction_results (
            date, city, category, item_id, store_id,
            sell_price, temperature, rain, snowfall,
            predicted_sales, suggestion, prediction_level, confidence
        )
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    """

    rows = []
    for _, row in df.iterrows():
        row_tuple = (
            row.get("date"),
            row.get("city"),
            row.get("category"),
            row.get("item_id"),
            row.get("store_id"),
            float(row.get("sell_price", 0) or 0),
            float(row.get("temperature_2m_mean", 0) or 0),
            float(row.get("rain_sum", 0) or 0),
            float(row.get("snowfall_sum", 0) or 0),
            float(row.get("predicted_sales", 0) or 0),
            row.get("suggestion"),
            row.get("prediction_level"),
            row.get("confidence"),
        )
        rows.append(row_tuple)

    print(" prepared insert rows:", len(rows))
    if rows:
        print(" first row:", rows[0])

    cursor.executemany(insert_sql, rows)
    db.commit()
    print(" insert committed")

    cursor.close()
    db.close()