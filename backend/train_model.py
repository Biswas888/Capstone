import pandas as pd
import mysql.connector
import os
from dotenv import load_dotenv
from sklearn.ensemble import RandomForestRegressor
import joblib

load_dotenv()

# 1. Connect to MySQL
db = mysql.connector.connect(
    host=os.getenv("MYSQL_HOST", "mysql-db"),
    user=os.getenv("MYSQL_USER", "root"),
    password=os.getenv("MYSQL_PASSWORD", "root"),
    database=os.getenv("MYSQL_DB", "weather_sales")
)

# 2. Query the data (Merging sales and weather into one training set)
query = """
SELECT s.*, w.avg_temp, w.rainfall, w.wind_speed, w.humidity 
FROM sales_features s
JOIN weather_data w ON s.date = w.date
WHERE s.total_quantity > 0
"""
df = pd.read_sql(query, db)
db.close()

# 3. Define Features and Target
features = ['avg_temp', 'rainfall', 'wind_speed', 'humidity', 'day_of_week', 'month', 'is_weekend']
X = df[features]
y = df['total_quantity']

# 4. Train
model = RandomForestRegressor(n_estimators=100)
model.fit(X, y)

# 5. Save the model
joblib.dump(model, "sales_forecaster.pkl")
print("Model trained and saved as sales_forecaster.pkl")