CREATE DATABASE IF NOT EXISTS weather_sales;
USE weather_sales;

DROP TABLE IF EXISTS sales;
DROP TABLE IF EXISTS sales_features;
DROP TABLE IF EXISTS weather_data;

CREATE TABLE sales_features (
    date DATE PRIMARY KEY,

    -- Core Targets we
    total_quantity INT NOT NULL,
    total_revenue FLOAT NOT NULL,

    -- Category Features
    Baby INT DEFAULT 0,
    `Baking/ Spices/ Condiments` INT DEFAULT 0,
    Beverages INT DEFAULT 0,
    Cleaning INT DEFAULT 0,
    Dairy INT DEFAULT 0,
    Food INT DEFAULT 0,
    Fruits INT DEFAULT 0,
    Hygiene INT DEFAULT 0,
    Meat INT DEFAULT 0,
    Miscellaneous INT DEFAULT 0,
    Pet INT DEFAULT 0,
    `School Supplies` INT DEFAULT 0,
    Snacks INT DEFAULT 0,
    Vegetables INT DEFAULT 0,
    city VARCHAR(50) DEFAULT 'Akron',

    -- Calendar Features
    day_of_week INT,
    month INT,
    is_weekend TINYINT(1),

    -- Lag Features
    lag_1 FLOAT,
    lag_3 FLOAT,
    lag_7 FLOAT,

    -- Rolling Features
    rolling_3 FLOAT,
    rolling_7 FLOAT,

    -- Timestamp
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE weather_data (
    date DATE PRIMARY KEY,

    avg_temp FLOAT NOT NULL,
    min_temp FLOAT,
    max_temp FLOAT,

    humidity INT NOT NULL,
    rainfall FLOAT NOT NULL DEFAULT 0 CHECK (rainfall >= 0),
    wind_speed FLOAT NOT NULL,

    description VARCHAR(100),

    forecast_type ENUM('historical', 'forecast') DEFAULT 'historical',

    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Add indexes
CREATE INDEX idx_sales_date ON sales_features(date);
CREATE INDEX idx_weather_date ON weather_data(date);