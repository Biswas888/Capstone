CREATE TABLE prediction_results (
    id INT AUTO_INCREMENT PRIMARY KEY,
    date DATE,
    city VARCHAR(50),
    category VARCHAR(50),
    item_id VARCHAR(50),
    store_id VARCHAR(50),
    predicted_sales FLOAT,
    temperature FLOAT,
    rain FLOAT,
    snowfall FLOAT,
    suggestion VARCHAR(255)
    prediction_level VARCHAR(20)
);