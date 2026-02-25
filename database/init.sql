CREATE DATABASE IF NOT EXISTS weather_sales;
USE weather_sales;

CREATE TABLE IF NOT EXISTS sales (
    id INT AUTO_INCREMENT PRIMARY KEY,
    city VARCHAR(50),
    customer_type VARCHAR(20),
    gender VARCHAR(10),
    category VARCHAR(50),
    total DECIMAL(10,2),
    quantity INT,
    tax DECIMAL(10,4),
    gross_income DECIMAL(10,3),
    sale_date DATE
);

CREATE TABLE IF NOT EXISTS weather_data (
    id INT AUTO_INCREMENT PRIMARY KEY,
    city VARCHAR(100),
    date DATE,
    avg_temp FLOAT,
    description VARCHAR(255),
    rainfall FLOAT DEFAULT 0,
    humidity INT,
    UNIQUE KEY unique_city_date (city, date)
);

LOAD DATA LOCAL INFILE '/var/lib/mysql-files/sales.csv'
INTO TABLE sales
FIELDS TERMINATED BY ','
ENCLOSED BY '"'
LINES TERMINATED BY '\n'
IGNORE 1 ROWS
(@invoice, @branch, city, customer_type, gender, category, total, quantity, tax, gross_income, sale_date)
SET invoice = @invoice,
    branch = @branch;