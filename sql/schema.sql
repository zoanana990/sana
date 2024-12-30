CREATE DATABASE IF NOT EXISTS stock_data;
USE stock_data;

CREATE TABLE IF NOT EXISTS stock_prices (
    id INT AUTO_INCREMENT PRIMARY KEY,
    stock_no VARCHAR(10) NOT NULL,
    date DATE NOT NULL,
    volume BIGINT,
    turnover BIGINT,
    open_price DECIMAL(10,2),
    high_price DECIMAL(10,2),
    low_price DECIMAL(10,2),
    close_price DECIMAL(10,2),
    price_change VARCHAR(10),
    transaction_count INT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE KEY stock_date (stock_no, date)
);

CREATE TABLE IF NOT EXISTS stock_income (
    id INT AUTO_INCREMENT PRIMARY KEY,
    stock_no VARCHAR(10) NOT NULL,
    date DATE NOT NULL,
    revenue BIGINT,           -- revenue
    profit BIGINT,            -- profit
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE KEY stock_income_date (stock_no, date)
); 