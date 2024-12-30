-- Get stock count
SELECT COUNT(*) 
FROM stock_prices 
WHERE stock_no = %s;

-- Get last update
SELECT MAX(date) 
FROM stock_prices 
WHERE stock_no = %s;

-- Get daily data
SELECT date, open_price, high_price, low_price, close_price, volume 
FROM stock_prices 
WHERE stock_no = %s
ORDER BY date;

-- List all stocks
SELECT stock_no, 
       MIN(date) as first_date, 
       MAX(date) as last_date,
       COUNT(*) as total_records,
       MIN(close_price) as min_price,
       MAX(close_price) as max_price
FROM stock_prices
GROUP BY stock_no
ORDER BY stock_no;

-- Insert or update stock data
REPLACE INTO stock_prices 
(stock_no, date, volume, turnover, open_price, high_price, 
 low_price, close_price, price_change, transaction_count)
VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s); 