-- Get monthly income data
SELECT date, revenue, profit
FROM stock_income
WHERE stock_no = %s
ORDER BY date;

-- Insert or update income data
REPLACE INTO stock_income
(stock_no, date, revenue, profit)
VALUES (%s, %s, %s, %s);

-- Get last income update
SELECT MAX(date)
FROM stock_income
WHERE stock_no = %s;

-- Check income exists
SELECT COUNT(*)
FROM stock_income
WHERE stock_no = %s;

-- List all income summary
SELECT 
    stock_no,
    MIN(date) as first_date,
    MAX(date) as last_date,
    COUNT(*) as total_records,
    AVG(revenue) as avg_revenue,
    AVG(profit) as avg_profit
FROM stock_income
GROUP BY stock_no
ORDER BY stock_no; 