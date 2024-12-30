WITH monthly_data AS (
    SELECT 
        DATE_FORMAT(date, '%Y-%m-01') as period_start,
        MIN(date) as first_date,
        MAX(date) as last_date,
        MAX(high_price) as high_price,
        MIN(low_price) as low_price,
        SUM(volume) as volume
    FROM stock_prices 
    WHERE stock_no = %s
    GROUP BY DATE_FORMAT(date, '%Y-%m-01')
)
SELECT 
    md.period_start as date,
    sp1.open_price,
    md.high_price,
    md.low_price,
    sp2.close_price,
    md.volume
FROM monthly_data md
JOIN stock_prices sp1 ON sp1.date = md.first_date AND sp1.stock_no = %s
JOIN stock_prices sp2 ON sp2.date = md.last_date AND sp2.stock_no = %s
ORDER BY md.period_start; 