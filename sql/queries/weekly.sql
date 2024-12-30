WITH week_bounds AS (
    SELECT 
        YEARWEEK(date, 1) as yearweek,
        MIN(date) as first_date,
        MAX(date) as last_date,
        DATE_SUB(MIN(date), INTERVAL WEEKDAY(MIN(date)) DAY) as week_start
    FROM stock_prices 
    WHERE stock_no = %s
    GROUP BY YEARWEEK(date, 1)
),
weekly_data AS (
    SELECT 
        wb.week_start as period_start,
        wb.first_date,
        wb.last_date,
        MAX(sp.high_price) as high_price,
        MIN(sp.low_price) as low_price,
        SUM(sp.volume) as volume
    FROM week_bounds wb
    JOIN stock_prices sp ON YEARWEEK(sp.date, 1) = wb.yearweek
    WHERE sp.stock_no = %s
    GROUP BY wb.week_start, wb.first_date, wb.last_date
)
SELECT 
    wd.period_start as date,
    sp1.open_price,
    wd.high_price,
    wd.low_price,
    sp2.close_price,
    wd.volume
FROM weekly_data wd
JOIN stock_prices sp1 ON sp1.date = wd.first_date AND sp1.stock_no = %s
JOIN stock_prices sp2 ON sp2.date = wd.last_date AND sp2.stock_no = %s
ORDER BY wd.period_start; 