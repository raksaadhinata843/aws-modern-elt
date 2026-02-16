-- Analisis Trend Mingguan Komoditas
SELECT 
    commodity_id,
    event_date,
    avg_price,
    -- Menghitung persentase perubahan harga dibanding hari sebelumnya
    (avg_price - LAG(avg_price) OVER (PARTITION BY commodity_id ORDER BY event_date)) 
    / LAG(avg_price) OVER (PARTITION BY commodity_id ORDER BY event_date) * 100 AS pct_change
FROM 
    analytics.commodity_daily -- Database & Table hasil dbt marts
WHERE 
    event_date >= date_add('day', -30, current_date)
ORDER BY 
    event_date DESC, 
    avg_price DESC;

