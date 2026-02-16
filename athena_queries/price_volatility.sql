-- Mencari komoditas dengan standar deviasi harga tertinggi (paling fluktuatif)
SELECT 
    commodity_id,
    AVG(avg_price) as mean_price,
    STDDEV(avg_price) as price_volatility,
    COUNT(*) as total_days_tracked
FROM 
    analytics.commodity_daily
GROUP BY 
    1
HAVING 
    COUNT(*) > 1
ORDER BY 
    price_volatility DESC
LIMIT 10;

