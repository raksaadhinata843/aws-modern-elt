-- Cek jumlah data yang masuk per tanggal ingest
SELECT 
    date(ingested_at) as ingest_date,
    COUNT(*) as total_records,
    COUNT(DISTINCT commodity_id) as unique_commodities
FROM 
    staging.stg_commodities -- Database & Table hasil dbt staging
GROUP BY 
    1
ORDER BY 
    1 DESC;
