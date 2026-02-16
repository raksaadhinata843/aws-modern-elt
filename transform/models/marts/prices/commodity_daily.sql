{{
    config(
        materialized='incremental',
        incremental_strategy='insert_overwrite',
        partitioned_by=['event_date'],
        external_location='s3://nama-bucket-lo/analytics/commodity_daily/'
    )
}}

SELECT
    date_trunc('day', ingested_at) AS event_date,
    commodity_id,
    AVG(price) AS avg_price,
    MAX(price) AS max_price
FROM {{ ref('stg_commodities') }}

{% if is_incremental() %}
  -- Cuma ambil data 7 hari terakhir karena lo fetch mingguan
  WHERE ingested_at >= date_add('day', -7, current_date)
{% endif %}

GROUP BY 1, 2

