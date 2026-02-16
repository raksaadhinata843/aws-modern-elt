{{ config(materialized='table') }}

WITH ranked AS (
    SELECT
        *,
        ROW_NUMBER() OVER (
            PARTITION BY commodity_id 
            ORDER BY ingested_at DESC
        ) AS rn
    FROM {{ ref('stg_commodities') }}
)

SELECT
    commodity_id,
    price,
    price_unit,
    ingested_at AS last_updated
FROM ranked
WHERE rn = 1

