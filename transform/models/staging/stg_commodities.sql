{{ config(materialized='view') }}

WITH source AS (
    SELECT * FROM {{ source('raw_eia', 'commodity_data') }}
),

renamed AS (
    SELECT
        -- Casting data EIA (biasanya formatnya series-id, period, value)
        CAST(series_id AS VARCHAR) AS commodity_id,
        CAST(period AS VARCHAR) AS observation_period,
        CAST(value AS DECIMAL(18, 2)) AS price,
        CAST(units AS VARCHAR) AS price_unit,
        -- Mengubah string period ke format date yang benar
        CAST(from_iso8601_timestamp(created_at) AS TIMESTAMP) AS ingested_at
    FROM source
)

SELECT * FROM renamed

