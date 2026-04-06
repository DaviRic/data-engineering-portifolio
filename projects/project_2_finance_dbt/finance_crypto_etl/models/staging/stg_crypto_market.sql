WITH raw_data AS (
    SELECT * FROM {{ source('coingecko', 'raw_market_data') }}
)

SELECT
    id AS crypto_id,
    symbol,
    name,
    current_price,
    market_cap,
    total_volume,
    price_change_percentage_24h,
    CAST(ingested_at AS TIMESTAMP) AS loaded_at
FROM raw_data
WHERE id IS NOT NULL