-- Criando uma tabela física no BigQuery
{{ config(materialized='table') }}

WITH silver_data AS (
    SELECT * FROM {{ ref('stg_crypto_market') }}
),

totals AS (
    SELECT
        SUM(market_cap) AS total_market_cap_top_100
    FROM silver_data
)

SELECT
    s.crypto_id,
    s.name,
    s.symbol,
    s.market_cap,
    (s.market_cap/t.total_market_cap_top_100) * 100 AS market_share_percentage,
    s.loaded_at
FROM silver_data s, totals t
ORDER BY market_share_percentage DESC
