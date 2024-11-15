-- models/price_momentum.sql

WITH stock_data AS (
    SELECT
        stock_symbol,
        date,
        close,
        LAG(close, 14) OVER (PARTITION BY stock_symbol ORDER BY date) AS close_14_days_ago
    FROM {{ ref('stock_prices') }}
)

SELECT
    stock_symbol,
    date,
    close,
    close - close_14_days_ago AS price_momentum_14d
FROM stock_data
ORDER BY stock_symbol, date
