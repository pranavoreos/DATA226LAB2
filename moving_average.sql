-- models/moving_average.sql

WITH stock_data AS (
    SELECT
        stock_symbol,
        date,
        close
    FROM {{ ref('stock_prices') }}  -- Corrected reference to the staging table
)

SELECT
    stock_symbol,
    date,
    close,
    AVG(close) OVER (
        PARTITION BY stock_symbol
        ORDER BY date
        ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
    ) AS moving_average_7d
FROM stock_data
ORDER BY stock_symbol, date
