WITH session_data AS (
    SELECT
        stock_symbol,
        MIN(date) AS start_date,
        MAX(date) AS end_date,
        COUNT(DISTINCT date) AS trading_days,
        AVG(close) AS average_close_price,
        MIN(close) AS lowest_close_price,
        MAX(close) AS highest_close_price,
        SUM(volume) AS total_volume,
        CURRENT_TIMESTAMP AS ts  
    FROM {{ ref('stock_prices') }}
    GROUP BY stock_symbol
)

SELECT
    stock_symbol,
    start_date,
    end_date,
    trading_days,
    average_close_price,
    lowest_close_price,
    highest_close_price,
    total_volume,
    ts
FROM session_data
ORDER BY stock_symbol
