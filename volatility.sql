-- models/analytics/volatility.sql

WITH stock_data AS (
    SELECT
        stock_symbol,
        date,
        close
    FROM {{ ref('stock_prices') }}  -- Refers to the staging model
),

price_variation AS (
    SELECT
        stock_symbol,
        date,
        close,
        -- Calculate the daily return by comparing the close price with the previous day's close
        (close - LAG(close, 1) OVER (PARTITION BY stock_symbol ORDER BY date)) / NULLIF(LAG(close, 1) OVER (PARTITION BY stock_symbol ORDER BY date), 0) AS daily_return
    FROM stock_data
),

volatility_calculation AS (
    SELECT
        stock_symbol,
        date,
        close,
        daily_return,
        -- Calculate the 10-day rolling standard deviation of daily returns as a measure of volatility
        STDDEV_SAMP(daily_return) OVER (
            PARTITION BY stock_symbol
            ORDER BY date
            ROWS BETWEEN 9 PRECEDING AND CURRENT ROW
        ) AS volatility_10d
    FROM price_variation
)

SELECT
    stock_symbol,
    date,
    close,
    volatility_10d
FROM volatility_calculation
ORDER BY stock_symbol, date
