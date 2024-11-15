-- models/staging/stock_prices.sql

WITH raw_stock_data AS (
    -- Replace this with the actual source of your raw stock data (e.g., raw table or staged CSV file)
    SELECT
        stock_symbol,
        date,
        open,
        close,
        min,       -- Minimum stock price
        max,       -- Maximum stock price
        volume     -- Volume of stocks traded
    FROM {{ source('STOCK_DATA_SCHEMA', 'STOCK_DATA') }}  -- Replace with actual source and table name
)

SELECT
    stock_symbol,
    date,
    open,
    close,
    min,
    max,
    volume
FROM raw_stock_data
WHERE date IS NOT NULL  -- Optional: Filter out any rows with missing date values
