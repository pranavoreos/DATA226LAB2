{% snapshot snapshot_session_summary %}

{{
  config(
    target_schema="snapshot",
    unique_key="stock_symbol",
    strategy="timestamp",
    updated_at="ts",
    invalidate_hard_deletes=True)
}}

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
FROM {{ ref('session_summary') }}

{% endsnapshot %}
