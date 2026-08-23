{{ config(materialized='table') }}

-- Daily ad spend by channel, keyed to dim_date so spend and revenue can sit on
-- the same axis. Separate fact table because the grain is different
-- (day x channel, not order item) — forcing it into fact_sales would multiply
-- revenue by the number of channels.

SELECT
    CAST(date_format(m.spend_date, 'yyyyMMdd') AS INT) AS date_key,
    m.spend_date,
    m.channel,
    m.campaign,
    m.spend_amount,
    m.clicks,
    CASE WHEN m.clicks > 0 THEN ROUND(m.spend_amount / m.clicks, 2) END AS cost_per_click
FROM {{ ref('stg_marketing_spend') }} m
