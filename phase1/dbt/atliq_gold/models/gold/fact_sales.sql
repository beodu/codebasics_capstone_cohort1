{{ config(
    materialized='table',
    file_format='delta'
) }}

-- one row per order item. cancelled stays, is_revenue = false.

SELECT
    oi.order_item_id,
    o.order_id,
    o.customer_id,
    oi.product_id,
    o.order_date,
    CAST(date_format(o.order_date, 'yyyyMMdd') AS INT) AS date_key,
    o.status,
    oi.quantity,
    oi.item_price,
    oi.quantity * oi.item_price AS gross_revenue,
    oi.quantity * COALESCE(dp.supplier_cost, 0) AS total_cost,
    CASE WHEN dp.supplier_cost IS NULL THEN NULL
         ELSE oi.quantity * (oi.item_price - dp.supplier_cost)
    END AS gross_profit,
    o.status NOT IN ('{{ var("excluded_statuses") | join("','") }}') AS is_revenue,
    o.status = 'Returned' AS is_returned,
    oi.created_at
FROM {{ ref('stg_order_items') }} oi
INNER JOIN {{ ref('stg_orders') }} o
        ON oi.order_id = o.order_id
LEFT JOIN {{ ref('dim_product') }} dp
       ON oi.product_id = dp.product_id
