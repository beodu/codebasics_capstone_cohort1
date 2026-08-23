-- The seed data guarantees order_amount equals the sum of its line items.
-- If this test ever fails, the fact table has gained or lost line items —
-- which is exactly what a broken (appending instead of merging) incremental
-- load looks like. This catches double-counting that a uniqueness test on
-- order_item_id would miss, because a duplicated *batch* can carry new
-- surrogate ids.

SELECT
    o.order_id,
    o.order_amount,
    SUM(f.gross_revenue) AS fact_total,
    ABS(o.order_amount - SUM(f.gross_revenue)) AS diff
FROM {{ ref('stg_orders') }} o
JOIN {{ ref('fact_sales') }} f
  ON o.order_id = f.order_id
GROUP BY o.order_id, o.order_amount
HAVING ABS(o.order_amount - SUM(f.gross_revenue)) > 0.01
