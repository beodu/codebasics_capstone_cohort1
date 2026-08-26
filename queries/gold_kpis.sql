-- AtliQ warehouse — connect to atliq_dw
SELECT
    COUNT(DISTINCT order_id) AS orders,
    COUNT(*)                 AS line_items,
    CAST(ROUND(SUM(gross_revenue), 2) AS DECIMAL(14,2)) AS gross_revenue,
    CAST(ROUND(SUM(CASE WHEN is_revenue = 1 THEN gross_revenue ELSE 0 END), 2) AS DECIMAL(14,2)) AS net_revenue
FROM gold.fact_sales;

SELECT TOP 10
    p.product_name,
    CAST(ROUND(SUM(f.gross_revenue), 2) AS DECIMAL(14,2)) AS net_revenue
FROM gold.fact_sales f
JOIN gold.dim_product p ON f.product_id = p.product_id
WHERE f.is_revenue = 1
GROUP BY p.product_name
ORDER BY SUM(f.gross_revenue) DESC;

SELECT TOP 10 run_id, stage, table_name, rows_in, rows_after, status, duration_secs, started_at
FROM ops.pipeline_run_log
ORDER BY started_at DESC;
