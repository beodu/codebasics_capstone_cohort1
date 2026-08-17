-- ============================================================================
--  AtliQ Commerce | OLTP load verification
--  Run after 01..07. Expected: 40, 25, 300, 783, 246.
-- ============================================================================

SELECT 'customers'   AS tbl, COUNT(*) AS [rows] FROM dbo.customers
UNION ALL SELECT 'products',    COUNT(*) FROM dbo.products
UNION ALL SELECT 'orders',      COUNT(*) FROM dbo.orders
UNION ALL SELECT 'order_items', COUNT(*) FROM dbo.order_items
UNION ALL SELECT 'payments',    COUNT(*) FROM dbo.payments;

-- Referential integrity: all three must return zero rows.
SELECT o.order_id FROM dbo.orders o
LEFT JOIN dbo.customers c ON o.customer_id = c.customer_id
WHERE c.customer_id IS NULL;

SELECT oi.order_item_id FROM dbo.order_items oi
LEFT JOIN dbo.products p ON oi.product_id = p.product_id
WHERE p.product_id IS NULL;

SELECT p.payment_id FROM dbo.payments p
LEFT JOIN dbo.orders o ON p.order_id = o.order_id
WHERE o.order_id IS NULL;

-- Revenue reconciles across all three tables: order header = sum of line items
-- = payment amount. Zero rows means the seed data is internally consistent, and
-- gives the pipeline a reference number to be checked against later.
SELECT o.order_id, o.order_amount, SUM(oi.quantity * oi.item_price) AS line_total
FROM dbo.orders o
JOIN dbo.order_items oi ON o.order_id = oi.order_id
GROUP BY o.order_id, o.order_amount
HAVING ABS(o.order_amount - SUM(oi.quantity * oi.item_price)) > 0.01;

-- The number the Fabric dashboard must reproduce exactly.
SELECT
    COUNT(DISTINCT o.order_id)              AS orders,
    SUM(oi.quantity * oi.item_price)        AS gross_revenue,
    SUM(CASE WHEN o.status <> 'Cancelled'
             THEN oi.quantity * oi.item_price ELSE 0 END) AS revenue_excl_cancelled
FROM dbo.orders o
JOIN dbo.order_items oi ON o.order_id = oi.order_id;

-- Watermark state: what the pipeline thinks it has already loaded.
SELECT table_name, load_type, watermark_column, last_loaded_at
FROM etl.control_table ORDER BY table_name;
