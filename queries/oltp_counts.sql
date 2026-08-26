-- AtliQ OLTP — connect to atliq_commerce
SELECT 'customers'   AS tbl, COUNT(*) AS [rows] FROM dbo.customers
UNION ALL SELECT 'products',    COUNT(*) FROM dbo.products
UNION ALL SELECT 'orders',      COUNT(*) FROM dbo.orders
UNION ALL SELECT 'order_items', COUNT(*) FROM dbo.order_items
UNION ALL SELECT 'payments',    COUNT(*) FROM dbo.payments;

SELECT table_name, load_type, watermark_column, last_loaded_at
FROM etl.control_table
ORDER BY table_name;
