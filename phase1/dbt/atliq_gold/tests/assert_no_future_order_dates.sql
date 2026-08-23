-- An order dated tomorrow means a timezone bug in the ingest, not a real sale.
SELECT order_item_id, order_date
FROM {{ ref('fact_sales') }}
WHERE order_date > current_date()
