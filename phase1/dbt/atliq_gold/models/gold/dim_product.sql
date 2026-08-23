{{ config(materialized='table') }}

-- Product catalog enriched with supplier cost from the external CSV.
-- LEFT JOIN on purpose: a product with no supplier row is still a real product
-- and must not vanish from the dimension (that would orphan its fact rows).

SELECT
    p.product_id,
    p.product_name,
    p.category,
    p.unit_price,
    s.supplier_name,
    s.supplier_cost,
    p.unit_price - s.supplier_cost AS unit_margin,
    CASE
        WHEN s.supplier_cost IS NULL OR p.unit_price = 0 THEN NULL
        ELSE ROUND((p.unit_price - s.supplier_cost) / p.unit_price * 100, 2)
    END AS margin_pct,
    p.updated_at
FROM {{ ref('stg_products') }} p
LEFT JOIN {{ ref('stg_supplier_price_list') }} s
       ON p.product_id = s.product_id
