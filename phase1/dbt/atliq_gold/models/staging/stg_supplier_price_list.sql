SELECT
    product_id,
    supplier_name,
    supplier_cost,
    effective_date
FROM {{ source('silver', 'supplier_price_list') }}
