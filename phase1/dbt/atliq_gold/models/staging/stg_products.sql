SELECT
    product_id,
    product_name,
    category,
    unit_price,
    updated_at
FROM {{ source('silver', 'products') }}
