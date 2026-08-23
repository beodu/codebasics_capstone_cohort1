SELECT
    payment_id,
    order_id,
    amount,
    method,
    paid_at,
    updated_at
FROM {{ source('silver', 'payments') }}
