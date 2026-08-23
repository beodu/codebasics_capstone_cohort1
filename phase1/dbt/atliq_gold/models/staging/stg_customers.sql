SELECT
    customer_id,
    customer_name,
    email,
    city,
    signup_date,
    updated_at
FROM {{ source('silver', 'customers') }}
