SELECT
    spend_date,
    channel,
    campaign,
    spend_amount,
    clicks
FROM {{ source('silver', 'marketing_spend') }}
