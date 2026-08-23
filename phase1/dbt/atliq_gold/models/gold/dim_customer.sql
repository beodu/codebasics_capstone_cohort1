{{ config(materialized='table') }}

-- One row per customer. The signup cohort is stamped here rather than computed
-- in Power BI so that "new vs returning" means the same thing in every visual.

SELECT
    c.customer_id,
    c.customer_name,
    c.email,
    c.city,
    c.signup_date,
    date_format(c.signup_date, 'yyyy-MM') AS signup_cohort,
    YEAR(c.signup_date)                   AS signup_year,
    c.updated_at
FROM {{ ref('stg_customers') }} c
