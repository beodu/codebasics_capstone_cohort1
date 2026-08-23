{{ config(materialized='table') }}

-- Generated calendar spanning every date the business can reference: the
-- earliest customer signup through the end of next year. Building it rather
-- than deriving it from order dates means gaps (a day with no orders) still
-- appear on the trend line instead of silently collapsing.

WITH date_spine AS (
    SELECT explode(sequence(
        to_date('{{ var("date_start") }}'),
        to_date('{{ var("date_end") }}'),
        interval 1 day
    )) AS date_day
)

SELECT
    CAST(date_format(date_day, 'yyyyMMdd') AS INT) AS date_key,
    date_day                                       AS full_date,
    YEAR(date_day)                                 AS year,
    QUARTER(date_day)                              AS quarter,
    CONCAT('Q', QUARTER(date_day), ' ', YEAR(date_day)) AS quarter_label,
    MONTH(date_day)                                AS month,
    date_format(date_day, 'MMMM')                  AS month_name,
    date_format(date_day, 'yyyy-MM')               AS year_month,
    DAY(date_day)                                  AS day_of_month,
    WEEKOFYEAR(date_day)                           AS week_of_year,
    date_format(date_day, 'EEEE')                  AS weekday_name,
    CASE WHEN date_format(date_day, 'E') IN ('Sat', 'Sun')
         THEN TRUE ELSE FALSE END                  AS is_weekend
FROM date_spine
