-- models/marts/dim_time.sql
{{ config(materialized='table') }}

WITH date_spine AS (
    SELECT DATEADD('day', SEQ4(), '2020-01-01'::DATE) AS calendar_date
    FROM TABLE(GENERATOR(ROWCOUNT => 3650))
)

SELECT
    TO_NUMBER(TO_CHAR(calendar_date, 'YYYYMMDD'))   AS time_sk,
    calendar_date,
    YEAR(calendar_date)                              AS year_num,
    QUARTER(calendar_date)                           AS quarter_num,
    MONTH(calendar_date)                             AS month_num,
    TO_CHAR(calendar_date, 'MMMM')                  AS month_name,
    WEEKOFYEAR(calendar_date)                        AS week_of_year,
    DAYOFWEEK(calendar_date)                         AS day_of_week,
    DAY(calendar_date)                               AS day_of_month,
    TO_CHAR(calendar_date, 'YYYY-MM')                AS year_month,
    CASE WHEN DAYOFWEEK(calendar_date) IN (0,6) 
         THEN TRUE ELSE FALSE END                    AS is_weekend,
    CASE
        WHEN TO_CHAR(calendar_date,'MM-DD') BETWEEN '11-25' AND '11-30'
            THEN 'black_friday_week'
        WHEN TO_CHAR(calendar_date,'MM-DD') BETWEEN '12-10' AND '12-31'
            THEN 'christmas_peak'
        WHEN TO_CHAR(calendar_date,'MM-DD') BETWEEN '01-08' AND '01-18'
            THEN 'winter_sales'
        WHEN TO_CHAR(calendar_date,'MM-DD') BETWEEN '06-25' AND '07-10'
            THEN 'summer_sales'
        ELSE 'standard'
    END                                              AS commercial_period,
    CURRENT_TIMESTAMP()                              AS dbt_updated_at
FROM date_spine