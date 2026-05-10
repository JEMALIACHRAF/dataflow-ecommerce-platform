-- models/marts/dim_time.sql
--
-- Snowflake Star Schema — Time Dimension
-- Pre-computed date spine from 2020-01-01 to 2030-12-31.
-- Materialised as a static table — never incremental.
--
-- Used by : fact_orders, gold_daily_sales, gold_funnel_metrics

{{
  config(
    materialized = 'table',
    tags         = ['marts', 'dimensions', 'static'],
    meta         = {
      'owner'      : 'data-engineering@dataflow.io',
      'description': 'Date dimension — covers 2020-01-01 to 2030-12-31'
    }
  )
}}

WITH

date_spine AS (
    {{ dbt_utils.date_spine(
        datepart   = "day",
        start_date = "cast('2020-01-01' as date)",
        end_date   = "cast('2030-12-31' as date)"
    ) }}
),

enriched AS (
    SELECT
        TO_NUMBER(TO_CHAR(date_day, 'YYYYMMDD'))        AS time_sk,       -- 20240115
        date_day                                        AS calendar_date,
        YEAR(date_day)                                  AS year_num,
        QUARTER(date_day)                               AS quarter_num,
        MONTH(date_day)                                 AS month_num,
        TO_CHAR(date_day, 'Month')                      AS month_name,
        TO_CHAR(date_day, 'Mon')                        AS month_name_short,
        WEEKOFYEAR(date_day)                            AS week_of_year,
        DAYOFWEEK(date_day)                             AS day_of_week,     -- 0=Sun
        DAYOFYEAR(date_day)                             AS day_of_year,
        TO_CHAR(date_day, 'Day')                        AS day_name,
        TO_CHAR(date_day, 'Dy')                         AS day_name_short,
        DAY(date_day)                                   AS day_of_month,

        -- Period keys (for aggregation shortcuts)
        TO_CHAR(date_day, 'YYYY-MM')                    AS year_month,      -- '2024-01'
        CONCAT('Q', QUARTER(date_day), '-', YEAR(date_day))
                                                        AS year_quarter,    -- 'Q1-2024'

        -- Flags
        CASE WHEN DAYOFWEEK(date_day) IN (0, 6) THEN TRUE ELSE FALSE END
                                                        AS is_weekend,
        CASE WHEN DAYOFWEEK(date_day) IN (0, 6) THEN FALSE ELSE TRUE END
                                                        AS is_weekday,

        -- French public holidays (static list — extend as needed)
        CASE
            WHEN TO_CHAR(date_day, 'MM-DD') IN (
                '01-01',  -- Jour de l'an
                '05-01',  -- Fête du travail
                '05-08',  -- Victoire 1945
                '07-14',  -- Fête nationale
                '08-15',  -- Assomption
                '11-01',  -- Toussaint
                '11-11',  -- Armistice
                '12-25'   -- Noël
            )
            THEN TRUE ELSE FALSE
        END                                             AS is_public_holiday_fr,

        -- Commercial peaks (key for auto-scaling triggers)
        CASE
            WHEN TO_CHAR(date_day, 'MM-DD') BETWEEN '11-25' AND '11-30'
                THEN 'black_friday_week'
            WHEN TO_CHAR(date_day, 'MM-DD') BETWEEN '12-10' AND '12-31'
                THEN 'christmas_peak'
            WHEN TO_CHAR(date_day, 'MM-DD') BETWEEN '01-08' AND '01-18'
                THEN 'winter_sales'
            WHEN TO_CHAR(date_day, 'MM-DD') BETWEEN '06-25' AND '07-10'
                THEN 'summer_sales'
            ELSE 'standard'
        END                                             AS commercial_period,

        -- Relative periods (refreshed at query time via dynamic flag)
        CASE WHEN date_day = CURRENT_DATE()               THEN TRUE ELSE FALSE END AS is_today,
        CASE WHEN date_day = CURRENT_DATE() - 1           THEN TRUE ELSE FALSE END AS is_yesterday,
        CASE WHEN date_day >= CURRENT_DATE() - 6          THEN TRUE ELSE FALSE END AS is_last_7d,
        CASE WHEN date_day >= CURRENT_DATE() - 29         THEN TRUE ELSE FALSE END AS is_last_30d,
        CASE WHEN date_day >= CURRENT_DATE() - 89         THEN TRUE ELSE FALSE END AS is_last_90d,
        CASE WHEN YEAR(date_day) = YEAR(CURRENT_DATE())   THEN TRUE ELSE FALSE END AS is_current_year

    FROM date_spine
)

SELECT * FROM enriched
ORDER BY calendar_date
