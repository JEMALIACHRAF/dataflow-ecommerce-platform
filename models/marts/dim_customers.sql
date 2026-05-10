-- models/marts/dim_customers.sql
--
-- Dimension: Customers  (SCD Type 2)
-- Tracks full history of customer profile changes (email, segment, tier).
-- Sources: Snowflake staging from MongoDB user profiles + CRM (Salesforce).
--
-- is_current = TRUE  → active / latest record
-- is_current = FALSE → historical snapshot

{{
  config(
    materialized     = 'incremental',
    unique_key       = 'customer_sk',
    incremental_strategy = 'merge',
    tags             = ['marts', 'customers', 'scd2']
  )
}}

WITH

source AS (
    SELECT * FROM {{ ref('stg_customers') }}
),

-- Detect changed records for SCD2 (hash comparison)
hashed AS (
    SELECT
        *,
        MD5(CONCAT_WS('||',
            COALESCE(email,           ''),
            COALESCE(first_name,      ''),
            COALESCE(last_name,       ''),
            COALESCE(country_code,    ''),
            COALESCE(city,            ''),
            COALESCE(customer_segment,''),
            COALESCE(loyalty_tier,    ''),
            COALESCE(phone_hash,      '')
        )) AS row_hash
    FROM source
),

-- SCD2 logic: close existing records on change, open new ones
scd2 AS (
    SELECT
        {{ dbt_utils.generate_surrogate_key(['customer_id', 'row_hash']) }}
                                                    AS customer_sk,
        customer_id,
        email,
        first_name,
        last_name,
        country_code,
        city,
        customer_segment,                           -- 'high_value' | 'mid' | 'low'
        loyalty_tier,                               -- 'gold' | 'silver' | 'bronze'
        phone_hash,                                 -- SHA-256 of phone (GDPR)
        acquisition_channel,
        acquisition_date,
        row_hash,
        -- SCD2 effective dates
        created_at                                  AS valid_from,
        LEAD(created_at) OVER (
            PARTITION BY customer_id
            ORDER BY created_at
        )                                           AS valid_to,
        CASE
            WHEN LEAD(created_at) OVER (
                PARTITION BY customer_id ORDER BY created_at
            ) IS NULL THEN TRUE
            ELSE FALSE
        END                                         AS is_current,
        CURRENT_TIMESTAMP()                         AS _dbt_updated_at
    FROM hashed
)

SELECT * FROM scd2
