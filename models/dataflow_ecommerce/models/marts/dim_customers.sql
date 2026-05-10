-- models/marts/dim_customers.sql
{{ config(
    materialized='table',
    cluster_by=['country_code', 'customer_segment']
) }}

SELECT
    MD5(user_id)                                    AS customer_sk,
    user_id,
    first_name,
    last_name,
    country_code,
    city,
    customer_segment,
    loyalty_tier,
    acquisition_channel,
    acquisition_date,
    preferred_category,
    preferred_device,
    ltv_estimate_eur,
    predicted_churn_score,
    profile_completeness_pct,
    last_active_at,
    CASE
        WHEN predicted_churn_score >= 0.7 THEN 'high'
        WHEN predicted_churn_score >= 0.4 THEN 'medium'
        ELSE 'low'
    END                                             AS churn_risk,
    CASE
        WHEN ltv_estimate_eur >= 1000 THEN TRUE
        ELSE FALSE
    END                                             AS is_high_value,
    DATEDIFF('day', last_active_at, CURRENT_DATE()) AS days_since_last_active,
    extracted_at,
    CURRENT_TIMESTAMP()                             AS dbt_updated_at
FROM {{ ref('stg_mongo_profiles') }}