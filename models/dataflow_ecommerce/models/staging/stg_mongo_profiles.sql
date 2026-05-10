-- models/staging/stg_mongo_profiles.sql
-- Source : RAW_MONGODB.STG_MONGO_PROFILES (chargé par mongodb_extractor.py)
-- Nettoie et type les profils utilisateurs pour les marts

{{ config(materialized='view') }}

SELECT
    _id                                         AS profile_id,
    user_id,
    email                                       AS email_raw,
    first_name,
    last_name,
    UPPER(COALESCE(country_code, 'XX'))         AS country_code,
    city,
    customer_segment,
    loyalty_tier,
    acquisition_channel,
    acquisition_date::DATE                      AS acquisition_date,
    preferred_category,
    preferred_device,
    COALESCE(ltv_estimate_eur::FLOAT, 0)        AS ltv_estimate_eur,
    COALESCE(predicted_churn_score::FLOAT, 0.5) AS predicted_churn_score,
    COALESCE(profile_completeness_pct::INT, 0)  AS profile_completeness_pct,
    last_active_at::TIMESTAMP_NTZ               AS last_active_at,
    _extracted_at::TIMESTAMP_NTZ                AS extracted_at,
    _storage_mode

FROM DATAFLOW_DEV.RAW_MONGODB.STG_MONGO_PROFILES
WHERE user_id IS NOT NULL