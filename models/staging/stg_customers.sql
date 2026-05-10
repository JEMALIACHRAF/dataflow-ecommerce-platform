-- models/staging/stg_customers.sql
--
-- Staging: Customers
-- Source: Salesforce CRM synced hourly via Airbyte → Snowflake raw schema.
-- Deduplicates on customer_id (Salesforce Account ID is the natural key).
-- PII fields are pseudonymised here — raw email never leaves staging.

{{
  config(
    materialized = 'view',
    tags         = ['staging', 'customers', 'pii']
  )
}}

WITH

raw AS (
    SELECT * FROM {{ source('silver', 'crm_accounts') }}
),

deduped AS (
    -- Salesforce can emit duplicate records during bulk exports;
    -- keep the most recently updated version
    SELECT *,
        ROW_NUMBER() OVER (
            PARTITION BY account_id
            ORDER BY system_modstamp DESC NULLS LAST
        ) AS _rn
    FROM raw
    WHERE account_id IS NOT NULL
),

cleaned AS (
    SELECT
        account_id                                          AS customer_id,
        SHA2(LOWER(TRIM(email)), 256)                      AS email_hashed,
        -- Raw email retained only for internal operational joins; never in marts
        email                                              AS email_raw,

        -- Name
        TRIM(first_name)                                   AS first_name,
        TRIM(last_name)                                    AS last_name,

        -- Geography
        UPPER(COALESCE(billing_country_code, 'XX'))        AS country_code,
        billing_city                                       AS city,
        billing_postal_code                                AS postal_code,
        phone_country_code,

        -- Segmentation (Salesforce custom field)
        COALESCE(customer_segment__c, 'unknown')           AS customer_segment,
        COALESCE(acquisition_channel__c, 'unknown')        AS acquisition_channel,
        acquisition_date__c::DATE                          AS acquisition_date,

        -- Salesforce metadata
        account_id                                         AS salesforce_account_id,
        created_date::TIMESTAMP_NTZ                        AS crm_created_at,
        system_modstamp::TIMESTAMP_NTZ                     AS crm_updated_at

    FROM deduped
    WHERE _rn = 1
      AND is_deleted = FALSE
      AND LOWER(COALESCE(customer_segment__c, '')) != 'internal'  -- exclude staff accounts
)

SELECT * FROM cleaned
