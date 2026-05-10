-- models/staging/stg_orders.sql
--
-- Staging: Orders
-- Cleans and types raw orders from Silver layer (PostgreSQL sources ×3).
-- Handles schema drift across 3 independent transactional databases
-- (FR, DE, ES storefronts) unified here before mart joins.

{{
  config(
    materialized = 'view',
    tags         = ['staging', 'orders']
  )
}}

WITH

raw AS (
    SELECT * FROM {{ source('silver', 'orders') }}
),

cleaned AS (
    SELECT
        -- Identifiers
        order_id                                              AS order_id,
        COALESCE(customer_id, anonymous_customer_id)          AS customer_id,
        session_id,

        -- Dates
        order_date::DATE                                      AS order_date,
        confirmed_at::TIMESTAMP_NTZ                          AS confirmed_at,
        shipped_at::TIMESTAMP_NTZ                            AS shipped_at,
        delivered_at::TIMESTAMP_NTZ                          AS delivered_at,
        cancelled_at::TIMESTAMP_NTZ                          AS cancelled_at,

        -- Status normalisation (3 storefronts use different status strings)
        CASE
            WHEN LOWER(order_status) IN ('paid', 'confirmed', 'validé')
                THEN 'confirmed'
            WHEN LOWER(order_status) IN ('shipped', 'expédié', 'versandt')
                THEN 'shipped'
            WHEN LOWER(order_status) IN ('delivered', 'livré', 'geliefert')
                THEN 'delivered'
            WHEN LOWER(order_status) IN ('cancelled', 'annulé', 'storniert')
                THEN 'cancelled'
            WHEN LOWER(order_status) IN ('refunded', 'remboursé', 'zurückerstattet')
                THEN 'refunded'
            ELSE 'pending'
        END                                                   AS order_status,

        -- Financials (already normalised to EUR in Silver)
        COALESCE(revenue_eur, 0)::FLOAT                      AS revenue_eur,
        COALESCE(shipping_cost_eur, 0)::FLOAT                AS shipping_cost_eur,
        COALESCE(discount_eur, 0)::FLOAT                     AS discount_eur,
        COALESCE(tax_eur, 0)::FLOAT                          AS tax_eur,
        (COALESCE(revenue_eur, 0)
            + COALESCE(shipping_cost_eur, 0)
            - COALESCE(discount_eur, 0))                     AS total_charged_eur,

        -- Payment
        LOWER(COALESCE(payment_method, 'unknown'))            AS payment_method,
        payment_provider,
        currency_code,

        -- Geography
        UPPER(COALESCE(shipping_country_code, 'XX'))         AS shipping_country_code,
        shipping_city,

        -- Customer flags
        is_first_order::BOOLEAN                              AS is_first_order,

        -- Source tracing
        source_system,   -- 'storefront_fr' | 'storefront_de' | 'storefront_es'
        _silver_processed_at

    FROM raw
    WHERE
        order_id IS NOT NULL
        AND order_date IS NOT NULL
        AND order_date >= '2020-01-01'
        -- Exclude test orders injected by QA team
        AND LOWER(COALESCE(payment_method, '')) != 'test'
        AND COALESCE(revenue_eur, 0) >= 0
)

SELECT * FROM cleaned
