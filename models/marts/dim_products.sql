-- models/marts/dim_products.sql
--
-- Snowflake Star Schema — Product Dimension (SCD Type 2)
-- Tracks full history of product attribute changes (price, category, status).
--
-- Grain    : one row per product per version
-- SCD Type : 2
-- Source   : stg_products (internal PIM system via REST API connector)

{{
  config(
    materialized   = 'table',
    unique_key     = 'product_sk',
    cluster_by     = ['category_l1', 'brand'],
    tags           = ['marts', 'dimensions', 'daily'],
    meta           = {
      'owner'      : 'data-engineering@dataflow.io',
      'description': 'Product dimension — SCD Type 2, sourced from PIM'
    }
  )
}}

WITH

stg AS (
    SELECT * FROM {{ ref('stg_products') }}
    WHERE product_id IS NOT NULL
),

enriched AS (
    SELECT
        product_id,
        product_sku,
        product_name,
        product_description,
        category_l1,
        category_l2,
        category_l3,
        brand,
        supplier_id,

        -- Pricing
        base_price_eur,
        cost_price_eur,
        ROUND(
            (base_price_eur - cost_price_eur) / NULLIF(base_price_eur, 0),
            4
        )                                               AS gross_margin_rate,

        -- Product attributes
        weight_kg,
        is_digital,
        is_active,
        stock_status,           -- in_stock / low_stock / out_of_stock
        launch_date,

        -- Content completeness (used by ML feature store)
        CASE
            WHEN product_name        IS NOT NULL
             AND product_description IS NOT NULL
             AND category_l1         IS NOT NULL
             AND brand               IS NOT NULL
            THEN TRUE ELSE FALSE
        END                                             AS is_catalog_complete,

        pim_updated_at                                  AS source_updated_at
    FROM stg
),

scd2 AS (
    SELECT
        {{ dbt_utils.generate_surrogate_key(['product_id', 'source_updated_at']) }}
                                                        AS product_sk,
        product_id,
        product_sku,
        product_name,
        product_description,
        category_l1,
        category_l2,
        category_l3,
        brand,
        supplier_id,
        base_price_eur,
        cost_price_eur,
        gross_margin_rate,
        weight_kg,
        is_digital,
        is_active,
        stock_status,
        launch_date,
        is_catalog_complete,
        source_updated_at                               AS valid_from,
        LEAD(source_updated_at) OVER (
            PARTITION BY product_id ORDER BY source_updated_at
        )                                               AS valid_to,
        CASE
            WHEN LEAD(source_updated_at) OVER (
                     PARTITION BY product_id ORDER BY source_updated_at
                 ) IS NULL
            THEN TRUE ELSE FALSE
        END                                             AS is_current,
        CURRENT_TIMESTAMP()                             AS _dbt_updated_at,
        '{{ invocation_id }}'                           AS _dbt_invocation_id
    FROM enriched
)

SELECT * FROM scd2
