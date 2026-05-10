-- models/marts/fact_orders.sql
--
-- Snowflake Star Schema — Fact Table
-- Central fact table for all e-commerce order lines.
-- Joined to dim_customers, dim_products, dim_time.
--
-- Grain        : one row per order line item
-- Partitioned  : order_date  (Snowflake micro-partition clustering)
-- Clustered by : order_date, country_code  (query pruning for dashboards)
-- Freshness    : updated daily by 06:00 UTC  (SLA enforced in Airflow)

{{
  config(
    materialized   = 'incremental',
    unique_key     = 'order_line_id',
    incremental_strategy = 'merge',
    cluster_by     = ['order_date', 'country_code'],
    on_schema_change = 'append_new_columns',
    tags           = ['marts', 'finance', 'daily'],
    meta           = {
      'owner'      : 'data-engineering@dataflow.io',
      'description': 'Order line-level fact table — star schema core',
      'sla'        : '06:00 UTC'
    }
  )
}}

WITH

-- -------------------------------------------------------------------------
-- Source CTEs
-- -------------------------------------------------------------------------

stg_orders AS (
    SELECT * FROM {{ ref('stg_orders') }}
    {% if is_incremental() %}
    WHERE order_date >= DATEADD('day', -3, CURRENT_DATE())
    -- -3 days to handle late-arriving records and reprocessing windows
    {% endif %}
),

stg_order_lines AS (
    SELECT * FROM {{ ref('stg_order_lines') }}
    {% if is_incremental() %}
    WHERE order_date >= DATEADD('day', -3, CURRENT_DATE())
    {% endif %}
),

dim_customers AS (
    SELECT customer_sk, customer_id
    FROM {{ ref('dim_customers') }}
    WHERE is_current = TRUE   -- SCD Type 2 — active record only
),

dim_products AS (
    SELECT product_sk, product_id, product_name, category_l1, category_l2, brand
    FROM {{ ref('dim_products') }}
    WHERE is_current = TRUE
),

dim_time AS (
    SELECT time_sk, calendar_date
    FROM {{ ref('dim_time') }}
),

-- -------------------------------------------------------------------------
-- Session attribution (last-touch model)
-- -------------------------------------------------------------------------

session_attribution AS (
    SELECT
        session_id,
        referrer_channel,
        device_type,
        country_code,
        user_id,
        -- Enrich with UTM parameters stored in clickstream properties
        GET_PATH(PARSE_JSON(properties), 'utm_source')   :: VARCHAR AS utm_source,
        GET_PATH(PARSE_JSON(properties), 'utm_medium')   :: VARCHAR AS utm_medium,
        GET_PATH(PARSE_JSON(properties), 'utm_campaign') :: VARCHAR AS utm_campaign
    FROM {{ source('silver', 'clickstream') }}
    WHERE event_type = 'session_start'
    {% if is_incremental() %}
      AND event_date >= DATEADD('day', -3, CURRENT_DATE())
    {% endif %}
),

-- -------------------------------------------------------------------------
-- Join order lines → orders → dims → attribution
-- -------------------------------------------------------------------------

order_lines_enriched AS (
    SELECT
        -- Surrogate key
        {{ dbt_utils.generate_surrogate_key(
            ['ol.order_id', 'ol.line_item_id']
        ) }}                                            AS order_line_id,

        -- Order / line identifiers
        o.order_id,
        ol.line_item_id,
        ol.product_id,
        o.customer_id,
        o.session_id,
        o.order_date,

        -- Dimension foreign keys
        COALESCE(dc.customer_sk, -1)                   AS customer_sk,
        COALESCE(dp.product_sk,  -1)                   AS product_sk,
        COALESCE(dt.time_sk,     -1)                   AS time_sk,

        -- Product attributes (denormalised for query performance)
        dp.product_name,
        dp.category_l1,
        dp.category_l2,
        dp.brand,

        -- Order metrics
        ol.quantity,
        ol.unit_price_eur,
        (ol.quantity * ol.unit_price_eur)              AS gross_revenue_eur,
        ol.discount_eur,
        (ol.quantity * ol.unit_price_eur - ol.discount_eur)
                                                       AS net_revenue_eur,
        o.shipping_cost_eur,
        o.tax_eur,

        -- Order status & fulfilment
        o.order_status,
        o.payment_method,
        o.is_first_order,
        o.shipping_country_code                        AS country_code,

        -- Attribution
        COALESCE(sa.referrer_channel, 'unknown')       AS referrer_channel,
        COALESCE(sa.device_type,      'unknown')       AS device_type,
        sa.utm_source,
        sa.utm_medium,
        sa.utm_campaign,

        -- Audit
        o.created_at                                   AS order_created_at,
        CURRENT_TIMESTAMP()                            AS _dbt_updated_at,
        '{{ invocation_id }}'                          AS _dbt_invocation_id

    FROM stg_order_lines    ol
    INNER JOIN stg_orders   o   ON ol.order_id    = o.order_id
    LEFT  JOIN dim_customers dc  ON o.customer_id  = dc.customer_id
    LEFT  JOIN dim_products  dp  ON ol.product_id  = dp.product_id
    LEFT  JOIN dim_time      dt  ON o.order_date   = dt.calendar_date
    LEFT  JOIN session_attribution sa ON o.session_id = sa.session_id
)

SELECT * FROM order_lines_enriched
