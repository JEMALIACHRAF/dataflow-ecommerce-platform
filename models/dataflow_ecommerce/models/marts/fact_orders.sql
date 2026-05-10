-- models/marts/fact_orders.sql
{{ config(
    materialized='table',
    cluster_by=['order_date', 'country_code']
) }}

WITH customers AS (
    SELECT customer_sk, user_id, country_code,
           customer_segment, acquisition_channel
    FROM {{ ref('dim_customers') }}
),

-- Simule des commandes réalistes basées sur les vrais customers
raw_orders AS (
    SELECT
        MD5(c.user_id || '-' || seq.n::VARCHAR)     AS order_id,
        c.customer_sk,
        c.user_id,
        c.country_code,
        c.customer_segment,
        c.acquisition_channel,

        -- Date de commande aléatoire entre 2022 et 2024
        DATEADD('day',
            ABS(MOD(HASH(c.user_id || seq.n::VARCHAR), 730)),
            '2022-01-01'::DATE
        )                                            AS order_date,

        -- Revenue basé sur le segment
        ROUND(
            CASE c.customer_segment
                WHEN 'VIP'    THEN 150 + ABS(MOD(HASH(c.user_id || 'r'), 500))
                WHEN 'Loyal'  THEN 80  + ABS(MOD(HASH(c.user_id || 'r'), 200))
                WHEN 'New'    THEN 30  + ABS(MOD(HASH(c.user_id || 'r'), 100))
                ELSE               20  + ABS(MOD(HASH(c.user_id || 'r'), 80))
            END, 2
        )                                            AS gross_revenue_eur,

        -- Discount aléatoire 0-15%
        ROUND(ABS(MOD(HASH(c.user_id || 'd'), 15)) / 100.0, 2) AS discount_rate,

        -- Payment method
        CASE ABS(MOD(HASH(c.user_id || 'p'), 4))
            WHEN 0 THEN 'card'
            WHEN 1 THEN 'paypal'
            WHEN 2 THEN 'bank_transfer'
            ELSE        'apple_pay'
        END                                          AS payment_method,

        -- First order flag
        CASE WHEN seq.n = 1 THEN TRUE ELSE FALSE END AS is_first_order,

        ROW_NUMBER() OVER (
            PARTITION BY c.user_id ORDER BY seq.n
        )                                            AS order_rank

    FROM customers c
    -- Chaque customer passe entre 1 et 5 commandes
    CROSS JOIN (
        SELECT ROW_NUMBER() OVER (ORDER BY SEQ4()) AS n
        FROM TABLE(GENERATOR(ROWCOUNT => 5))
    ) seq
    WHERE ABS(MOD(HASH(c.user_id || seq.n::VARCHAR), 10)) < 
          CASE c.customer_segment
              WHEN 'VIP'   THEN 9
              WHEN 'Loyal' THEN 7
              WHEN 'New'   THEN 4
              ELSE              3
          END
),

final AS (
    SELECT
        MD5(order_id)                               AS order_sk,
        order_id,
        customer_sk,
        TO_NUMBER(TO_CHAR(order_date, 'YYYYMMDD'))  AS time_sk,
        order_date,
        country_code,
        customer_segment,
        acquisition_channel,
        payment_method,
        gross_revenue_eur,
        ROUND(gross_revenue_eur * discount_rate, 2) AS discount_eur,
        ROUND(gross_revenue_eur * (1 - discount_rate), 2) AS net_revenue_eur,
        ROUND(gross_revenue_eur * 0.20, 2)          AS tax_eur,
        is_first_order,
        order_rank,
        CURRENT_TIMESTAMP()                         AS dbt_updated_at
    FROM raw_orders
)

SELECT * FROM final