SELECT
    'fact_orders' AS table_name,

    COUNT(*) AS total_rows,

    COUNT(DISTINCT order_id) AS distinct_orders,

    COUNT(*) - COUNT(customer_id)
        AS null_customer_id_count,

    COUNT(*) - COUNT(product_id)
        AS null_product_id_count

FROM {{ ref('fact_orders') }}

UNION ALL

SELECT
    'dim_customers' AS table_name,

    COUNT(*) AS total_rows,

    COUNT(DISTINCT customer_id) AS distinct_orders,

    COUNT(*) - COUNT(customer_id)
        AS null_customer_id_count,

    0 AS null_product_id_count

FROM {{ ref('dim_customers') }}