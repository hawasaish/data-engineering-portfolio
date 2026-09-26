SELECT
    customer_id,

    COUNT(
        DISTINCT order_id
    ) AS total_orders,

    SUM(
        price + freight_value
    ) AS lifetime_value,

    AVG(
        price + freight_value
    ) AS average_order_value,

    MIN(
        order_purchase_timestamp
    ) AS first_order_date,

    MAX(
        order_purchase_timestamp
    ) AS last_order_date

FROM {{ ref('fact_orders') }}

GROUP BY customer_id