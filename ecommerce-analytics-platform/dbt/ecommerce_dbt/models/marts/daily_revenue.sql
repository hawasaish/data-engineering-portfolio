SELECT
    CAST(
        order_purchase_timestamp AS DATE
    ) AS order_date,

    COUNT(
        DISTINCT order_id
    ) AS total_orders,

    SUM(price) AS product_revenue,

    SUM(freight_value) AS freight_revenue,

    SUM(price + freight_value)
        AS gross_revenue,

    AVG(price + freight_value)
        AS average_order_value

FROM {{ ref('fact_orders') }}

GROUP BY 1

ORDER BY 1