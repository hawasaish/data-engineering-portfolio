SELECT
    f.customer_id

FROM {{ ref('fact_orders') }} f

LEFT JOIN {{ ref('dim_customers') }} c
    ON f.customer_id = c.customer_id

WHERE c.customer_id IS NULL