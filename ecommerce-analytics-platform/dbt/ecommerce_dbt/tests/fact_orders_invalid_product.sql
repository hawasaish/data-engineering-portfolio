SELECT
    f.product_id

FROM {{ ref('fact_orders') }} f

LEFT JOIN {{ ref('dim_products') }} p
    ON f.product_id = p.product_id

WHERE p.product_id IS NULL