SELECT
    oi.order_id,
    oi.order_item_id,
    oi.product_id,
    oi.seller_id,
    oi.price,
    oi.freight_value,

    o.customer_id,
    o.order_purchase_timestamp,

    p.product_category_name

FROM {{ ref('stg_order_items') }} oi

LEFT JOIN {{ ref('stg_orders') }} o
    ON oi.order_id = o.order_id

LEFT JOIN {{ ref('stg_products') }} p
    ON oi.product_id = p.product_id