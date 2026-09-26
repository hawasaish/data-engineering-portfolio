SELECT
    oi.order_id,
    oi.order_item_id,

    oi.customer_id,
    oi.product_id,
    oi.seller_id,

    oi.order_purchase_timestamp,

    oi.price,
    oi.freight_value,

    COALESCE(
        p.total_payment_value,
        0
    ) AS total_payment_value,

    oi.product_category_name

FROM {{ ref('int_order_items') }} oi

LEFT JOIN {{ ref('int_order_payments') }} p
    ON oi.order_id = p.order_id