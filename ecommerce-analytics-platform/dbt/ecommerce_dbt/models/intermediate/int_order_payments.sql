SELECT
    order_id,

    SUM(payment_value)
        AS total_payment_value,

    MAX(payment_installments)
        AS max_payment_installments

FROM {{ ref('stg_order_payments') }}

GROUP BY order_id