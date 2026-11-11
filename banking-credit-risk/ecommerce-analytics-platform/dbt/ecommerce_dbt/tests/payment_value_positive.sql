SELECT *

FROM {{ ref('stg_order_payments') }}

WHERE payment_value < 0