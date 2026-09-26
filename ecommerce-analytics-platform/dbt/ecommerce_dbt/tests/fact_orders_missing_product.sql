SELECT
    *

FROM {{ ref('fact_orders') }}

WHERE product_id IS NULL