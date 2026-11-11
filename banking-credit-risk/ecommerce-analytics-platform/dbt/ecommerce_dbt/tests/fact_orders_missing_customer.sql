SELECT
    *

FROM {{ ref('fact_orders') }}

WHERE customer_id IS NULL