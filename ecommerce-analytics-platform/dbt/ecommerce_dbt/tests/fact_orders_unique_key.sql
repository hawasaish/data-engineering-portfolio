SELECT
    order_id,
    order_item_id,
    COUNT(*) AS record_count

FROM {{ ref('fact_orders') }}

GROUP BY
    order_id,
    order_item_id

HAVING COUNT(*) > 1