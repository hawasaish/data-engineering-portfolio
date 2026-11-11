WITH source_counts AS (

    SELECT
        CAST(order_purchase_timestamp AS TIMESTAMP)::DATE AS order_date,
        COUNT(*) AS source_row_count
    FROM raw.orders
    GROUP BY 1

),

target_counts AS (

    SELECT
        order_date,
        COUNT(*) AS target_row_count
    FROM {{ ref('fct_orders') }}
    GROUP BY 1

)

SELECT
    COALESCE(s.order_date, t.order_date) AS order_date,
    COALESCE(s.source_row_count, 0) AS source_row_count,
    COALESCE(t.target_row_count, 0) AS target_row_count
FROM source_counts s
FULL OUTER JOIN target_counts t
    ON s.order_date = t.order_date
WHERE COALESCE(s.source_row_count, 0)
    != COALESCE(t.target_row_count, 0)