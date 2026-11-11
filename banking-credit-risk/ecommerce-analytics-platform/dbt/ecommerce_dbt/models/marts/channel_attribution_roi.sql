SELECT
    marketing_date,
    utm_source,

    SUM(landing_page_clicks)
        AS landing_page_clicks,

    SUM(cost)
        AS marketing_cost

FROM {{ ref('stg_marketing') }}

GROUP BY
    marketing_date,
    utm_source