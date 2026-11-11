SELECT
    CAST(date AS DATE) AS marketing_date,
    utm_source,
    campaign,
    landing_page_clicks,
    coupon_codes_used,
    cost
FROM raw.marketing