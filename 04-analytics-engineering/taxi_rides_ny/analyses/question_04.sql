-- East Harlem North
SELECT
  mzr.pickup_zone,
  SUM(mzr.revenue_monthly_total_amount) AS total_revenue
FROM {{ ref('fct_monthly_zone_revenue') }} AS mzr
WHERE mzr.service_type = 'Green'
  AND mzr.revenue_month >= '2020-01-01'
  AND mzr.revenue_month <= '2020-12-31'
GROUP BY mzr.pickup_zone
ORDER BY total_revenue DESC
