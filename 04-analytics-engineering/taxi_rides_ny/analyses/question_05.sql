-- 384624
SELECT 
  SUM(mzr.total_monthly_trips) AS total_revenue
FROM {{ ref('fct_monthly_zone_revenue') }} AS mzr
WHERE mzr.service_type = 'Green'
  AND mzr.revenue_month >= '2019-10-01'
  AND mzr.revenue_month <= '2019-10-31'
