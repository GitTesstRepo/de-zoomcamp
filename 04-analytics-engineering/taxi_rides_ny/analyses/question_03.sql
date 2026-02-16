-- 12184
SELECT
  COUNT(*)
FROM {{ ref('fct_monthly_zone_revenue') }} AS mzr
