-- 43244693
SELECT
    COUNT(*) 
FROM {{ ref("stg_fhv_tripdata") }}
