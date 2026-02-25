# Module 1 Homework: Docker & SQL
https://github.com/DataTalksClub/data-engineering-zoomcamp/blob/main/cohorts/2026/01-docker-terraform/homework.md

How to run:

docker build -t taxi_ingest:v002 .

 docker run -it --rm   --network=pipeline_default   taxi_ingest:v002   --pg-user=root   --pg-pass=root   --pg-host=pgdatabase   --pg-port=5432   --pg-db=ny_taxi   --target-table=green_taxi_trips   --year=2025   --month=11


SQL Part:
-- Question 3. Counting short trips
-- 8007
SELECT 
    COUNT(*)
FROM public.green_taxi_trips AS gtp
WHERE gtp.lpep_pickup_datetime >= '2025-11-01'::TIMESTAMP
	AND gtp.lpep_pickup_datetime < '2025-12-01'::TIMESTAMP
	AND gtp.trip_distance <= 1::NUMERIC;


-- Question 4. Longest trip for each day
-- 2025-11-14
SELECT 
    lpep_pickup_datetime::date
FROM public.green_taxi_trips AS gtp.
WHERE gtp.trip_distance = (SELECT MAX(trip_distance) FROM public.green_taxi_trips WHERE trip_distance< 100::NUMERIC);


-- Question 5. Biggest pickup zone
-- East Harlem North
SELECT
	tzl."Zone",
	SUM(gtp.total_amount) AS sum_total_amount
FROM public.green_taxi_trips AS gtp
	INNER JOIN public.taxi_zone_lookup AS tzl ON gtp."PULocationID" = tzl."LocationID"
WHERE lpep_pickup_datetime::date = '2025-11-18'::date
GROUP BY tzl."Zone"
ORDER BY sum_total_amount DESC
LIMIT 1;


-- Question 6. Largest tip
-- Yorkville West
SELECT
	zdo."Zone",
	MAX(gtp.tip_amount) AS sum_tip_amount
FROM public.green_taxi_trips AS gtp
	INNER JOIN public.taxi_zone_lookup AS zpu ON gtp."PULocationID" = zpu."LocationID"
	INNER JOIN public.taxi_zone_lookup AS zdo ON gtp."DOLocationID" = zdo."LocationID"
WHERE lpep_pickup_datetime >= '2025-11-01'::date
	AND lpep_pickup_datetime < '2025-12-01'::date
	AND zpu."Zone" = 'East Harlem North'
GROUP BY zdo."Zone"
ORDER BY sum_tip_amount DESC
LIMIT 1;