## Homework: Build Your Own dlt Pipeline
https://github.com/DataTalksClub/data-engineering-zoomcamp/blob/main/cohorts/2026/workshops/dlt/dlt_homework.md

docker build -t dlt-temp .
docker run -it --rm dlt-temp




### Quiz Questions
Question 1: What is the start date and end date of the dataset?
- 2009-01-01 to 2009-01-31
- 2009-06-01 to 2009-07-01
- 2024-01-01 to 2024-02-01
- 2024-06-01 to 2024-07-01
'''
-- 2009-06-01 2009-07-01
SELECT
  MIN(trip_pickup_date_time)::date AS min_date,
  MAX(trip_dropoff_date_time)::date AS end_date,
FROM "nyc_taxi_trips"
LIMIT 1000;
'''

Question 2: What proportion of trips are paid with credit card?
- 16.66%
- +++ 26.66%
- 36.66%
- 46.66%
'''
-- 26.66
SELECT
  UPPER(payment_type) AS payment_type,
  ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (), 2) AS percent_of_total
FROM "nyc_taxi_trips"
GROUP BY UPPER(payment_type)
ORDER BY payment_type
LIMIT 1000;
'''

Question 3: What is the total amount of money generated in tips?
- $4,063.41
- +++$6,063.41
- $8,063.41
- $10,063.41
'''
-- 6,063.41
SELECT
  ROUND(SUM(tip_amt), 2) total_tips
FROM "nyc_taxi_trips"
LIMIT 1000;
'''
