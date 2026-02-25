## Module 4 Homework: Analytics Engineering with dbt
https://github.com/DataTalksClub/data-engineering-zoomcamp/blob/main/cohorts/2026/04-analytics-engineering/homework.md

Upload .csv files:
```
docker build -t gcp_fhv_taxi_upload .
docker run -v ./gcs.json:/code/gcs.json --rm gcp_fhv_taxi_upload
```

```
Bucket 'terra-77564-demo-bucket' exists and belongs to your project. Proceeding...
Downloading https://github.com/DataTalksClub/nyc-tlc-data/releases/download/fhv/fhv_tripdata_2019-01.csv.gz...
Downloading https://github.com/DataTalksClub/nyc-tlc-data/releases/download/fhv/fhv_tripdata_2019-02.csv.gz...
Downloading https://github.com/DataTalksClub/nyc-tlc-data/releases/download/fhv/fhv_tripdata_2019-03.csv.gz...
Downloading https://github.com/DataTalksClub/nyc-tlc-data/releases/download/fhv/fhv_tripdata_2019-04.csv.gz...
Downloaded: ./fhv_tripdata_2019-02.csv.gz
Downloading https://github.com/DataTalksClub/nyc-tlc-data/releases/download/fhv/fhv_tripdata_2019-05.csv.gz...
Downloaded: ./fhv_tripdata_2019-04.csv.gz
Downloading https://github.com/DataTalksClub/nyc-tlc-data/releases/download/fhv/fhv_tripdata_2019-06.csv.gz...
Downloaded: ./fhv_tripdata_2019-03.csv.gz
Downloading https://github.com/DataTalksClub/nyc-tlc-data/releases/download/fhv/fhv_tripdata_2019-07.csv.gz...
Downloaded: ./fhv_tripdata_2019-06.csv.gz
Downloading https://github.com/DataTalksClub/nyc-tlc-data/releases/download/fhv/fhv_tripdata_2019-08.csv.gz...
Downloaded: ./fhv_tripdata_2019-05.csv.gz
Downloading https://github.com/DataTalksClub/nyc-tlc-data/releases/download/fhv/fhv_tripdata_2019-09.csv.gz...
Downloaded: ./fhv_tripdata_2019-07.csv.gz
Downloading https://github.com/DataTalksClub/nyc-tlc-data/releases/download/fhv/fhv_tripdata_2019-10.csv.gz...
Downloaded: ./fhv_tripdata_2019-09.csv.gz
Downloading https://github.com/DataTalksClub/nyc-tlc-data/releases/download/fhv/fhv_tripdata_2019-11.csv.gz...
Downloaded: ./fhv_tripdata_2019-08.csv.gz
Downloading https://github.com/DataTalksClub/nyc-tlc-data/releases/download/fhv/fhv_tripdata_2019-12.csv.gz...
Downloaded: ./fhv_tripdata_2019-01.csv.gz
Downloaded: ./fhv_tripdata_2019-10.csv.gz
Downloaded: ./fhv_tripdata_2019-11.csv.gz
Downloaded: ./fhv_tripdata_2019-12.csv.gz
Bucket 'terra-77564-demo-bucket' exists and belongs to your project. Proceeding...
Uploading ./fhv_tripdata_2019-03.csv.gz to terra-77564-demo-bucket (Attempt 1)...
Bucket 'terra-77564-demo-bucket' exists and belongs to your project. Proceeding...
Uploading ./fhv_tripdata_2019-01.csv.gz to terra-77564-demo-bucket (Attempt 1)...
Bucket 'terra-77564-demo-bucket' exists and belongs to your project. Proceeding...
Uploading ./fhv_tripdata_2019-04.csv.gz to terra-77564-demo-bucket (Attempt 1)...
Bucket 'terra-77564-demo-bucket' exists and belongs to your project. Proceeding...
Uploading ./fhv_tripdata_2019-02.csv.gz to terra-77564-demo-bucket (Attempt 1)...
Uploaded: gs://terra-77564-demo-bucket/fhv_tripdata_2019-03.csv.gz
Verification successful for fhv_tripdata_2019-03.csv.gz
Uploaded: gs://terra-77564-demo-bucket/fhv_tripdata_2019-02.csv.gz
Verification successful for fhv_tripdata_2019-02.csv.gz
Bucket 'terra-77564-demo-bucket' exists and belongs to your project. Proceeding...
Uploading ./fhv_tripdata_2019-06.csv.gz to terra-77564-demo-bucket (Attempt 1)...
Bucket 'terra-77564-demo-bucket' exists and belongs to your project. Proceeding...
Uploading ./fhv_tripdata_2019-05.csv.gz to terra-77564-demo-bucket (Attempt 1)...
Uploaded: gs://terra-77564-demo-bucket/fhv_tripdata_2019-04.csv.gz
Verification successful for fhv_tripdata_2019-04.csv.gz
Bucket 'terra-77564-demo-bucket' exists and belongs to your project. Proceeding...
Uploading ./fhv_tripdata_2019-07.csv.gz to terra-77564-demo-bucket (Attempt 1)...
Uploaded: gs://terra-77564-demo-bucket/fhv_tripdata_2019-05.csv.gz
Uploaded: gs://terra-77564-demo-bucket/fhv_tripdata_2019-06.csv.gz
Verification successful for fhv_tripdata_2019-05.csv.gz
Verification successful for fhv_tripdata_2019-06.csv.gz
Bucket 'terra-77564-demo-bucket' exists and belongs to your project. Proceeding...
Uploading ./fhv_tripdata_2019-08.csv.gz to terra-77564-demo-bucket (Attempt 1)...
Bucket 'terra-77564-demo-bucket' exists and belongs to your project. Proceeding...
Uploading ./fhv_tripdata_2019-09.csv.gz to terra-77564-demo-bucket (Attempt 1)...
Uploaded: gs://terra-77564-demo-bucket/fhv_tripdata_2019-07.csv.gz
Verification successful for fhv_tripdata_2019-07.csv.gz
Bucket 'terra-77564-demo-bucket' exists and belongs to your project. Proceeding...
Uploading ./fhv_tripdata_2019-10.csv.gz to terra-77564-demo-bucket (Attempt 1)...
Uploaded: gs://terra-77564-demo-bucket/fhv_tripdata_2019-09.csv.gz
Verification successful for fhv_tripdata_2019-09.csv.gz
Uploaded: gs://terra-77564-demo-bucket/fhv_tripdata_2019-08.csv.gz
Verification successful for fhv_tripdata_2019-08.csv.gz
Bucket 'terra-77564-demo-bucket' exists and belongs to your project. Proceeding...
Uploading ./fhv_tripdata_2019-11.csv.gz to terra-77564-demo-bucket (Attempt 1)...
Bucket 'terra-77564-demo-bucket' exists and belongs to your project. Proceeding...
Uploading ./fhv_tripdata_2019-12.csv.gz to terra-77564-demo-bucket (Attempt 1)...
Uploaded: gs://terra-77564-demo-bucket/fhv_tripdata_2019-10.csv.gz
Verification successful for fhv_tripdata_2019-10.csv.gz
Uploaded: gs://terra-77564-demo-bucket/fhv_tripdata_2019-11.csv.gz
Verification successful for fhv_tripdata_2019-11.csv.gz
Uploaded: gs://terra-77564-demo-bucket/fhv_tripdata_2019-12.csv.gz
Verification successful for fhv_tripdata_2019-12.csv.gz
Uploaded: gs://terra-77564-demo-bucket/fhv_tripdata_2019-01.csv.gz
Verification successful for fhv_tripdata_2019-01.csv.gz
All files processed and verified.
```

```
-- Create an external table
CREATE OR REPLACE EXTERNAL TABLE `terra-77564.nytaxi.external_fhv_tripdata_2019_csv`
OPTIONS (
  FORMAT = 'CSV',
  uris = ['gs://terra-77564-demo-bucket/fhv_tripdata_2019-*.csv.gz'],
  compression = 'GZIP'
);
```

```
-- Create an optimized materialized table (partitioned and clustered)
CREATE OR REPLACE TABLE `terra-77564.nytaxi.fhv_tripdata_2019` 
PARTITION BY (DATE(dropOff_datetime))
AS 
SELECT * FROM `terra-77564.nytaxi.external_fhv_tripdata_2019_csv`;
```


### Quiz Questions
Question 1. dbt Lineage and Execution
Given a dbt project with the following structure:
```
models/
├── staging/
│   ├── stg_green_tripdata.sql
│   └── stg_yellow_tripdata.sql
└── intermediate/
    └── int_trips_unioned.sql (depends on stg_green_tripdata & stg_yellow_tripdata)
```
If you run dbt run --select int_trips_unioned, what models will be built?

- stg_green_tripdata, stg_yellow_tripdata, and int_trips_unioned (upstream dependencies) dbt run --select +int_trips_unioned
- Any model with upstream and downstream dependencies to int_trips_unioned  dbt run --select +int_trips_unioned+
- +++ int_trips_unioned only
- int_trips_unioned, int_trips, and fct_trips (downstream dependencies) dbt run --select +int_trips_unioned+


Question 2. dbt Tests
You've configured a generic test like this in your schema.yml:
```
columns:
  - name: payment_type
    data_tests:
      - accepted_values:
          arguments:
            values: [1, 2, 3, 4, 5]
            quote: false
```

Your model fct_trips has been running successfully for months. A new value 6 now appears in the source data.

What happens when you run dbt test --select fct_trips?

- dbt will skip the test because the model didn't change
- +++ dbt will fail the test, returning a non-zero exit code
- dbt will pass the test with a warning about the new value
- dbt will update the configuration to include the new value


```
dbt test --select dim_vendors
18:11:40
1 of 1 START test accepted_values_dim_vendors_vendor_id__False__1__2 ........... [RUN]
18:11:42
1 of 1 FAIL 1 accepted_values_dim_vendors_vendor_id__False__1__2 ............... [FAIL 1 in 1.79s]
18:11:42
Failure in test accepted_values_dim_vendors_vendor_id__False__1__2 (models/marts/schema.yml)
```

Question 3. Counting Records in fct_monthly_zone_revenue
After running your dbt project, query the fct_monthly_zone_revenue model.

What is the count of records in the fct_monthly_zone_revenue model?

- 12,998
- 14,120
- +++ 12,184
- 15,421

```
-- 12184
SELECT
  COUNT(*)
FROM {{ ref('fct_monthly_zone_revenue') }} AS mzr
```


Question 4. Best Performing Zone for Green Taxis (2020)
Using the fct_monthly_zone_revenue table, find the pickup zone with the highest total revenue (revenue_monthly_total_amount) for Green taxi trips in 2020.

Which zone had the highest revenue?

- +++ East Harlem North
- Morningside Heights
- East Harlem South
- Washington Heights South

```
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
```


Question 5. Green Taxi Trip Counts (October 2019)
Using the fct_monthly_zone_revenue table, what is the total number of trips (total_monthly_trips) for Green taxis in October 2019?

- 500,234
- 350,891
- +++ 384,624
- 421,509

```
-- 384624
SELECT 
  SUM(mzr.total_monthly_trips) AS total_revenue
FROM {{ ref('fct_monthly_zone_revenue') }} AS mzr
WHERE mzr.service_type = 'Green'
  AND mzr.revenue_month >= '2019-10-01'
  AND mzr.revenue_month <= '2019-10-31'
```


Question 6. Build a Staging Model for FHV Data
Create a staging model for the For-Hire Vehicle (FHV) trip data for 2019.

Load the FHV trip data for 2019 into your data warehouse
Create a staging model stg_fhv_tripdata with these requirements:
Filter out records where dispatching_base_num IS NULL
Rename fields to match your project's naming conventions (e.g., PUlocationID → pickup_location_id)
What is the count of records in stg_fhv_tripdata?

- 42,084,899
- +++ 43,244,693
- 22,998,722
- 44,112,187

```
-- 43244693
SELECT
    COUNT(*) 
FROM {{ ref("stg_fhv_tripdata") }}
```
