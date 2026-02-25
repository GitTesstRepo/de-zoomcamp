## Module 3 Homework: Data Warehousing & BigQuery
https://github.com/DataTalksClub/data-engineering-zoomcamp/blob/main/cohorts/2026/03-data-warehouse/homework.md

Upload .parquet files:
```
docker build -t gcp_yellow_taxi_upload .
docker run -v ./gcs.json:/code/gcs.json --rm gcp_yellow_taxi_upload
```

Logs:
```
Bucket 'terra-77564-demo-bucket' exists and belongs to your project. Proceeding...
Downloading https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2024-01.parquet...
Downloading https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2024-02.parquet...
Downloading https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2024-03.parquet...
Downloading https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2024-04.parquet...
Downloaded: ./yellow_tripdata_2024-02.parquet
Downloading https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2024-05.parquet...
Downloaded: ./yellow_tripdata_2024-04.parquet
Downloading https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2024-06.parquet...
Downloaded: ./yellow_tripdata_2024-01.parquet
Downloaded: ./yellow_tripdata_2024-05.parquet
Downloaded: ./yellow_tripdata_2024-06.parquet
Downloaded: ./yellow_tripdata_2024-03.parquet
Bucket 'terra-77564-demo-bucket' exists and belongs to your project. Proceeding...
Uploading ./yellow_tripdata_2024-03.parquet to terra-77564-demo-bucket (Attempt 1)...
Bucket 'terra-77564-demo-bucket' exists and belongs to your project. Proceeding...
Uploading ./yellow_tripdata_2024-01.parquet to terra-77564-demo-bucket (Attempt 1)...
Bucket 'terra-77564-demo-bucket' exists and belongs to your project. Proceeding...
Uploading ./yellow_tripdata_2024-04.parquet to terra-77564-demo-bucket (Attempt 1)...
Bucket 'terra-77564-demo-bucket' exists and belongs to your project. Proceeding...
Uploading ./yellow_tripdata_2024-02.parquet to terra-77564-demo-bucket (Attempt 1)...
Uploaded: gs://terra-77564-demo-bucket/yellow_tripdata_2024-01.parquet
Verification successful for yellow_tripdata_2024-01.parquet
Bucket 'terra-77564-demo-bucket' exists and belongs to your project. Proceeding...
Uploading ./yellow_tripdata_2024-05.parquet to terra-77564-demo-bucket (Attempt 1)...
Uploaded: gs://terra-77564-demo-bucket/yellow_tripdata_2024-03.parquet
Verification successful for yellow_tripdata_2024-03.parquet
Uploaded: gs://terra-77564-demo-bucket/yellow_tripdata_2024-02.parquet
Verification successful for yellow_tripdata_2024-02.parquet
Uploaded: gs://terra-77564-demo-bucket/yellow_tripdata_2024-04.parquet
Verification successful for yellow_tripdata_2024-04.parquet
Bucket 'terra-77564-demo-bucket' exists and belongs to your project. Proceeding...
Uploading ./yellow_tripdata_2024-06.parquet to terra-77564-demo-bucket (Attempt 1)...
Uploaded: gs://terra-77564-demo-bucket/yellow_tripdata_2024-05.parquet
Verification successful for yellow_tripdata_2024-05.parquet
Uploaded: gs://terra-77564-demo-bucket/yellow_tripdata_2024-06.parquet
Verification successful for yellow_tripdata_2024-06.parquet
All files processed and verified.
```

```
-- Create an external table
CREATE OR REPLACE EXTERNAL TABLE `terra-77564.kestra_dataset.external_yellow_tripdata_parquet`
OPTIONS (
  FORMAT = 'PARQUET',
  uris = ['gs://terra-77564-demo-bucket/yellow_tripdata_2024-*.parquet']
);

-- Create a materialized table
CREATE OR REPLACE TABLE `terra-77564.kestra_dataset.yellow_tripdata_parquet` AS 
SELECT * FROM `terra-77564.kestra_dataset.external_yellow_tripdata_parquet`;

-- Create an optimized materialized table (partitioned and clustered)
CREATE OR REPLACE TABLE `terra-77564.kestra_dataset.yellow_tripdata_parquet_optimized` 
PARTITION BY (DATE(tpep_dropoff_datetime))
CLUSTER BY VendorID
AS 
SELECT * FROM `terra-77564.kestra_dataset.external_yellow_tripdata_parquet`;
```

### Quiz Questions

Question 1. Counting records
What is count of records for the 2024 Yellow Taxi Data?

- 65,623
- 840,402
- ++++ 20,332,093
- 85,431,289

Comment:
```
-- 20 332 093
SELECT COUNT(*) FROM `terra-77564.kestra_dataset.external_yellow_tripdata_parquet`;
SELECT COUNT(*) FROM `terra-77564.kestra_dataset.yellow_tripdata_parquet`;
```


Question 2. Data read estimation
Write a query to count the distinct number of PULocationIDs for the entire dataset on both the tables.

What is the estimated amount of data that will be read when this query is executed on the External Table and the Table?

- 18.82 MB for the External Table and 47.60 MB for the Materialized Table
- ++++ 0 MB for the External Table and 155.12 MB for the Materialized Table
- 2.14 GB for the External Table and 0MB for the Materialized Table
- 0 MB for the External Table and 0MB for the Materialized Table

Comment:
```
-- This script will process 0 MB when run.
SELECT 
  COUNT(DISTINCT VendorID)
FROM `terra-77564.kestra_dataset.external_yellow_tripdata_parquet`;

-- This script will process 155.12 MB when run.
SELECT 
  COUNT(DISTINCT VendorID)
FROM `terra-77564.kestra_dataset.yellow_tripdata_parquet`;
```


Question 3. Understanding columnar storage
Write a query to retrieve the PULocationID from the table (not the external table) in BigQuery. Now write a query to retrieve the PULocationID and DOLocationID on the same table.

Why are the estimated number of Bytes different?

- ++++ BigQuery is a columnar database, and it only scans the specific columns requested in the query. Querying two columns (PULocationID, DOLocationID) requires reading more data than querying one column (PULocationID), leading to a higher estimated number of bytes processed.
BigQuery duplicates data across multiple storage partitions, so selecting two columns instead of one requires scanning the table twice, doubling the estimated bytes processed.
- BigQuery automatically caches the first queried column, so adding a second column increases processing time but does not affect the estimated bytes scanned.
- When selecting multiple columns, BigQuery performs an implicit join operation between them, increasing the estimated bytes processed

Comment:
```
-- This script will process 155.12 MB when run.
SELECT 
	PULocationID
FROM `terra-77564.kestra_dataset.yellow_tripdata_parquet`;

-- This query will process 310.24 MB when run.
SELECT 
	PULocationID, 
	DOLocationID 
FROM `terra-77564.kestra_dataset.yellow_tripdata_parquet`;
```


Question 4. Counting zero fare trips
How many records have a fare_amount of 0?

- 128,210
- 546,578
- 20,188,016
- ++++ 8,333

Comment:
```
-- 8333
SELECT 
  COUNT(fare_amount)
FROM `terra-77564.kestra_dataset.yellow_tripdata_parquet`
WHERE fare_amount = 0.0;
```


Question 5. Partitioning and clustering
What is the best strategy to make an optimized table in Big Query if your query will always filter based on tpep_dropoff_datetime and order the results by VendorID (Create a new table with this strategy)

- ++++ Partition by tpep_dropoff_datetime and Cluster on VendorID
- Cluster on by tpep_dropoff_datetime and Cluster on VendorID
- Cluster on tpep_dropoff_datetime Partition by VendorID
- Partition by tpep_dropoff_datetime and Partition by VendorID


Question 6. Partition benefits
Write a query to retrieve the distinct VendorIDs between tpep_dropoff_datetime 2024-03-01 and 2024-03-15 (inclusive)

Use the materialized table you created earlier in your from clause and note the estimated bytes. Now change the table in the from clause to the partitioned table you created for question 5 and note the estimated bytes processed. What are these values?

Choose the answer which most closely matches.

- 12.47 MB for non-partitioned table and 326.42 MB for the partitioned table
-  ++++ 310.24 MB for non-partitioned table and 26.84 MB for the partitioned table
- 5.87 MB for non-partitioned table and 0 MB for the partitioned table
- 310.31 MB for non-partitioned table and 285.64 MB for the partitioned table

Comment:
```
-- This query will process 310.24 MB when run.
SELECT 
  DISTINCT(VendorID)
FROM `terra-77564.kestra_dataset.yellow_tripdata_parquet`
WHERE DATE(tpep_dropoff_datetime) BETWEEN '2024-03-01' AND '2024-03-15';

-- This query will process 26.84 MB when run.
SELECT 
  DISTINCT(VendorID)
FROM `terra-77564.kestra_dataset.yellow_tripdata_parquet_optimized`
WHERE DATE(tpep_dropoff_datetime) BETWEEN '2024-03-01' AND '2024-03-15';
```


Question 7. External table storage
Where is the data stored in the External Table you created?

- Big Query
- Container Registry
- ++++ GCP Bucket
- Big Table

Comment:
External Table is just a bridge to the files stored in the GCP Bucket.


Question 8. Clustering best practices
It is best practice in Big Query to always cluster your data:

- True
- ++++ False

Comment:
It depends on the table size. If the table is less than 1Gb, the service processing (for example after adding new data) can take more time than benefits of Clustering.


Question 9. Understanding table scans
No Points: Write a SELECT count(*) query FROM the materialized table you created. How many bytes does it estimate will be read? Why?

Comment:
```
-- This query will process 0 B when run
SELECT COUNT(*)
FROM `terra-77564.kestra_dataset.yellow_tripdata_parquet`;
```
The reason that in this case it will use metadata for getting total number of records.
