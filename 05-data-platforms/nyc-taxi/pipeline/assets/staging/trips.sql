/* @bruin
name: staging.trips
type: duckdb.sql

depends:
  - ingestion.trips
  - ingestion.payment_lookup

materialization:
  type: table

custom_checks:
  - name: row_count_possitive
    description: Ensures the table is not empty
    query: select count(*) > 0 from ingestion.trips
    value: 1

@bruin */

SELECT *
FROM ingestion.trips
WHERE tpep_pickup_datetime >= '{{ start_datetime }}'
  AND tpep_pickup_datetime < '{{ end_datetime }}'
