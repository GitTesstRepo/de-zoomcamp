/* @bruin

# Docs:
# - SQL assets: https://getbruin.com/docs/bruin/assets/sql
# - Materialization: https://getbruin.com/docs/bruin/assets/materialization
# - Quality checks: https://getbruin.com/docs/bruin/quality/available_checks

# TODO: Set the asset name (recommended: reports.trips_report).
name: reports.trips_report

# TODO: Set platform type.
# Docs: https://getbruin.com/docs/bruin/assets/sql
# suggested type: duckdb.sql
type: duckdb.sql

# TODO: Declare dependency on the staging asset(s) this report reads from.
depends:
  - staging.trips

# TODO: Choose materialization strategy.
# For reports, `time_interval` is a good choice to rebuild only the relevant time window.
# Important: Use the same `incremental_key` as staging (e.g., pickup_datetime) for consistency.
materialization:
  type: table
  # suggested strategy: time_interval
  # strategy: time_interval
  strategy: time_interval
  # TODO: set to your report's date column
  # incremental_key: tpep_pickup_datetime
  incremental_key: pickup_datetime
  # TODO: set to `date` or `timestamp`
  # time_granularity: timestamp
  time_granularity: timestamp

# TODO: Define report columns + primary key(s) at your chosen level of aggregation.
# columns:
  # - name: TODO_dim
  #   type: TODO
  #   description: TODO
  #   primary_key: true
  # - name: TODO_date
  #   type: DATE
  #   description: TODO
  #   primary_key: true
  # - name: TODO_metric
  #   type: BIGINT
  #   description: TODO
  #   checks:
  #     - name: non_negative
columns:
  - name: payment_type_id
    type: BIGINT
    description: A numeric code signifying how the passenger paid for the trip.
    primary_key: true
    nullable: false
    checks:
      - name: not_null
      - name: unique    
  - name: payment_type_name
    type: VARCHAR
    description: A text name signifying how the passenger paid for the trip.
    nullable: false
    checks:
      - name: not_null
  - name: trip_distance_average
    type: DOUBLE
    description: The elapsed trip distance in miles reported by the taximeter.
    checks:
      - name: not_null
  - name: tip_amount_max
    type: DOUBLE
    description: Tip amount - This field is automatically populated for credit card tips. Cash tips are not included.
    checks:
      - name: not_null
  - name: tolls_amount_sum
    type: DOUBLE
    description: Total amount of all tolls paid in trip.
    checks:
      - name: not_null
  - name: total_amount_sum
    type: DOUBLE
    description: The total amount charged to passengers. Does not include cash tips.
    checks:
      - name: not_null

@bruin */

-- Purpose of reports:
-- - Aggregate staging data for dashboards and analytics
-- Required Bruin concepts:
-- - Filter using `{{ start_datetime }}` / `{{ end_datetime }}` for incremental runs
-- - GROUP BY your dimension + date columns

SELECT -- TODO: replace with your aggregation logic
    payment_type_id,
    payment_type_name,
    ROUND(AVG(trip_distance), 2) AS trip_distance_average,
    MAX(tip_amount) AS tip_amount_max,
    ROUND(SUM(tolls_amount), 2) AS tolls_amount_sum,
    ROUND(SUM(total_amount), 2) AS total_amount_sum
FROM staging.trips
WHERE
    pickup_datetime >= '{{ start_datetime }}'
    AND pickup_datetime < '{{ end_datetime }}'
    AND payment_type_id IS NOT NULL
GROUP BY payment_type_id, payment_type_name
ORDER BY payment_type_id, payment_type_name;
