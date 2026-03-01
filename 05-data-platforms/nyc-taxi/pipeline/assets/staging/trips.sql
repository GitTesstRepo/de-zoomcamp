/* @bruin

# Docs:
# - Materialization: https://getbruin.com/docs/bruin/assets/materialization
# - Quality checks (built-ins): https://getbruin.com/docs/bruin/quality/available_checks
# - Custom checks: https://getbruin.com/docs/bruin/quality/custom

# TODO: Set the asset name (recommended: staging.trips).
name: staging.trips
# TODO: Set platform type.
# Docs: https://getbruin.com/docs/bruin/assets/sql
# suggested type: duckdb.sql
type: duckdb.sql

# TODO: Declare dependencies so `bruin run ... --downstream` and lineage work.
# Examples:
# depends:
#   - ingestion.trips
#   - ingestion.payment_lookup
depends:
  - ingestion.trips
  - ingestion.payment_lookup

# TODO: Choose time-based incremental processing if the dataset is naturally time-windowed.
# - This module expects you to use `time_interval` to reprocess only the requested window.
materialization:
  # What is materialization?
  # Materialization tells Bruin how to turn your SELECT query into a persisted dataset.
  # Docs: https://getbruin.com/docs/bruin/assets/materialization
  #
  # Materialization "type":
  # - table: persisted table
  # - view: persisted view (if the platform supports it)
  type: table
  # TODO: set a materialization strategy.
  # Docs: https://getbruin.com/docs/bruin/assets/materialization
  # suggested strategy: time_interval
  #
  # Incremental strategies (what does "incremental" mean?):
  # Incremental means you update only part of the destination instead of rebuilding everything every run.
  # In Bruin, this is controlled by `strategy` plus keys like `incremental_key` and `time_granularity`.
  #
  # Common strategies you can choose from (see docs for full list):
  # - create+replace (full rebuild)
  # - truncate+insert (full refresh without drop/create)
  # - append (insert new rows only)
  # - delete+insert (refresh partitions based on incremental_key values)
  # - merge (upsert based on primary key)
  # - time_interval (refresh rows within a time window)
  # strategy: TODO
  strategy: time_interval
  # TODO: set incremental_key to your event time column (DATE or TIMESTAMP).
  # incremental_key: TODO_SET_INCREMENTAL_KEY
  incremental_key: pickup_datetime
  # TODO: choose `date` vs `timestamp` based on the incremental_key type.
  # time_granularity: TODO_SET_GRANULARITY
  time_granularity: timestamp

# TODO: Define output columns, mark primary keys, and add a few checks.
columns:
  - name: pk
    type: VARCHAR
    description: Unique identifier of a trip
    primary_key: true
    nullable: false
    checks:
      - name: not_null
      - name: unique
  # - name: TODO_metric
    # type: TODO
    # description: TODO
    # checks:
      # - name: non_negative

  - name: vendor_id
    type: BIGINT
    description: A code indicating the TPEP provider that provided the record.
    checks:
      - name: not_null
  - name: pickup_datetime
    type: TIMESTAMP
    description: The date and time when the meter was engaged.
    checks:
      - name: not_null
  - name: dropoff_datetime
    type: TIMESTAMP
    description: The date and time when the meter was disengaged.
    checks:
      - name: not_null
  - name: passenger_count
    type: DOUBLE
    description: The number of passengers in the vehicle.
    checks:
      - name: non_negative
  - name: trip_distance
    type: DOUBLE
    description: The elapsed trip distance in miles reported by the taximeter.
    checks:
      - name: not_null
  - name: ratecode_id
    type: DOUBLE
    description: The final rate code in effect at the end of the trip.
  - name: store_and_fwd_flag
    type: VARCHAR
    description: This flag indicates whether the trip record was held in vehicle memory before sending to the vendor, aka "store and forward", because the vehicle did not have a connection to the server.
  - name: pickup_location_id
    type: BIGINT
    description: TLC Taxi Zone in which the taximeter was engaged.
    checks:
      - name: not_null
  - name: dropoff_location_id
    type: BIGINT
    description: TLC Taxi Zone in which the taximeter was disengaged.
    checks:
      - name: not_null
  - name: payment_type_id
    type: BIGINT
    description: A numeric code signifying how the passenger paid for the trip.
  - name: fare_amount
    type: DOUBLE
    description: The time-and-distance fare calculated by the meter.
    checks:
      - name: not_null
  - name: extra
    type: DOUBLE
    description: Miscellaneous extras and surcharges.
    checks:
      - name: not_null
  - name: mta_tax
    type: DOUBLE
    description: Tax that is automatically triggered based on the metered rate in use.
    checks:
      - name: not_null
  - name: tip_amount
    type: DOUBLE
    description: Tip amount - This field is automatically populated for credit card tips. Cash tips are not included.
    checks:
      - name: not_null
  - name: tolls_amount
    type: DOUBLE
    description: Total amount of all tolls paid in trip.
    checks:
      - name: not_null
  - name: improvement_surcharge
    type: DOUBLE
    description: Improvement surcharge assessed trips at the flag drop. The improvement surcharge began being levied in 2015.
    checks:
      - name: not_null
  - name: total_amount
    type: DOUBLE
    description: The total amount charged to passengers. Does not include cash tips.
    checks:
      - name: not_null
  - name: congestion_surcharge
    type: DOUBLE
    description: Total amount collected in trip for NYS congestion surcharge.
  - name: airport_fee
    type: DOUBLE
    description: For pick up only at LaGuardia and John F. Kennedy Airports.
  - name: cbd_congestion_fee
    type: DOUBLE
    description: Per-trip charge for MTA's Congestion Relief Zone starting Jan. 5, 2025.
    checks:
      - name: not_null
  - name: trip_type
    type: DOUBLE
    description: A code indicating whether the trip was a street-hail or a dispatch that is automatically assigned based on the metered rate in use but can be altered by the driver.
  - name: ehail_fee
    type: string
    description: represents a charge for booking a trip through an electronic hailing application or authorized service.
  - name: service_type
    type: VARCHAR
    description: Reflect the source type of data, e.g., green, yellow,..
    checks:
      - name: not_null
  - name: payment_type_name
    type: VARCHAR
    description: A text name signifying how the passenger paid for the trip.
    checks:
      - name: not_null

# TODO: Add one custom check that validates a staging invariant (uniqueness, ranges, etc.)
# Docs: https://getbruin.com/docs/bruin/quality/custom
custom_checks:
  - name: row_count_possitive
    description: Ensures the table is not empty
    query: |
      -- TODO: return a single scalar (COUNT(*), etc.) that should match `value`
      SELECT COUNT(*) > 0 FROM ingestion.trips
    value: 1

@bruin */

-- TODO: Write the staging SELECT query.
--
-- Purpose of staging:
-- - Clean and normalize schema from ingestion
-- - Deduplicate records (important if ingestion uses append strategy)
-- - Enrich with lookup tables (JOINs)
-- - Filter invalid rows (null PKs, negative values, etc.)
--
-- Why filter by {{ start_datetime }} / {{ end_datetime }}?
-- When using `time_interval` strategy, Bruin:
--   1. DELETES rows where `incremental_key` falls within the run's time window
--   2. INSERTS the result of your query
-- Therefore, your query MUST filter to the same time window so only that subset is inserted.
-- If you don't filter, you'll insert ALL data but only delete the window's data = duplicates.

SELECT
    it.vendor_id,
    it.pickup_datetime,
    it.dropoff_datetime,
    it.passenger_count,
    it.trip_distance,
    it.ratecode_id,
    it.store_and_fwd_flag,
    it.pickup_location_id,
    it.dropoff_location_id,
    it.payment_type AS payment_type_id,
    it.fare_amount,
    it.extra,
    it.mta_tax,
    it.tip_amount,
    it.tolls_amount,
    it.improvement_surcharge,
    it.total_amount,
    it.congestion_surcharge,
    it.airport_fee,
    it.cbd_congestion_fee,
    it.trip_type,
    it.ehail_fee,
    it.service_type,
    -- ipl."_DLT_LOAD_ID",
    -- ipl."_DLT_ID",
    -- ipl.payment_type_id,
    md5(it.vendor_id || it.pickup_datetime || it.pickup_location_id || it.service_type) AS pk,
    COALESCE(ipl.payment_type_name, 'Unknown') AS payment_type_name
FROM ingestion.trips AS it
    LEFT JOIN ingestion.payment_lookup AS ipl ON it.payment_type = ipl.payment_type_id
WHERE
    it.pickup_datetime >= '{{ start_datetime }}'
    AND it.pickup_datetime < '{{ end_datetime }}'
--QUALIFY ROW_NUMBER() OVER (
--  PARTITION BY it.vendor_id, it.pickup_datetime, it.dropoff_datetime, it.total_amount
--) = 1
QUALIFY ROW_NUMBER() OVER (
    PARTITION BY it.vendor_id, it.pickup_datetime, pickup_location_id, it.service_type
    ORDER BY it.dropoff_datetime
  ) = 1;
