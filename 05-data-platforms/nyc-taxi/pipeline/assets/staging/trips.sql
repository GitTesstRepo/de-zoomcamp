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
  # TODO: set incremental_key to your event time column (DATE or TIMESTAMP).
  # incremental_key: TODO_SET_INCREMENTAL_KEY
  # TODO: choose `date` vs `timestamp` based on the incremental_key type.
  # time_granularity: TODO_SET_GRANULARITY

# TODO: Define output columns, mark primary keys, and add a few checks.
columns:
  # - name: TODO_pk1
    # type: TODO
    # description: TODO
    # primary_key: true
    # nullable: false
    # checks:
      # - name: not_null
  # - name: TODO_metric
    # type: TODO
    # description: TODO
    # checks:
      # - name: non_negative
  - name: tpep_pickup_datetime
    type: string
    description: The date and time when the meter was engaged.
  - name: tpep_dropoff_datetime
    type: string
    description: The date and time when the meter was disengaged.
  - name: passenger_count
    type: string
    description: The number of passengers in the vehicle.
  - name: trip_distance
    type: string
    description: The elapsed trip distance in miles reported by the taximeter.
  - name: RatecodeID
    type: string
    description: The final rate code in effect at the end of the trip.
  - name: store_and_fwd_flag
    type: string
    description: This flag indicates whether the trip record was held in vehicle memory before
sending to the vendor, aka “store and forward,” because the vehicle did not
have a connection to the server.
  - name: PULocationID
    type: string
    description: TLC Taxi Zone in which the taximeter was engaged.
  - name: DOLocationID
    type: string
    description: TLC Taxi Zone in which the taximeter was disengaged.
  - name: payment_type
    type: string
    description: A numeric code signifying how the passenger paid for the trip.
  - name: fare_amount
    type: string
    description: The time-and-distance fare calculated by the meter.
  - name: extra
    type: string
    description: Miscellaneous extras and surcharges.
  - name: mta_tax
    type: string
    description: Tax that is automatically triggered based on the metered rate in use.
  - name: tip_amount
    type: string
    description: Tip amount - This field is automatically populated for credit card tips. Cash
tips are not included.
  - name: tolls_amount
    type: string
    description: Total amount of all tolls paid in trip.
  - name: improvement_surcharge
    type: string
    description: Improvement surcharge assessed trips at the flag drop. The improvement
surcharge began being levied in 2015.
  - name: total_amount
    type: string
    description: The total amount charged to passengers. Does not include cash tips.
  - name: congestion_surcharge
    type: string
    description: Total amount collected in trip for NYS congestion surcharge.
  - name: airport_fee
    type: string
    description: For pick up only at LaGuardia and John F. Kennedy Airports.

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

SELECT *
FROM ingestion.trips
WHERE tpep_pickup_datetime >= '{{ start_datetime }}'
  AND tpep_pickup_datetime < '{{ end_datetime }}'
