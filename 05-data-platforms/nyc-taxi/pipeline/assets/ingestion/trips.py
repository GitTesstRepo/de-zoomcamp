"""@bruin

# TODO: Set the asset name (recommended pattern: schema.asset_name).
# - Convention in this module: use an `ingestion.` schema for raw ingestion tables.
name: ingestion.trips

# TODO: Set the asset type.
# Docs: https://getbruin.com/docs/bruin/assets/python
type: python

# TODO: Pick a Python image version (Bruin runs Python in isolated environments).
# Example: python:3.11
image: python:3.11

# TODO: Set the connection.
connection: duckdb-default

# TODO: Choose materialization (optional, but recommended).
# Bruin feature: Python materialization lets you return a DataFrame (or list[dict]) and Bruin loads it into your destination.
# This is usually the easiest way to build ingestion assets in Bruin.
# Alternative (advanced): you can skip Bruin Python materialization and write a "plain" Python asset that manually writes
# into DuckDB (or another destination) using your own client library and SQL. In that case:
# - you typically omit the `materialization:` block
# - you do NOT need a `materialize()` function; you just run Python code
# Docs: https://getbruin.com/docs/bruin/assets/python#materialization
materialization:
  # TODO: choose `table` or `view` (ingestion generally should be a table)
  type: table
  # TODO: pick a strategy.
  # suggested strategy: append
  strategy: append

# TODO: Define output columns (names + types) for metadata, lineage, and quality checks.
# Tip: mark stable identifiers as `primary_key: true` if you plan to use `merge` later.
# Docs: https://getbruin.com/docs/bruin/assets/columns
columns:
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

@bruin"""

# TODO: Add imports needed for your ingestion (e.g., pandas, requests).
# - Put dependencies in the nearest `requirements.txt` (this template has one at the pipeline root).
# Docs: https://getbruin.com/docs/bruin/assets/python
import pandas as pd

# TODO: Only implement `materialize()` if you are using Bruin Python materialization.
# If you choose the manual-write approach (no `materialization:` block), remove this function and implement ingestion
# as a standard Python script instead.
def materialize():
    """
    TODO: Implement ingestion using Bruin runtime context.

    Required Bruin concepts to use here:
    - Built-in date window variables:
      - BRUIN_START_DATE / BRUIN_END_DATE (YYYY-MM-DD)
      - BRUIN_START_DATETIME / BRUIN_END_DATETIME (ISO datetime)
      Docs: https://getbruin.com/docs/bruin/assets/python#environment-variables
    - Pipeline variables:
      - Read JSON from BRUIN_VARS, e.g. `taxi_types`
      Docs: https://getbruin.com/docs/bruin/getting-started/pipeline-variables

    Design TODOs (keep logic minimal, focus on architecture):
    - Use start/end dates + `taxi_types` to generate a list of source endpoints for the run window.
    - Fetch data for each endpoint, parse into DataFrames, and concatenate.
    - Add a column like `extracted_at` for lineage/debugging (timestamp of extraction).
    - Prefer append-only in ingestion; handle duplicates in staging.
    """

    url = "https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2022-03.parquet"

    df = pd.read_parquet(url)
    final_dataframe = df

    return final_dataframe
