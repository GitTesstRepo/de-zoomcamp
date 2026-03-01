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

secrets:
  - key: duckdb-default
    inject_as: duckdb-default

# TODO: Define output columns (names + types) for metadata, lineage, and quality checks.
# Tip: mark stable identifiers as `primary_key: true` if you plan to use `merge` later.
# Docs: https://getbruin.com/docs/bruin/assets/columns
columns:
  - name: vendor_id
    type: BIGINT
    description: A code indicating the TPEP provider that provided the record.
  - name: pickup_datetime
    type: TIMESTAMP
    description: The date and time when the meter was engaged.
  - name: dropoff_datetime
    type: TIMESTAMP
    description: The date and time when the meter was disengaged.
  - name: passenger_count
    type: DOUBLE
    description: The number of passengers in the vehicle.
  - name: trip_distance
    type: DOUBLE
    description: The elapsed trip distance in miles reported by the taximeter.
  - name: ratecode_id
    type: DOUBLE
    description: The final rate code in effect at the end of the trip.
  - name: store_and_fwd_flag
    type: VARCHAR
    description: This flag indicates whether the trip record was held in vehicle memory before sending to the vendor, aka "store and forward", because the vehicle did not have a connection to the server.
  - name: pickup_location_id
    type: BIGINT
    description: TLC Taxi Zone in which the taximeter was engaged.
  - name: dropoff_location_id
    type: BIGINT
    description: TLC Taxi Zone in which the taximeter was disengaged.
  - name: payment_type
    type: BIGINT
    description: A numeric code signifying how the passenger paid for the trip.
  - name: fare_amount
    type: DOUBLE
    description: The time-and-distance fare calculated by the meter.
  - name: extra
    type: DOUBLE
    description: Miscellaneous extras and surcharges.
  - name: mta_tax
    type: DOUBLE
    description: Tax that is automatically triggered based on the metered rate in use.
  - name: tip_amount
    type: DOUBLE
    description: Tip amount - This field is automatically populated for credit card tips. Cash tips are not included.
  - name: tolls_amount
    type: DOUBLE
    description: Total amount of all tolls paid in trip.
  - name: improvement_surcharge
    type: DOUBLE
    description: Improvement surcharge assessed trips at the flag drop. The improvement surcharge began being levied in 2015.
  - name: total_amount
    type: DOUBLE
    description: The total amount charged to passengers. Does not include cash tips.
  - name: congestion_surcharge
    type: DOUBLE
    description: Total amount collected in trip for NYS congestion surcharge.
  - name: airport_fee
    type: DOUBLE
    description: For pick up only at LaGuardia and John F. Kennedy Airports.
  - name: cbd_congestion_fee
    type: DOUBLE
    description: Per-trip charge for MTA's Congestion Relief Zone starting Jan. 5, 2025.
  - name: trip_type
    type: DOUBLE
    description: A code indicating whether the trip was a street-hail or a dispatch that is automatically assigned based on the metered rate in use but can be altered by the driver.
  - name: ehail_fee
    type: string
    description: represents a charge for booking a trip through an electronic hailing application or authorized service.
  - name: service_type
    type: VARCHAR
    description: Reflect the source type of data, e.g., green, yellow,..

@bruin"""

# TODO: Add imports needed for your ingestion (e.g., pandas, requests).
# - Put dependencies in the nearest `requirements.txt` (this template has one at the pipeline root).
# Docs: https://getbruin.com/docs/bruin/assets/python
import json
import os
from datetime import datetime

import pandas as pd
from dateutil.relativedelta import relativedelta


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
    vars = json.loads(os.environ["BRUIN_VARS"])
    taxi_types = vars["taxi_types"]
    # taxi_types = ["green", "yellow"]

    start_date = (
        datetime.fromisoformat(os.environ["BRUIN_START_DATE"]).date().replace(day=1)
    )
    end_date = datetime.fromisoformat(os.environ["BRUIN_END_DATE"]).date()
    # end_date = datetime.strptime(os.environ["BRUIN_END_DATE"], "%Y-%m-%d").date()

    # start_date = date(2022, 7, 21).replace(day=1)
    # end_date = date(2022, 8, 1)

    url = "https://d37ci6vzurychx.cloudfront.net/trip-data/"

    final_dataframe = pd.DataFrame()

    renaming_dict = {
        "VendorID": "vendor_id",
        "lpep_pickup_datetime": "pickup_datetime",
        "lpep_dropoff_datetime": "dropoff_datetime",
        "RatecodeID": "ratecode_id",
        "PULocationID": "pickup_location_id",
        "DOLocationID": "dropoff_location_id",
        "tpep_pickup_datetime": "pickup_datetime",
        "tpep_dropoff_datetime": "dropoff_datetime",
        "Airport_fee": "airport_fee",
    }

    while start_date <= end_date:
        year = start_date.year
        month = start_date.month

        for taxi_type in taxi_types:
            file_name = f"{taxi_type}_tripdata_{year}-{month:02d}.parquet"

            try:
                print(f"Processing {url + file_name}...")

                df = pd.read_parquet(url + file_name)

                df["service_type"] = taxi_type

                # print(df.info())

                df.rename(columns=renaming_dict, inplace=True)

                if "ehail_fee" not in df.columns:
                    df["ehail_fee"] = 0  # Yellow taxis don't have ehail_fee

                # print('Checking:', df['ehail_fee'].apply(type).unique())

                if "trip_type" not in df.columns:
                    df["trip_type"] = 1  # Yellow taxis only do street-hail (code 1)

                if "airport_fee" not in df.columns:
                    df["airport_fee"] = 0.0  # Green taxis doesn't have airport fee

                if "cbd_congestion_fee" not in df.columns:
                    df["cbd_congestion_fee"] = 0.0  # Added starting Jan. 5, 2025.

                final_dataframe = pd.concat(
                    [final_dataframe, df], axis=0, ignore_index=True
                )
            except Exception as e:
                print(f"Error: {e} in file {file_name}")

        start_date += relativedelta(months=1)

    # print(final_dataframe.info())
    # print(final_dataframe.head(10))
    # print(final_dataframe[['service_type', 'airport_fee']].head(10))
    # print(final_dataframe[['service_type', 'airport_fee']].tail(10))

    return final_dataframe
