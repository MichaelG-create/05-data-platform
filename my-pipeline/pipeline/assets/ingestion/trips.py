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
  - name: trip_id
    type: varchar
    description: Synthetic unique identifier for the trip (e.g. hash of key fields).
    primary_key: true

  - name: service_type
    type: varchar
    description: Taxi service type (e.g. yellow, green, fhv).

  - name: vendor_id
    type: integer
    description: Taxi technology provider (1 = Creative Mobile Technologies, 2 = VeriFone Inc.); may be null for some FHV records.

  - name: pickup_datetime
    type: timestamp
    description: Date and time when the trip started (tpep/lpep/pickup_datetime normalized to a common field).

  - name: dropoff_datetime
    type: timestamp
    description: Date and time when the trip ended (tpep/lpep/dropOff_datetime normalized to a common field).

  - name: passenger_count
    type: integer
    description: Number of passengers in the vehicle; driver-entered and may be unreliable.

  - name: trip_distance
    type: double
    description: Trip distance in miles reported by the taximeter.

  - name: pickup_location_id
    type: integer
    description: TLC Taxi Zone where the trip began (PULocationID).

  - name: dropoff_location_id
    type: integer
    description: TLC Taxi Zone where the trip ended (DOLocationID).

  - name: ratecode_id
    type: integer
    description: Final rate code at trip end (1=Standard, 2=JFK, 3=Newark, 4=Nassau/Westchester, 5=Negotiated, 6=Group).

  - name: store_and_fwd_flag
    type: varchar
    description: Indicates if trip record was stored on vehicle and forwarded later (Y/N).

  - name: payment_type
    type: integer
    description: Payment method (1=Credit card, 2=Cash, 3=No charge, 4=Dispute, 5=Unknown, 6=Voided).

  - name: fare_amount
    type: double
    description: Time-and-distance meter fare.

  - name: extra
    type: double
    description: Miscellaneous extras and surcharges (e.g. rush hour, overnight).

  - name: mta_tax
    type: double
    description: MTA tax charged on the trip.

  - name: tip_amount
    type: double
    description: Tip amount (captured for card payments only).

  - name: tolls_amount
    type: double
    description: Total tolls paid during the trip.

  - name: improvement_surcharge
    type: double
    description: Taxi improvement surcharge applied at flag drop.

  - name: total_amount
    type: double
    description: Total amount charged to the passenger, excluding cash tips.

  - name: trip_type
    type: integer
    description: Trip type for green taxis (1=Street-hail, 2=Dispatch); null for other services.

  - name: ehail_fee
    type: double
    description: E-hail fee for applicable trips; often null.

  - name: dispatching_base_num
    type: varchar
    description: TLC base license number that dispatched the trip (FHV only).

  - name: affiliated_base_number
    type: varchar
    description: Base number with which the vehicle is affiliated (FHV only).

  - name: sr_flag
    type: integer
    description: Shared ride flag for High Volume FHV (1=part of shared ride chain; null otherwise).

  - name: extracted_at
    type: timestamp
    description: Ingestion timestamp when this record was fetched into Bruin.


@bruin"""

# TODO: Add imports needed for your ingestion (e.g., pandas, requests).
# - Put dependencies in the nearest `requirements.txt` (this template has one at the pipeline root).
# Docs: https://getbruin.com/docs/bruin/assets/python


# TODO: Only implement `materialize()` if you are using Bruin Python materialization.
# If you choose the manual-write approach (no `materialization:` block), remove this function and implement ingestion
# as a standard Python script instead.


import json
import os
import pandas as pd
import requests
from io import BytesIO  # For read_csv


def materialize():
    vars_ = json.loads(os.environ.get("BRUIN_VARS", "{}"))
    taxi_types = vars_.get("taxi_types", ["yellow"])

    start_date = os.environ["BRUIN_START_DATE"]    # "2020-01-01"
    end_date = os.environ["BRUIN_END_DATE"]        # "2020-01-31"
    # parse these to decide which months to fetch

    BASE_URL = "https://github.com/DataTalksClub/nyc-tlc-data/releases/download"

    year = start_date[:4]
    month = start_date[5:7]  # "01", "02", ...

    dfs = []

    for service in taxi_types:
      csv_file_name = f"{service}_tripdata_{year}-{month}.csv.gz"
      url = f"{BASE_URL}/{service}/{csv_file_name}"

      # requests.get + read_csv here
      response = requests.get(url, timeout=60)
      response.raise_for_status()

      df = pd.read_csv(
          BytesIO(response.content),
          compression="gzip",
          low_memory=False,
      )

      



    

