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
# NYC Taxi Trips - Unified schema across yellow, green, FHV
# Normalized column names, nulls for service-specific fields
columns:
  - name: trip_id
    type: varchar(32)
    description: Synthetic unique identifier for the trip (SHA256 hash of service_type|pickup_datetime|dropoff_datetime|pickup_location_id|dropoff_location_id|fare_amount).
    primary_key: true

  - name: service_type
    type: varchar(10)
    description: Taxi service type (yellow, green, fhv).

  - name: vendor_id
    type: integer
    description: Taxi technology provider (1=Creative Mobile Technologies, 2=VeriFone Inc.); null for FHV records.

  - name: pickup_datetime
    type: timestamp
    description: Date and time when the trip started (normalized from tpep_pickup_datetime/lpep_pickup_datetime/pickup_datetime).

  - name: dropoff_datetime
    type: timestamp
    description: Date and time when the trip ended (normalized from tpep_dropoff_datetime/lpep_dropoff_datetime/dropOff_datetime).

  - name: passenger_count
    type: integer
    description: Number of passengers in the vehicle; driver-entered data (may be unreliable or null).

  - name: trip_distance
    type: double
    description: Trip distance in miles reported by the taximeter.

  - name: pickup_location_id
    type: integer
    description: TLC Taxi Zone where the trip began (normalized from PULocationID/PUlocationID).

  - name: dropoff_location_id
    type: integer
    description: TLC Taxi Zone where the trip ended (normalized from DOLocationID/DOlocationID).

  - name: ratecode_id
    type: integer
    description: Final rate code at trip end (1=Standard rate, 2=JFK, 3=Newark, 4=Nassau/Westchester, 5=Negotiated, 6=Group ride).

  - name: store_and_fwd_flag
    type: varchar(1)
    description: "'Y' if trip record stored on vehicle and forwarded later, 'N' otherwise."

  - name: payment_type
    type: integer
    description: Payment method (1=Credit card, 2=Cash, 3=No charge, 4=Dispute, 5=Unknown, 6=Voided).

  - name: fare_amount
    type: double
    description: Time-and-distance meter fare (base fare).

  - name: extra
    type: double
    description: Miscellaneous extras and surcharges (rush hour, overnight, etc.).

  - name: mta_tax
    type: double
    description: MTA tax charged on the trip ($0.50 per trip).

  - name: tip_amount
    type: double
    description: Tip amount (captured automatically for credit card payments only).

  - name: tolls_amount
    type: double
    description: Total tolls paid during the trip.

  - name: improvement_surcharge
    type: double
    description: Taxi improvement surcharge applied at flag drop ($0.50-$1.00).

  - name: total_amount
    type: double
    description: Total amount charged to passenger (fare + extras + surcharges, excludes cash tips).

  - name: trip_type
    type: integer
    description: Green taxi trip type (1=Street-hail, 2=Dispatch/e-hail); null for yellow/FHV.

  - name: ehail_fee
    type: double
    description: E-hail fee for green taxi e-hail trips; null for non-applicable services.

  - name: dispatching_base_num
    type: varchar(10)
    description: TLC base license number that dispatched the FHV trip; null for yellow/green.

  - name: affiliated_base_number
    type: varchar(10)
    description: Base number with which FHV vehicle is affiliated; null for yellow/green.

  - name: sr_flag
    type: integer
    description: Shared ride flag for High Volume FHV (1=part of shared ride chain); null otherwise.

  - name: extracted_at
    type: timestamp
    description: UTC timestamp when record was ingested into Bruin pipeline.

  - name: data_file_month
    type: varchar(7)
    description: Source filename partition (YYYY-MM format from BRUIN_START_DATE); use for lineage and partitioning.

@bruin"""

# TODO: Add imports needed for your ingestion (e.g., pandas, requests).
# - Put dependencies in the nearest `requirements.txt` (this template has one at the pipeline root).
# Docs: https://getbruin.com/docs/bruin/assets/python

import json
import os
import pandas as pd
import requests
from io import BytesIO  # For read_csv
from glob import glob
from datetime import datetime
import hashlib

# TODO: Only implement `materialize()` if you are using Bruin Python materialization.
# If you choose the manual-write approach (no `materialization:` block), remove this function and implement ingestion
# as a standard Python script instead.


DTYPES = {
    "VendorID": "Int64",
    "RatecodeID": "Int64",
    "PULocationID": "Int64",
    "DOLocationID": "Int64",
    "passenger_count": "Int64",
    "payment_type": "Int64",
    "trip_type": "Int64",           # green only (ok if missing)
    "store_and_fwd_flag": "string",
    "trip_distance": "float64",
    "fare_amount": "float64",
    "extra": "float64",
    "mta_tax": "float64",
    "tip_amount": "float64",
    "tolls_amount": "float64",
    "ehail_fee": "float64",         # green only (ok if missing)
    "improvement_surcharge": "float64",
    "total_amount": "float64",
    "congestion_surcharge": "float64",
    # FHV
    "dispatching_base_num": "string",
    "PUlocationID": "Int64",
    "DOlocationID": "Int64",
    "SR_Flag": "string",
    "Affiliated_base_number": "string",
}

PARSE_DATES = [
    # yellow
    "tpep_pickup_datetime", "tpep_dropoff_datetime",
    # green
    "lpep_pickup_datetime", "lpep_dropoff_datetime",
    # fhv
    "pickup_datetime", "dropOff_datetime",
]

TARGET_COLUMNS = [
    "data_file_month",
    "service_type",
    "trip_id",
    "vendor_id",
    "pickup_datetime",
    "dropoff_datetime",
    "pickup_location_id",
    "dropoff_location_id",
    "trip_distance",
    "passenger_count",
    "ratecode_id",
    "store_and_fwd_flag",
    "payment_type",
    "fare_amount",
    "extra",
    "mta_tax",
    "tip_amount",
    "tolls_amount",
    "improvement_surcharge",
    "total_amount",
    "trip_type",
    "ehail_fee",
    "dispatching_base_num",
    "affiliated_base_number",
    "sr_flag",
    "extracted_at",
]

def read_taxi_file_from_bytes(content: bytes, service: str) -> pd.DataFrame:
    """Adapted read_taxi_file for in-memory bytes (requests response)"""
    df = pd.read_csv(
        BytesIO(content),
        compression="gzip",
        dtype=DTYPES,
        low_memory=False,
    )
    
    # Add service_type
    df["service_type"] = service
    
    # Normalize datetime columns
    if "tpep_pickup_datetime" in df.columns:
        df["pickup_datetime"] = pd.to_datetime(df["tpep_pickup_datetime"])
        df["dropoff_datetime"] = pd.to_datetime(df["tpep_dropoff_datetime"])
    elif "lpep_pickup_datetime" in df.columns:
        df["pickup_datetime"] = pd.to_datetime(df["lpep_pickup_datetime"])
        df["dropoff_datetime"] = pd.to_datetime(df["lpep_dropoff_datetime"])
    elif "pickup_datetime" in df.columns:
        df["pickup_datetime"] = pd.to_datetime(df["pickup_datetime"])
        df["dropoff_datetime"] = pd.to_datetime(df["dropOff_datetime"]) if "dropOff_datetime" in df.columns else pd.NA
    
    # Normalize location columns
    df["pickup_location_id"] = df.get("PULocationID", df.get("PUlocationID"))
    df["dropoff_location_id"] = df.get("DOLocationID", df.get("DOlocationID"))
    
    # Normalize other columns
    df["vendor_id"] = df.get("VendorID")
    df["ratecode_id"] = df.get("RatecodeID")
    
    if "SR_Flag" in df.columns:
        df["sr_flag"] = pd.to_numeric(df["SR_Flag"], errors="coerce").astype("Int64")
    else:
        df["sr_flag"] = pd.NA
    
    df["affiliated_base_number"] = df.get("Affiliated_base_number")
    df["dispatching_base_num"] = df.get("dispatching_base_num")
    
    # Null out green-only columns for other services
    if service != "green":
        df["trip_type"] = pd.NA
        df["ehail_fee"] = pd.NA
    
    df["extracted_at"] = datetime.utcnow()
    
    return df

def add_trip_id(df: pd.DataFrame) -> pd.DataFrame:
    """Generate trip_id from key fields"""
    key_cols = ["service_type", "pickup_datetime", "dropoff_datetime", 
                "pickup_location_id", "dropoff_location_id", "fare_amount"]
    
    # Fill missing keys with NA string for hashing
    for col in key_cols:
        if col not in df.columns:
            df[col] = pd.NA
    
    key_str = df[key_cols].astype(str).agg('|'.join, axis=1).str.encode('utf-8')
    df["trip_id"] = [hashlib.sha256(x).hexdigest()[:32] for x in key_str]  # Shortened for varchar
    return df

def materialize():
    vars_ = json.loads(os.environ.get("BRUIN_VARS", "{}"))
    taxi_types = vars_.get("taxi_types", ["green"])
    
    start_date = os.environ["BRUIN_START_DATE"]  # "2020-01-01"
    end_date = os.environ["BRUIN_END_DATE"]      # "2020-01-31"
    
    BASE_URL = "https://github.com/DataTalksClub/nyc-tlc-data/releases/download"
    
    year = start_date[:4]
    month = start_date[5:7]
    data_file_month = f"{year}-{month}"
    
    dfs = []
    
    for service in taxi_types:
        csv_file_name = f"{service}_tripdata_{year}-{month}.csv.gz"
        url = f"{BASE_URL}/{service}/{csv_file_name}"
        
        print(f"Fetching {service}: {url}")  # Debug log
        
        response = requests.get(url, timeout=60)
        response.raise_for_status()
        
        # Use the unified reader
        df = read_taxi_file_from_bytes(response.content, service)
        df = add_trip_id(df)
        
        # Add year/month metadata for lineage
        df["data_file_month"] = data_file_month
        
        # Select only target columns (fills missing as NA)
        available_cols = [col for col in TARGET_COLUMNS if col in df.columns]
        df = df[available_cols]  # Keep your metadata
        
        dfs.append(df)
    
    if not dfs:
        return pd.DataFrame(columns=TARGET_COLUMNS)
    
    df = pd.concat(dfs, ignore_index=True)
    
    # Reorder to match schema + your extras at end
    cols = TARGET_COLUMNS
    df = df.reindex(columns=cols)
    
    return df