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
  strategy: time_interval
  # TODO: set incremental_key to your event time column (DATE or TIMESTAMP).
  incremental_key: pickup_datetime
  # TODO: choose `date` vs `timestamp` based on the incremental_key type.
  time_granularity: timestamp

# TODO: Define output columns, mark primary keys, and add a few checks.
columns:
  - name: trip_id
    type: varchar(32)
    description: Synthetic unique identifier for the trip (SHA256 hash of service_type|pickup_datetime|dropoff_datetime|pickup_location_id|dropoff_location_id|fare_amount).
    primary_key: true
    nullable: false
    checks:
      - name: not_null

  - name: service_type
    type: varchar(10)
    description: Taxi service type (yellow, green, fhv).

  - name: vendor_id
    type: integer
    description: Taxi technology provider (1=Creative Mobile Technologies, 2=VeriFone Inc.); null for FHV.
    checks:
      - name: non_negative

  - name: pickup_datetime
    type: timestamp
    description: Date and time when the trip started (normalized).
    checks:
      - name: not_null

  - name: dropoff_datetime
    type: timestamp
    description: Date and time when the trip ended (normalized).

  - name: passenger_count
    type: integer
    description: Number of passengers in the vehicle; driver-entered, may be unreliable.

  - name: trip_distance
    type: double
    description: Trip distance in miles reported by the taximeter.
    checks:
      - name: non_negative

  - name: pickup_location_id
    type: integer
    description: TLC Taxi Zone where the trip began.

  - name: dropoff_location_id
    type: integer
    description: TLC Taxi Zone where the trip ended.

  - name: ratecode_id
    type: integer
    description: Final rate code at trip end (1=Standard, 2=JFK, 3=Newark, 4=Nassau/Westchester, 5=Negotiated, 6=Group).

  - name: store_and_fwd_flag
    type: varchar(1)
    description: 'Y' if trip record stored on vehicle and forwarded later, 'N' otherwise.

  - name: payment_type
    type: integer
    description: Payment method (1=Credit card, 2=Cash, 3=No charge, 4=Dispute, 5=Unknown, 6=Voided).

  - name: fare_amount
    type: double
    description: Time-and-distance meter fare (base fare).

  - name: extra
    type: double
    description: Miscellaneous extras and surcharges.

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
    description: Total amount charged to the passenger; excludes cash tips.

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
    description: UTC timestamp when record was ingested into Bruin.

  - name: data_file_month
    type: varchar(7)
    description: Source filename partition (YYYY-MM from BRUIN_START_DATE) used for lineage and partitioning.

# TODO: Add one custom check that validates a staging invariant (uniqueness, ranges, etc.)
# Docs: https://getbruin.com/docs/bruin/quality/custom
custom_checks:
  - name: unique_trip_id
    description: Trip ID should be unique in staging.trips.
    query: |
      SELECT COUNT(*) AS duplicate_trip_ids
      FROM (
        SELECT trip_id
        FROM staging.trips
        GROUP BY trip_id
        HAVING COUNT(*) > 1
      )

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

WITH ranked AS (
  SELECT
    *,
    ROW_NUMBER() OVER (
      PARTITION BY trip_id
      ORDER BY extracted_at DESC
    ) AS row_num
  FROM ingestion.trips
  WHERE pickup_datetime >= '{{ start_datetime }}'
    AND pickup_datetime < '{{ end_datetime }}'
)
SELECT
  trip_id,
  service_type,
  vendor_id,
  pickup_datetime,
  dropoff_datetime,
  passenger_count,
  trip_distance,
  pickup_location_id,
  dropoff_location_id,
  ratecode_id,
  store_and_fwd_flag,
  payment_type,
  fare_amount,
  extra,
  mta_tax,
  tip_amount,
  tolls_amount,
  improvement_surcharge,
  total_amount,
  trip_type,
  ehail_fee,
  dispatching_base_num,
  affiliated_base_number,
  sr_flag,
  extracted_at,
  data_file_month
FROM ranked
WHERE row_num = 1;
