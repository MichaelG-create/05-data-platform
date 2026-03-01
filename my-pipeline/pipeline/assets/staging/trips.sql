/* @bruin

name: staging.trips
type: duckdb.sql

depends:
  - ingestion.trips
  - ingestion.payment_lookup

materialization:
  type: table
  strategy: time_interval
  incremental_key: pickup_datetime
  time_granularity: timestamp

columns:
  - name: pickup_datetime
    type: timestamp
    description: "Pickup datetime"
    primary_key: true
    nullable: false
    checks:
      - name: not_null
  - name: dropoff_datetime
    type: timestamp
    description: "Dropoff datetime"
    primary_key: true
    checks:
      - name: not_null
  - name: pickup_location_id
    type: bigint
    description: "Pickup location ID"
    primary_key: true
    checks:
      - name: not_null
      - name: positive
  - name: dropoff_location_id
    type: bigint
    description: "Dropoff location ID"
    primary_key: true
    checks:
      - name: not_null
      - name: positive
  - name: fare_amount
    type: double
    description: "Fare amount"
    primary_key: true
    checks:
      - name: not_null
  - name: passenger_count
    type: double
    description: "Passenger count"
    checks:
      - name: not_null
      - name: non_negative
  - name: taxi_type
    type: string
    description: "Type of taxi"
    checks:
      - name: not_null
  - name: payment_type_id
    type: double
    description: "Payment type ID"
    checks:
      - name: not_null

custom_checks:
  - name: ensure_no_duplicates
    description: "Checks if there are duplicate trips based on composite key"
    query: |
      SELECT COUNT(*)
      FROM (
        SELECT pickup_datetime, dropoff_datetime, pickup_location_id, dropoff_location_id, fare_amount, COUNT(*)
        FROM staging.trips
        GROUP BY pickup_datetime, dropoff_datetime, pickup_location_id, dropoff_location_id, fare_amount
        HAVING COUNT(*) > 1
      )
    value: 0

@bruin */

WITH trips_unified AS (
  SELECT
    taxi_type,
    COALESCE(tpep_pickup_datetime, lpep_pickup_datetime) AS pickup_datetime,
    COALESCE(tpep_dropoff_datetime, lpep_dropoff_datetime) AS dropoff_datetime,
    pu_location_id AS pickup_location_id,
    do_location_id AS dropoff_location_id,
    payment_type AS payment_type_id,
    fare_amount,
    tip_amount,
    tolls_amount,
    total_amount,
    passenger_count
  FROM ingestion.trips
  WHERE COALESCE(tpep_pickup_datetime, lpep_pickup_datetime) >= '{{ start_datetime }}'
    AND COALESCE(tpep_pickup_datetime, lpep_pickup_datetime) < '{{ end_datetime }}'
    AND passenger_count IS NOT NULL
    AND passenger_count >= 0
    AND payment_type IS NOT NULL
    AND fare_amount >= 0
),
trips_with_lookup AS (
  SELECT
    t.*,
    COALESCE(p.payment_type_name, 'unknown') AS payment_type_name
  FROM trips_unified t
  LEFT JOIN ingestion.payment_lookup p ON t.payment_type_id = p.payment_type_id
),
deduplicated AS (
  SELECT
    *,
    ROW_NUMBER() OVER(
      PARTITION BY pickup_datetime, dropoff_datetime, pickup_location_id, dropoff_location_id, fare_amount
      ORDER BY pickup_datetime
    ) as row_num
  FROM trips_with_lookup
)
SELECT * EXCLUDE (row_num)
FROM deduplicated
WHERE row_num = 1
