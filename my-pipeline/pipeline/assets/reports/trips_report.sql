/* @bruin

name: reports.trips_report

type: duckdb.sql

depends:
  - staging.trips

materialization:
  type: table
  strategy: time_interval
  incremental_key: pickup_date
  time_granularity: date

columns:
  - name: pickup_date
    type: date
    description: "Pickup date"
    primary_key: true
    checks:
      - name: not_null
  - name: taxi_type
    type: string
    description: "Type of taxi"
    primary_key: true
    checks:
      - name: not_null
      - name: accepted_values
        value: ["yellow", "green"]
  - name: payment_type_name
    type: string
    description: "Payment type"
    primary_key: true
    checks:
      - name: not_null
      - name: accepted_values
        value: ["flex_fare", "credit_card", "cash", "no_charge", "dispute", "unknown", "voided_trip"]
  - name: trip_count
    type: bigint
    description: "Total number of trips"
    checks:
      - name: not_null
      - name: non_negative
  - name: total_fare
    type: double
    description: "Sums of the total fares"
    checks:
      - name: not_null
  - name: total_passengers
    type: double
    description: "Sums of total passengers"
    checks:
      - name: not_null

@bruin */

-- Purpose of reports:
-- - Aggregate staging data for dashboards and analytics
-- Required Bruin concepts:
-- - Filter using `{{ start_datetime }}` / `{{ end_datetime }}` for incremental runs
-- - GROUP BY your dimension + date columns

SELECT 
  CAST(pickup_datetime AS DATE) AS pickup_date,
  taxi_type,
  payment_type_name,
  COUNT(*) AS trip_count,
  SUM(fare_amount) AS total_fare,
  SUM(passenger_count) AS total_passengers
FROM staging.trips
WHERE pickup_datetime >= '{{ start_datetime }}'
  AND pickup_datetime < '{{ end_datetime }}'
GROUP BY 1, 2, 3
