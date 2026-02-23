/* @bruin
name: reports.trips_report
type: duckdb.sql

depends:
  - staging.trips

materialization:
  type: table
  strategy: create+replace

columns:
  - name: payment_type_name
    type: string
    description: Human-readable payment type.
    primary_key: true
  - name: trip_date
    type: date
    description: Trip date (from pickup_datetime).
    primary_key: true
  - name: trip_count
    type: bigint
    description: Number of trips.
    checks:
      - name: non_negative
  - name: fare_amount_sum
    type: double
    description: Sum of fare_amount.
    checks:
      - name: non_negative
@bruin */

select
  coalesce(payment_type_name, 'Unknown') as payment_type_name,
  cast(pickup_datetime as date) as trip_date,
  count(*) as trip_count,
  sum(coalesce(fare_amount, 0)) as fare_amount_sum
from staging.trips
group by 1, 2
order by trip_date, trip_count desc;