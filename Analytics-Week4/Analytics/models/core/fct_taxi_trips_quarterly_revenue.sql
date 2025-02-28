{{ config(materialized='table') }}

with trips_data as (
    select * from {{ ref('fact_trips') }}
)

    select 
    -- Reveneue grouping according to quarter    
   CONCAT(EXTRACT(YEAR FROM pickup_datetime), '/Q', EXTRACT(QUARTER FROM pickup_datetime)) AS year_quarter,
   service_type, 

    -- Revenue calculation 
    sum(total_amount) as Q_total_amount,
    sum(fare_amount) as revenue_Q_fare,
    sum(extra) as revenue_Q_extra,
    sum(mta_tax) as revenue_Q_mta_tax,
    sum(tip_amount) as revenue_Q_amount,
    sum(tolls_amount) as revenue_Q_amount,
    sum(ehail_fee) as revenue_Q_ehail_fee,
    sum(improvement_surcharge) as revenue_Q_improvement_surcharge,
  
    -- Additional calculations
    count(tripid) as total_quarter_trips,
    avg(passenger_count) as avg_quarter_passenger_count,
    avg(trip_distance) as avg_quarter_trip_distance

    from trips_data
    where EXTRACT(YEAR FROM pickup_datetime) in (2019,2020) 
    group by 1,2


