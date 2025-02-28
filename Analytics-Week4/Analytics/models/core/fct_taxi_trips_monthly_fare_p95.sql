{{
    config(
        materialized='view'
    )
}}

with filtered_fact_data as (
    select 
EXTRACT(month from pickup_datetime) as mon ,
EXTRACT(year from pickup_datetime) as yr , 
service_type,
fare_amount

from {{ ref('fact_trips') }}
where 
 fare_amount > 0 and trip_distance > 0 
  AND lower(payment_type_description) in ('cash', 'credit card')

)
select 
    service_type,
    yr,
    mon,
    PERCENTILE_CONT(fare_amount, 0.90) OVER (PARTITION BY service_type, yr, mon) AS p90,
    PERCENTILE_CONT(fare_amount, 0.95) OVER (PARTITION BY service_type, yr, mon) AS p95,
    PERCENTILE_CONT(fare_amount, 0.97) OVER (PARTITION BY service_type, yr, mon) AS p97
    from filtered_fact_data

  
