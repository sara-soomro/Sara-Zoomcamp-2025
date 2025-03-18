{{
    config(
        materialized='view'
    )
}}

with data as (

from {{ ref('fhv_fact_trips') }}
)
select 
    
    data.yr,
    data.mon,
    Pickup_locationid,
    dropoff_locationid,
    dropoff_zone,
    pickup_zone,
    PERCENTILE_CONT(data.trip_duration, 0.90) OVER (PARTITION BY  data.yr, data.mon,Pickup_locationid,dropoff_locationid) AS p90 
    from data
    where  pickup_zone in ('Yorkville East')
    order by p90 desc
    
   


        
   

  
