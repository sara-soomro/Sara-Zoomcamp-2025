{{
    config(
        materialized='view'
    )
}}

select 
    dispatching_base_num  ,
    cast(PUlocationID as float64  ) as Pickup_locationid ,
    CAST(DOlocationID AS float64 ) AS dropoff_locationid,
    
    --CAST(pickup_datetime AS TIMESTAMP) AS    pickup_datetime,
     pickup_datetime,
   -- CAST(dropOff_datetime AS TIMESTAMP) AS dropoff_datetime,
     dropoff_datetime,
    CAST(SR_Flag AS float64 ) AS SR_Flag,

from  {{ source('staging','fhv_tripdata1') }}
where extract( year from  pickup_datetime ) = 2019




-- dbt build --select <model_name> --vars '{'is_test_run': 'false'}'
{% if var('is_test_run', default=true) %}

  limit 100

{% endif %}
