{{
    config(
        materialized='view'
    )
}}

select 
     dispatching_base_num  ,
    cast(PUlocationID as integer ) as Pickup_locationid ,
    cast(DOlocationID  as integer) as dropoff_locationid,
    CAST(pickup_datetime AS TIMESTAMP)) AS pickup_datetime,
    CAST(dropOff_datetime AS TIMESTAMP)) AS dropoff_datetime,
   , SR_Flag 
from  {{ source('staging','fhv_tripdata`') }}
where extract( year from Pickup_locationid ) = 2019


-- dbt build --select <model_name> --vars '{'is_test_run': 'false'}'
{% if var('is_test_run', default=true) %}

  limit 100

{% endif %}

