{{
    config(
        materialized='table'
    )
}}

with fhv_tripdata as 
(
  select *
  from {{ source('staging','fhv_tripdata_2019_ext`') }}
)
select

    {{ dbt.safe_cast("PUlocationID", api.Column.translate_type("integer")) }} as pickup_locationid,
    {{ dbt.safe_cast("DOlocationID", api.Column.translate_type("integer")) }} as dropoff_locationid,   
    TIMESTAMP(CAST(fhv_tripdata.pickup_datetime AS STRING)) AS pickup_datetime,
    TIMESTAMP(CAST(fhv_tripdata.dropOff_datetime AS STRING)) AS dropoff_datetime,
    dispatching_base_num , SR_Flag , Affiliated_base_number
from  fhv_tripdata



-- dbt build --select <model_name> --vars '{'is_test_run': 'false'}'
{% if var('is_test_run', default=true) %}

  limit 100

{% endif %}

