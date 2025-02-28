SELECT COUNT(*) AS row_count
FROM {{ ref('stg_green_tripdata') }}