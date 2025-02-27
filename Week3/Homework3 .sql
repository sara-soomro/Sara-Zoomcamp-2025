CREATE EXTERNAL TABLE `ivory-period-444518-v9.zoomcamp.yellow_tripdata_2024_ext`
OPTIONS (
  format = 'PARQUET',
  uris = ['gs://kestra-ivory-period-444518-v9/yellow_tripdata_2024-*.parquet']
) ;
CREATE TABLE `ivory-period-444518-v9.zoomcamp.yellow_tripdata_2024` AS
Select *
FROM `ivory-period-444518-v9.zoomcamp.yellow_tripdata_2024_ext`;  # '20,332,093'


SELECT DISTINCT(PULocationID)
FROM `ivory-period-444518-v9.zoomcamp.yellow_tripdata_2024` ;  


SELECT DISTINCT(PULocationID)
FROM `ivory-period-444518-v9.zoomcamp.yellow_tripdata_2024_ext` ; -155.12
 -155.12MB

select count(*) 
FROM `ivory-period-444518-v9.zoomcamp.yellow_tripdata_2024` 
where fare_amount =0; -8333



CREATE OR REPLACE TABLE `ivory-period-444518-v9.zoomcamp.yellow_tripdata_2024_optimized`
PARTITION BY DATE(tpep_dropoff_datetime)   -- Partitioning for faster filtering
CLUSTER BY VendorID      AS (
  SELECT * FROM `ivory-period-444518-v9.zoomcamp.yellow_tripdata_2024`
);



SELECT DOLocationID  FROM  `ivory-period-444518-v9.zoomcamp.yellow_tripdata_2024` ; -155.12



SELECT DOLocationID , PULocationID  FROM  `ivory-period-444518-v9.zoomcamp.yellow_tripdata_2024_optimized` ; - 310.24 ML
 
SELECT distinct(VendorID)  FROM  `ivory-period-444518-v9.zoomcamp.yellow_tripdata_2024`    #310MB
WHERE DATE(tpep_dropoff_datetime ) BETWEEN '2024-03-01' AND '2024-03-15' ;
2024-03-01 and 2024-03-15
 SELECT distinct(VendorID)  FROM  `ivory-period-444518-v9.zoomcamp.yellow_tripdata_2024_optimized`    #26.84MB
WHERE DATE(tpep_dropoff_datetime ) BETWEEN '2024-03-01' AND '2024-03-15' ;


