


{{ config(materialized='table') }}

  select *
  from {{ source('staging','Walmart_customer_purchases_partitioned') }}
  
