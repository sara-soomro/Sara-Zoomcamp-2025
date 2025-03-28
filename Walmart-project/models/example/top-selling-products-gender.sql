{{ config(materialized='table') }}

--WITH RankedCategories AS (
    SELECT 
        Gender, 
        Category, 
        SUM(Purchase_Amount) AS total_sales,
        ROW_NUMBER() OVER (PARTITION BY Gender ORDER BY SUM(Purchase_Amount) DESC) AS rank
    FROM {{ source('staging','Walmart_customer_purchases_partitioned') }}
    GROUP BY Gender, Category
--)
--SELECT Gender, Category, total_sales
--FROM RankedCategories
--WHERE rank <= 3
