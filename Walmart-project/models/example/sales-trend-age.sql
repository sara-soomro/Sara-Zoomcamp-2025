{{ config(materialized='table') }}

WITH sales_age AS (
    SELECT
        Customer_ID, 
        Gender, 
        Category, 
        Purchase_Amount,
        CASE 
            WHEN Age < 20 THEN 'Kid'
            WHEN Age BETWEEN 20 AND 40 THEN 'Adult'
            WHEN Age > 55 THEN 'Senior'
            ELSE 'Unknown'
        END AS Age_Category 
    FROM {{ source('staging', 'Walmart_customer_purchases_partitioned') }}
)

SELECT 
    Age_Category, 
    Gender, 
    SUM(Purchase_Amount) AS Total_Purchase_Amount
FROM sales_age
GROUP BY Age_Category, Gender
