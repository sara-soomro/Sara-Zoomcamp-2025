{{ config(materialized='table') }}

with trips_data as (
    select * from {{ ref('fact_trips') }}
),
quarterly_revenue as (
    -- Step 1: Aggregate revenue by quarter and year
    SELECT 
        EXTRACT(YEAR FROM pickup_datetime) AS revenue_year,
        EXTRACT(QUARTER FROM pickup_datetime) AS revenue_quarter,
        service_type,
        SUM(total_amount) AS revenue -- 
    FROM trips_data 
    where EXTRACT(YEAR FROM pickup_datetime) in (2019,2020) 
    GROUP BY 
        revenue_year,
        revenue_quarter, 
        service_type
),

yoy_revenue_growth AS (

    SELECT 
        present.service_type,
        present.revenue_year,
        present.revenue_quarter,
        present.revenue AS present_year_revenue,
        previous.revenue AS previous_year_revenue,
        -- Step 3: Calculate YoY growth
        IFNULL(((present.revenue - previous.revenue) / previous.revenue) * 100, 0) AS yoy_growth_percentage
    FROM quarterly_revenue AS present
    LEFT JOIN quarterly_revenue AS previous
        ON present.revenue_quarter = previous.revenue_quarter
        AND present.revenue_year = previous.revenue_year + 1 

)
-- Final output
SELECT 
    service_type,
    revenue_year,
    revenue_quarter,
    present_year_revenue,
    previous_year_revenue,
    yoy_growth_percentage
FROM yoy_revenue_growth 