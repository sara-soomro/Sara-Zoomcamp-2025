{{ config(materialized='table') }}

with trips_data as (
    select * from {{ ref('fact_trips') }}
),

 quarterly_revenue AS (
    -- Step 1: Aggregate revenue by quarter and year
    SELECT 
        EXTRACT(YEAR FROM pickup_datetime) AS revenue_year,
        EXTRACT(QUARTER FROM pickup_datetime) AS revenue_quarter,
        SUM(total_amount) AS revenue -- Replace `total_amount` with your revenue column
    FROM trips_data -- Replace with your actual fact table or model
    GROUP BY 
        revenue_year,
        revenue_quarter
),

yoy_revenue_growth AS (
    -- Step 2: Join the current year's data with the previous year's data for the same quarter
    SELECT 
        present.revenue_year,
        present.revenue_quarter,
        present.revenue AS present_year_revenue,
        previous.revenue AS previous_year_revenue,
        -- Step 3: Calculate YoY growth
        IFNULL(((present.revenue - previous.revenue) / previous.revenue) * 100, 0) AS yoy_growth_percentage
    FROM quarterly_revenue AS present
    LEFT JOIN quarterly_revenue AS previous
        ON present.revenue_quarter = previous.revenue_quarter
        AND present.revenue_year = previous.revenue_year + 1  -- Ensure we're comparing the same quarter of the current year to last year
)
-- Final output
SELECT 
    revenue_year,
    revenue_quarter,
    present_year_revenue,
    previous_year_revenue,
    yoy_growth_percentage
FROM yoy_revenue_growth; 