{{ config(materialized='table') }}

SELECT DISTINCT
    household_id,
    customer_category AS customer_type,
    meter_type AS connection_type,
    region_name AS region,
    city_name AS city

FROM {{ ref('energy_cleaned') }}

WHERE household_id IS NOT NULL