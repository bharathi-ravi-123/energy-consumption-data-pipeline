{{ config(materialized='table') }}

SELECT DISTINCT
    grid_region,
    distribution_zone

FROM {{ ref('grid_load_cleaned') }}

WHERE distribution_zone IS NOT NULL