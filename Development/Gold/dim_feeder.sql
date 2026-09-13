{{ config(materialized='table') }}

SELECT DISTINCT
    grid_region,
    substation_name,
    feeder_line,
    distribution_zone

FROM {{ ref('grid_load_cleaned') }}

WHERE feeder_line IS NOT NULL