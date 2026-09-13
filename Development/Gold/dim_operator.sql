{{ config(materialized='table') }}

SELECT DISTINCT
    grid_operator

FROM {{ ref('grid_load_cleaned') }}

WHERE grid_operator IS NOT NULL