{{ config(materialized='table') }}

SELECT DISTINCT
    substation_name

FROM {{ ref('grid_load_cleaned') }}

WHERE substation_name IS NOT NULL