{{ config(materialized='table') }}

SELECT
    household_id,
    grid_region,
    substation_name,
    feeder_line,
    distribution_zone,
    grid_operator,
    grid_voltage,
    grid_current,
    grid_load_kw,
    transformer_load,
    line_loss_percent,
    load_variation,
    frequency_variation,
    grid_capacity_kw,
    demand_forecast_kw,
    reserve_margin

FROM {{ ref('grid_load_cleaned') }}

WHERE household_id IS NOT NULL