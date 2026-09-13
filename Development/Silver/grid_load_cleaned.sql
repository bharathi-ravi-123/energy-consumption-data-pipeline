{{ config(
    materialized='table'
) }}

WITH cleaned AS (

    SELECT

        NULLIF(TRIM(household_id), '') AS household_id,

        INITCAP(
            COALESCE(
                NULLIF(TRIM(grid_region), ''),
                'Unknown'
            )
        ) AS grid_region,

        COALESCE(
            NULLIF(TRIM(substation_name), ''),
            'Unknown'
        ) AS substation_name,

        COALESCE(
            NULLIF(TRIM(feeder_line), ''),
            'Unknown'
        ) AS feeder_line,

        COALESCE(
            NULLIF(TRIM(distribution_zone), ''),
            'Unknown'
        ) AS distribution_zone,

        COALESCE(
            NULLIF(
                UPPER(REPLACE(TRIM(grid_operator), ' ', '')),
                ''
            ),
            'Unknown'
        ) AS grid_operator,

        TRY_CAST(TRIM(grid_voltage) AS DOUBLE)
            AS grid_voltage,

        TRY_CAST(TRIM(grid_current) AS DOUBLE)
            AS grid_current,

        TRY_CAST(TRIM(grid_load_kw) AS DOUBLE)
            AS grid_load_kw,

        TRY_CAST(TRIM(transformer_load) AS DOUBLE)
            AS transformer_load,

        TRY_CAST(TRIM(line_loss_percent) AS DOUBLE)
            AS line_loss_percent,

        TRY_CAST(TRIM(load_variation) AS DOUBLE)
            AS load_variation,

        TRY_CAST(TRIM(frequency_variation) AS DOUBLE)
            AS frequency_variation,

        TRY_CAST(TRIM(grid_capacity_kw) AS DOUBLE)
            AS grid_capacity_kw,

        TRY_CAST(TRIM(demand_forecast_kw) AS DOUBLE)
            AS demand_forecast_kw,

        TRY_CAST(TRIM(reserve_margin) AS DOUBLE)
            AS reserve_margin

    FROM {{ source('bronze', 'grid_load_stream') }}

),

final AS (

    SELECT

        household_id,
        grid_region,
        substation_name,
        feeder_line,
        distribution_zone,
        grid_operator,

        COALESCE(grid_voltage, 0)
            AS grid_voltage,

        COALESCE(grid_current, 0)
            AS grid_current,

        COALESCE(grid_load_kw, 0)
            AS grid_load_kw,

        COALESCE(transformer_load, 0)
            AS transformer_load,

        COALESCE(line_loss_percent, 0)
            AS line_loss_percent,

        COALESCE(load_variation, 0)
            AS load_variation,

        COALESCE(frequency_variation, 0)
            AS frequency_variation,

        COALESCE(grid_capacity_kw, 0)
            AS grid_capacity_kw,

        COALESCE(demand_forecast_kw, 0)
            AS demand_forecast_kw,

        COALESCE(reserve_margin, 0)
            AS reserve_margin

    FROM cleaned

)

SELECT DISTINCT

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

FROM final

WHERE household_id IS NOT NULL
