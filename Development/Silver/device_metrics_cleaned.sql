{{ config(
    materialized='table'
) }}

WITH cleaned AS (

    SELECT
        NULLIF(TRIM(household_id), '') AS household_id,

        COALESCE(NULLIF(TRIM(device_category), ''), 'Unknown')
            AS device_category,

        COALESCE(NULLIF(TRIM(device_brand), ''), 'Unknown')
            AS device_brand,

        COALESCE(NULLIF(TRIM(device_model), ''), 'Unknown')
            AS device_model,

        COALESCE(NULLIF(TRIM(maintenance_status), ''), 'Unknown')
            AS maintenance_status,

        COALESCE(NULLIF(TRIM(installation_region), ''), 'Unknown')
            AS installation_region,

        TRY_CAST(TRIM(runtime_hours) AS DOUBLE) AS runtime_hours,
        TRY_CAST(TRIM(device_power_kw) AS DOUBLE) AS device_power_kw,
        TRY_CAST(TRIM(motor_speed_rpm) AS DOUBLE) AS motor_speed_rpm,
        TRY_CAST(TRIM(efficiency_ratio) AS DOUBLE) AS efficiency_ratio,
        TRY_CAST(TRIM(energy_draw_kwh) AS DOUBLE) AS energy_draw_kwh,
        TRY_CAST(TRIM(heat_output) AS DOUBLE) AS heat_output,
        TRY_CAST(TRIM(cooling_load) AS DOUBLE) AS cooling_load,
        TRY_CAST(TRIM(device_voltage) AS DOUBLE) AS device_voltage,
        TRY_CAST(TRIM(device_current) AS DOUBLE) AS device_current,
        TRY_CAST(TRIM(device_temperature) AS DOUBLE) AS device_temperature

    FROM {{ source('bronze', 'device_metrics') }}

),

validated AS (

    SELECT
        household_id,
        device_category,
        device_brand,
        device_model,
        maintenance_status,
        installation_region,

        CASE WHEN runtime_hours BETWEEN 0 AND 24
             THEN runtime_hours ELSE 0 END AS runtime_hours,

        CASE WHEN device_power_kw BETWEEN 0 AND 8
             THEN device_power_kw ELSE 0 END AS device_power_kw,

        CASE WHEN motor_speed_rpm BETWEEN 0 AND 3000
             THEN motor_speed_rpm ELSE 0 END AS motor_speed_rpm,

        CASE WHEN efficiency_ratio BETWEEN 0 AND 1
             THEN efficiency_ratio ELSE 0 END AS efficiency_ratio,

        CASE WHEN energy_draw_kwh BETWEEN 0 AND 40
             THEN energy_draw_kwh ELSE 0 END AS energy_draw_kwh,

        CASE WHEN heat_output BETWEEN 0 AND 80
             THEN heat_output ELSE 0 END AS heat_output,

        CASE WHEN cooling_load BETWEEN 0 AND 60
             THEN cooling_load ELSE 0 END AS cooling_load,

        CASE WHEN device_voltage BETWEEN 210 AND 250
             THEN device_voltage ELSE 0 END AS device_voltage,

        CASE WHEN device_current BETWEEN 5 AND 25
             THEN device_current ELSE 0 END AS device_current,

        CASE WHEN device_temperature BETWEEN 20 AND 70
             THEN device_temperature ELSE 0 END AS device_temperature

    FROM cleaned

)

SELECT DISTINCT
    household_id,
    device_category,
    device_brand,
    device_model,
    maintenance_status,
    installation_region,
    runtime_hours,
    device_power_kw,
    motor_speed_rpm,
    efficiency_ratio,
    energy_draw_kwh,
    heat_output,
    cooling_load,
    device_voltage,
    device_current,
    device_temperature

FROM validated

WHERE household_id IS NOT NULL