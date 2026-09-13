{{ config(
    materialized='table'
) }}

WITH cleaned AS (

    SELECT
        NULLIF(TRIM(household_id), '') AS household_id,

        COALESCE(
            NULLIF(TRIM(region_name), ''),
            'Unknown'
        ) AS region_name,

        COALESCE(
            NULLIF(TRIM(city_name), ''),
            'Unknown'
        ) AS city_name,

        COALESCE(
            NULLIF(TRIM(meter_type), ''),
            'Unknown'
        ) AS meter_type,

        COALESCE(
            NULLIF(TRIM(customer_category), ''),
            'Unknown'
        ) AS customer_category,

        COALESCE(
            NULLIF(TRIM(grid_zone), ''),
            'Unknown'
        ) AS grid_zone,

        TRY_CAST(TRIM(voltage_reading) AS DOUBLE)
            AS voltage_reading,

        TRY_CAST(TRIM(current_reading) AS DOUBLE)
            AS current_reading,

        TRY_CAST(TRIM(active_power_kw) AS DOUBLE)
            AS active_power_kw,

        TRY_CAST(TRIM(reactive_power_kvar) AS DOUBLE)
            AS reactive_power_kvar,

        TRY_CAST(TRIM(energy_usage_kwh) AS DOUBLE)
            AS energy_usage_kwh,

        TRY_CAST(TRIM(frequency_hz) AS DOUBLE)
            AS frequency_hz,

        TRY_CAST(TRIM(load_factor) AS DOUBLE)
            AS load_factor,

        TRY_CAST(TRIM(peak_demand_kw) AS DOUBLE)
            AS peak_demand_kw,

        TRY_CAST(TRIM(offpeak_demand_kw) AS DOUBLE)
            AS offpeak_demand_kw,

        TRY_CAST(TRIM(daily_consumption_kwh) AS DOUBLE)
            AS daily_consumption_kwh,

        TRY_TO_TIMESTAMP(
            TRIM(timestamp),
            'dd-MM-yyyy HH:mm'
        ) AS timestamp

    FROM {{ source('bronze', 'energy_usage_stream') }}

),

final AS (

    SELECT
        household_id,
        region_name,
        city_name,
        meter_type,
        customer_category,
        grid_zone,

        COALESCE(voltage_reading, 0) AS voltage_reading,

        COALESCE(current_reading, 0) AS current_reading,

        COALESCE(active_power_kw, 0) AS active_power_kw,

        COALESCE(reactive_power_kvar, 0)
            AS reactive_power_kvar,

        COALESCE(energy_usage_kwh, 0)
            AS energy_usage_kwh,

        COALESCE(frequency_hz, 0)
            AS frequency_hz,

        COALESCE(load_factor, 0)
            AS load_factor,

        COALESCE(peak_demand_kw, 0)
            AS peak_demand_kw,

        COALESCE(offpeak_demand_kw, 0)
            AS offpeak_demand_kw,

        COALESCE(daily_consumption_kwh, 0)
            AS daily_consumption_kwh,

        timestamp,

        CAST(timestamp AS DATE) AS usage_date,

        YEAR(timestamp) AS usage_year,

        MONTH(timestamp) AS usage_month,

        DAY(timestamp) AS usage_day,

        HOUR(timestamp) AS usage_hour,

        DAYOFWEEK(timestamp) AS day_of_week

    FROM cleaned

)

SELECT DISTINCT
    household_id,
    region_name,
    city_name,
    meter_type,
    customer_category,
    grid_zone,
    voltage_reading,
    current_reading,
    active_power_kw,
    reactive_power_kvar,
    energy_usage_kwh,
    frequency_hz,
    load_factor,
    peak_demand_kw,
    offpeak_demand_kw,
    daily_consumption_kwh,
    timestamp,
    usage_date,
    usage_year,
    usage_month,
    usage_day,
    usage_hour,
    day_of_week

FROM final

WHERE household_id IS NOT NULL