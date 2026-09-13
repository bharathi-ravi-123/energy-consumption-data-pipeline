{{ config(
    materialized='table'
) }}

WITH cleaned AS (

    SELECT
        NULLIF(TRIM(CAST(household_id AS STRING)), '') AS household_id,

        COALESCE(
            NULLIF(TRIM(weather_region), ''),
            'Unknown'
        ) AS weather_region,

        COALESCE(
            NULLIF(TRIM(weather_city), ''),
            'Unknown'
        ) AS weather_city,

        COALESCE(
            NULLIF(TRIM(weather_station), ''),
            'Unknown'
        ) AS weather_station,

        COALESCE(
            NULLIF(TRIM(climate_zone), ''),
            'Unknown'
        ) AS climate_zone,

        CASE
            WHEN LOWER(TRIM(condition_type)) LIKE 'sunny%'
                THEN 'Sunny'
            WHEN LOWER(TRIM(condition_type)) LIKE 'rainy%'
                THEN 'Rainy'
            WHEN LOWER(TRIM(condition_type)) LIKE 'cloudy%'
                THEN 'Cloudy'
            WHEN NULLIF(TRIM(condition_type), '') IS NULL
                THEN 'Unknown'
            ELSE INITCAP(TRIM(condition_type))
        END AS condition_type,

        TRY_CAST(TRIM(CAST(temperature_celsius AS STRING)) AS DOUBLE)
            AS temperature_celsius,

        TRY_CAST(TRIM(CAST(humidity_percent AS STRING)) AS DOUBLE)
            AS humidity_percent,

        TRY_CAST(TRIM(CAST(wind_speed_kmh AS STRING)) AS DOUBLE)
            AS wind_speed_kmh,

        TRY_CAST(TRIM(CAST(rainfall_mm AS STRING)) AS DOUBLE)
            AS rainfall_mm,

        TRY_CAST(TRIM(CAST(pressure_hpa AS STRING)) AS DOUBLE)
            AS pressure_hpa,

        TRY_CAST(TRIM(CAST(solar_radiation AS STRING)) AS DOUBLE)
            AS solar_radiation,

        TRY_CAST(TRIM(CAST(dew_point AS STRING)) AS DOUBLE)
            AS dew_point,

        TRY_CAST(TRIM(CAST(uv_index AS STRING)) AS DOUBLE)
            AS uv_index,

        TRY_CAST(TRIM(CAST(visibility_km AS STRING)) AS DOUBLE)
            AS visibility_km,

        TRY_CAST(TRIM(CAST(cloud_cover_percent AS STRING)) AS DOUBLE)
            AS cloud_cover_percent,

        TRY_TO_TIMESTAMP(
            TRIM(timestamp),
            'dd-MM-yyyy'
        ) AS timestamp

    FROM {{ source('bronze', 'weather_source') }}

),

validated AS (

    SELECT
        household_id,
        weather_region,
        weather_city,
        weather_station,
        climate_zone,
        condition_type,

        CASE
            WHEN temperature_celsius BETWEEN 15 AND 40
                THEN temperature_celsius
            ELSE 0
        END AS temperature_celsius,

        CASE
            WHEN humidity_percent BETWEEN 0 AND 100
                THEN humidity_percent
            ELSE 0
        END AS humidity_percent,

        CASE
            WHEN wind_speed_kmh >= 0
                THEN wind_speed_kmh
            ELSE 0
        END AS wind_speed_kmh,

        CASE
            WHEN rainfall_mm >= 0
                THEN rainfall_mm
            ELSE 0
        END AS rainfall_mm,

        CASE
            WHEN pressure_hpa BETWEEN 980 AND 1050
                THEN pressure_hpa
            ELSE 0
        END AS pressure_hpa,

        CASE
            WHEN solar_radiation >= 0
                THEN solar_radiation
            ELSE 0
        END AS solar_radiation,

        CASE
            WHEN dew_point BETWEEN 10 AND 25
                THEN dew_point
            ELSE 0
        END AS dew_point,

        CASE
            WHEN uv_index >= 0
                THEN uv_index
            ELSE 0
        END AS uv_index,

        CASE
            WHEN visibility_km >= 0
                THEN visibility_km
            ELSE 0
        END AS visibility_km,

        CASE
            WHEN cloud_cover_percent BETWEEN 0 AND 100
                THEN cloud_cover_percent
            ELSE 0
        END AS cloud_cover_percent,

        timestamp

    FROM cleaned

)

SELECT DISTINCT
    household_id,
    weather_region,
    weather_city,
    weather_station,
    climate_zone,
    condition_type,
    temperature_celsius,
    humidity_percent,
    wind_speed_kmh,
    rainfall_mm,
    pressure_hpa,
    solar_radiation,
    dew_point,
    uv_index,
    visibility_km,
    cloud_cover_percent,
    timestamp

FROM validated

WHERE household_id IS NOT NULL