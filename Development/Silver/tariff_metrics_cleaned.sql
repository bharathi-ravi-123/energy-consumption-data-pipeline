{{ config(
    materialized='table'
) }}

WITH cleaned AS (

    SELECT

        NULLIF(TRIM(household_id), '') AS household_id,

        COALESCE(
            NULLIF(INITCAP(TRIM(tariff_region)), ''),
            'Unknown'
        ) AS tariff_region,

        COALESCE(
            NULLIF(INITCAP(TRIM(tariff_city)), ''),
            'Unknown'
        ) AS tariff_city,

        COALESCE(
            NULLIF(INITCAP(TRIM(tariff_plan_type)), ''),
            'Unknown'
        ) AS tariff_plan_type,

        CASE
            WHEN LOWER(TRIM(billing_cycle)) = 'monthly'
                THEN 'Monthly'
            WHEN LOWER(TRIM(billing_cycle)) = 'bi-monthly'
                THEN 'Bi-Monthly'
            WHEN NULLIF(TRIM(billing_cycle), '') IS NULL
                THEN 'Unknown'
            ELSE TRIM(billing_cycle)
        END AS billing_cycle,

        COALESCE(
            NULLIF(TRIM(utility_provider), ''),
            'Unknown'
        ) AS utility_provider,

        TRY_CAST(TRIM(unit_rate) AS DOUBLE) AS unit_rate,
        TRY_CAST(TRIM(peak_rate) AS DOUBLE) AS peak_rate,
        TRY_CAST(TRIM(offpeak_rate) AS DOUBLE) AS offpeak_rate,
        TRY_CAST(TRIM(fixed_charge) AS DOUBLE) AS fixed_charge,
        TRY_CAST(TRIM(tax_amount) AS DOUBLE) AS tax_amount,
        TRY_CAST(TRIM(subsidy_amount) AS DOUBLE) AS subsidy_amount,
        TRY_CAST(TRIM(monthly_bill) AS DOUBLE) AS monthly_bill,
        TRY_CAST(TRIM(billing_units) AS DOUBLE) AS billing_units,
        TRY_CAST(TRIM(late_fee) AS DOUBLE) AS late_fee,
        TRY_CAST(TRIM(adjustment_amount) AS DOUBLE) AS adjustment_amount

    FROM {{ source('bronze', 'tariff_metrics') }}

),

final AS (

    SELECT

        household_id,
        tariff_region,
        tariff_city,
        tariff_plan_type,
        billing_cycle,
        utility_provider,

        COALESCE(unit_rate, 0) AS unit_rate,
        COALESCE(peak_rate, 0) AS peak_rate,
        COALESCE(offpeak_rate, 0) AS offpeak_rate,
        COALESCE(fixed_charge, 0) AS fixed_charge,
        COALESCE(tax_amount, 0) AS tax_amount,
        COALESCE(subsidy_amount, 0) AS subsidy_amount,
        COALESCE(monthly_bill, 0) AS monthly_bill,
        COALESCE(billing_units, 0) AS billing_units,
        COALESCE(late_fee, 0) AS late_fee,
        COALESCE(adjustment_amount, 0) AS adjustment_amount

    FROM cleaned

)

SELECT DISTINCT
    household_id,
    tariff_region,
    tariff_city,
    tariff_plan_type,
    billing_cycle,
    utility_provider,
    unit_rate,
    peak_rate,
    offpeak_rate,
    fixed_charge,
    tax_amount,
    subsidy_amount,
    monthly_bill,
    billing_units,
    late_fee,
    adjustment_amount

FROM final

WHERE household_id IS NOT NULL