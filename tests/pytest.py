import pytest
from pyspark.sql import functions as F


# ============================================================
# CONFIGURATION
# ============================================================

CATALOG = "adb_ws_energy_project_v2"


# ============================================================
# SILVER LAYER - DEVICE METRICS
# ============================================================

def test_device_household_id_not_null():

    df = spark.table(
        f"{CATALOG}.silver.stg_device_metrics"
    )

    null_count = df.filter(
        F.col("household_id").isNull()
    ).count()

    assert null_count == 0, (
        f"Found {null_count} NULL household_id values"
    )


def test_device_no_duplicate_rows():

    df = spark.table(
        f"{CATALOG}.silver.stg_device_metrics"
    )

    total_count = df.count()
    distinct_count = df.distinct().count()

    assert total_count == distinct_count, (
        f"Found {total_count - distinct_count} duplicate rows"
    )


# ============================================================
# SILVER LAYER - ENERGY USAGE
# ============================================================

def test_energy_household_id_not_null():

    df = spark.table(
        f"{CATALOG}.silver.stg_energy_usage_stream"
    )

    null_count = df.filter(
        F.col("household_id").isNull()
    ).count()

    assert null_count == 0, (
        f"Found {null_count} NULL household_id values"
    )


def test_energy_timestamp_not_null():

    df = spark.table(
        f"{CATALOG}.silver.stg_energy_usage_stream"
    )

    null_count = df.filter(
        F.col("timestamp").isNull()
    ).count()

    assert null_count == 0, (
        f"Found {null_count} NULL timestamp values"
    )


def test_energy_no_duplicate_rows():

    df = spark.table(
        f"{CATALOG}.silver.stg_energy_usage_stream"
    )

    total_count = df.count()
    distinct_count = df.distinct().count()

    assert total_count == distinct_count, (
        f"Found {total_count - distinct_count} duplicate rows"
    )


# ============================================================
# SILVER LAYER - GRID LOAD
# ============================================================

def test_grid_household_id_not_null():

    df = spark.table(
        f"{CATALOG}.silver.stg_grid_load_stream"
    )

    null_count = df.filter(
        F.col("household_id").isNull()
    ).count()

    assert null_count == 0, (
        f"Found {null_count} NULL household_id values"
    )


def test_grid_region_valid():

    df = spark.table(
        f"{CATALOG}.silver.stg_grid_load_stream"
    )

    valid_regions = [
        "East",
        "North",
        "South",
        "West",
        "Unknown"
    ]

    invalid_count = df.filter(
        ~F.col("grid_region").isin(valid_regions)
    ).count()

    assert invalid_count == 0, (
        f"Found {invalid_count} invalid grid_region values"
    )


def test_grid_no_duplicate_rows():

    df = spark.table(
        f"{CATALOG}.silver.stg_grid_load_stream"
    )

    total_count = df.count()
    distinct_count = df.distinct().count()

    assert total_count == distinct_count, (
        f"Found {total_count - distinct_count} duplicate rows"
    )


# ============================================================
# SILVER LAYER - TARIFF METRICS
# ============================================================

def test_tariff_household_id_not_null():

    df = spark.table(
        f"{CATALOG}.silver.stg_tariff_metrics"
    )

    null_count = df.filter(
        F.col("household_id").isNull()
    ).count()

    assert null_count == 0, (
        f"Found {null_count} NULL household_id values"
    )


def test_tariff_no_duplicate_rows():

    df = spark.table(
        f"{CATALOG}.silver.stg_tariff_metrics"
    )

    total_count = df.count()
    distinct_count = df.distinct().count()

    assert total_count == distinct_count, (
        f"Found {total_count - distinct_count} duplicate rows"
    )


# ============================================================
# SILVER LAYER - WEATHER
# ============================================================

def test_weather_household_id_not_null():

    df = spark.table(
        f"{CATALOG}.silver.stg_weather_source"
    )

    null_count = df.filter(
        F.col("household_id").isNull()
    ).count()

    assert null_count == 0, (
        f"Found {null_count} NULL household_id values"
    )


def test_weather_timestamp_not_null():

    df = spark.table(
        f"{CATALOG}.silver.stg_weather_source"
    )

    null_count = df.filter(
        F.col("timestamp").isNull()
    ).count()

    assert null_count == 0, (
        f"Found {null_count} NULL timestamp values"
    )


def test_weather_condition_valid():

    df = spark.table(
        f"{CATALOG}.silver.stg_weather_source"
    )

    valid_conditions = [
        "Sunny",
        "Rainy",
        "Cloudy"
    ]

    invalid_count = df.filter(
        ~F.col("condition_type").isin(valid_conditions)
    ).count()

    assert invalid_count == 0, (
        f"Found {invalid_count} invalid weather conditions"
    )


def test_weather_no_duplicate_rows():

    df = spark.table(
        f"{CATALOG}.silver.stg_weather_source"
    )

    total_count = df.count()
    distinct_count = df.distinct().count()

    assert total_count == distinct_count, (
        f"Found {total_count - distinct_count} duplicate rows"
    )