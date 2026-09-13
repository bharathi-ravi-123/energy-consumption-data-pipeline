# ⚡ Energy Consumption Forecasting Pipeline (Azure + Databricks)

## 📌 Project Overview

The **Energy Consumption Forecasting Pipeline** is an end-to-end data engineering solution designed to ingest, process, validate, transform, and analyze energy consumption data for analytics and forecasting.

The project uses **Azure Data Factory, Azure Data Lake Storage Gen2, Databricks, PySpark, Delta Lake, dbt, Apache Airflow, PyTest, Databricks SQL, and Slack** to build a scalable and automated data pipeline.

The pipeline follows the **Medallion Architecture**, where data is processed through Bronze, Silver, and Gold layers before being used for analytics and dashboard reporting.

The pipeline ensures:

- Reliable data ingestion
- Data cleaning and standardization
- Data quality validation
- Structured data transformation
- Analytics-ready data
- Automated pipeline orchestration
- Pipeline monitoring and failure notifications
- Business-ready dashboards for energy analysis

---

## 🎯 Project Objective

- Build a scalable data engineering pipeline for energy consumption analysis
- Ingest energy-related datasets into Azure Data Lake Storage Gen2
- Process raw data using Databricks and PySpark
- Implement the Medallion Architecture using Bronze, Silver, and Gold layers
- Clean and standardize data using dbt
- Build dimension and fact tables for analytics
- Perform data quality validation using PyTest
- Orchestrate the pipeline using Apache Airflow
- Monitor pipeline execution and failures
- Send pipeline notifications through Slack
- Generate analytics and forecasting insights using Gold-layer data

---

## 📊 Datasets

The project uses multiple energy-related datasets representing different aspects of an energy monitoring environment.

### Datasets Used

- **Energy Usage** → Household energy consumption, power readings, demand, and usage timestamps
- **Device Metrics** → Device category, power consumption, runtime, efficiency, temperature, and device performance
- **Grid Load** → Grid voltage, current, load, transformer load, capacity, demand forecast, and reserve margin
- **Tariff Metrics** → Unit rates, peak/off-peak rates, billing information, taxes, subsidies, and monthly bills
- **Weather Data** → Temperature, humidity, rainfall, wind speed, pressure, solar radiation, and weather conditions

These datasets are combined to analyze energy consumption patterns and identify factors that influence energy usage.

---

## 🛠️ Technologies Used

| Technology | Purpose |
|------------|---------|
| **Python** | Data processing, scripting, and testing |
| **PySpark** | Distributed data processing |
| **Azure Data Factory** | Data ingestion and pipeline movement |
| **Azure Data Lake Storage Gen2** | Cloud data lake storage |
| **Databricks** | Data processing and Lakehouse development |
| **Delta Lake** | Reliable and ACID-compliant data storage |
| **Unity Catalog** | Data governance and access management |
| **dbt** | Data transformation and data modelling |
| **Apache Airflow** | Workflow orchestration and automation |
| **PyTest** | Data quality and validation testing |
| **Databricks SQL** | SQL analytics and querying |
| **Databricks Dashboards** | Data visualization and reporting |
| **Slack** | Pipeline monitoring and failure notifications |

---

# 🏗️ Lakehouse Architecture

The pipeline follows a layered **Lakehouse / Medallion Architecture** using Azure and Databricks.

![High Level Architecture](Designs/High%20level%20design.jpg)

---

# 🔄 ETL Pipeline Design

## 🥉 Bronze Layer — Raw Data

### Purpose

The Bronze layer stores the ingested data in its raw form while maintaining the original structure and data lineage.

### Data Sources

The pipeline receives the following datasets:

```text
Energy Usage
Device Metrics
Grid Load
Tariff Metrics
Weather Data

## 🥉 Bronze Layer — Raw Data

### Purpose

The Bronze layer stores the raw energy-related datasets ingested from the source systems while preserving the original data for downstream processing.

### Bronze Tables

```text
energy_catalog.bronze.energy_usage_stream
energy_catalog.bronze.device_metrics_stream
energy_catalog.bronze.grid_load_stream
energy_catalog.bronze.tariff_metrics_stream_v2
energy_catalog.bronze.weather_source_v2

### ⚙️ Operations

- Ingest data into **Azure Data Lake Storage Gen2**
- Load raw data into **Databricks**
- Store raw datasets in **Delta format**
- Maintain **data lineage**
- Preserve source data for downstream processing

---

## 🥈 Silver Layer — Cleaned & Transformed Data

### 🎯 Purpose

The Silver layer cleans, standardizes, validates, and prepares the Bronze data for analytics.

### 🔄 Transformations

- Remove duplicate records
- Handle null and empty values
- Trim string values
- Standardize categorical values
- Convert string columns into appropriate data types
- Validate numeric ranges
- Handle malformed numeric values
- Parse timestamps
- Create date and time attributes
- Filter invalid household records

### 📋 Silver Models

```text
energy_cleaned
device_metrics_cleaned
grid_load_cleaned
tariff_metrics_cleaned
weather_source_cleaned

## 🥇 Gold Layer — Analytics & Business Data

### 🎯 Purpose

The Gold layer contains **business-ready datasets** designed for analytics, reporting, and forecasting.

The Gold layer follows a **Star Schema** with dimension tables and a fact table.

### ⭐ Gold Data Model

#### 📚 Dimension Tables

```text
dim_household
dim_feeder
dim_operator
dim_substation
dim_zone
dim_time
dim_weather

📊 Fact Table
fact_energy_consumption
📈 Gold Layer Features
  - Household-level energy consumption
  - Grid and feeder information
  - Substation and operator details
  - Distribution zone information
  - Weather attributes
  - Time-based analysis
  - Energy consumption metrics
  - Forecasting-related features
  - KPI calculations

## 🧪 Data Quality & Testing

Data quality validation is implemented using **PyTest and PySpark**.

The testing framework validates the processed data before it is used for analytics and dashboards.

### 🔍 Validation Areas

- Null value validation
- Duplicate record detection
- Schema validation
- Data type validation
- Valid business values
- Numeric range validation
- Dimension table validation
- Fact table validation
- Referential integrity checks
- KPI validation

### 🥈 Silver Layer Validation

The Silver data is validated for important data quality conditions such as:

- ✅ Required fields are not null
- 🔄 Duplicate records are identified
- 🔢 Numeric values are valid
- 🕒 Timestamp values are valid
- ✔️ Business values follow expected formats

### 🥇 Gold Layer Validation

The Gold layer is validated for:

- 📚 Dimension table integrity
- 📊 Fact table integrity
- 🔗 Dimension-to-fact relationships
- ✅ Referential integrity
- 🚨 Orphan record detection
- 📈 KPI calculation validation

These validations help ensure that only **reliable and consistent data** reaches the analytics and dashboard layer.

## 🔄 Pipeline Orchestration

The complete pipeline is orchestrated using **Apache Airflow**.

### 🔁 Pipeline Flow

```text
Azure Data Factory
        ↓
Azure Data Lake Storage Gen2
        ↓
Databricks Bronze
        ↓
dbt Silver
        ↓
dbt Gold
        ↓
PyTest Validation
        ↓
Databricks SQL
        ↓
Dashboards

⚙️ Airflow Responsibilities
 ~ Trigger pipeline execution
 ~ Coordinate dbt jobs
 ~ Trigger Databricks validation jobs
 ~ Monitor task execution
 ~ Handle task failures
 ~ Send pipeline notifications

## 🔔 Monitoring & Alerts

The pipeline uses **Slack notifications** to monitor pipeline execution and identify failures quickly.

### ❌ Failure Notification

When a pipeline task fails, **Apache Airflow** sends a Slack notification containing details about the failed task.

### ✅ Success Notification

After successful pipeline execution, Airflow sends a **pipeline completion notification** to Slack.

### 📡 Monitoring Benefits

- 🔍 Quickly identify pipeline failures
- 🚨 Receive task failure notifications
- ✅ Track successful pipeline execution
- 📊 Improve pipeline monitoring and reliability
- ⚙️ Support reliable production execution
## 📊 Analytics Dashboards

The project includes dashboards generated from the Gold layer for energy consumption analysis and forecasting.

### ⚡ Energy Overview

[View Energy Overview Dashboard](Dashboards/energy_overview.pdf)

### 🔌 Energy Grid Analysis

[View Energy Grid Dashboard](Dashboards/energy_grid.pdf)

### 📈 Energy Forecast

[View Energy Forecast Dashboard](Dashboards/energy_forecast.pdf)

These dashboards provide insights into energy consumption, grid performance, forecasting, and related operational metrics.

---

## 📈 Analytics & Business Insights

The pipeline enables analysis of:

- 🏠 Household energy consumption
- 📊 Energy usage trends
- ⏰ Peak and off-peak demand
- ⚡ Grid load patterns
- 🔌 Device energy consumption
- 🌦️ Weather impact on energy usage
- 💰 Tariff and billing patterns
- 🏭 Grid capacity and demand
- 🔮 Energy forecasting
- 📅 Time-based consumption patterns

These insights can support better energy monitoring, forecasting, and operational decision-making.


## 📁 Project Structure

```text
energy-consumption-data-pipeline/
│
├── Dashboards/
│   ├── energy_forecast.pdf
│   ├── energy_grid.pdf
│   └── energy_overview.pdf
│
├── Datasets/
│   ├── device_metrics_stream.csv
│   ├── energy_usage_stream.csv
│   ├── grid_load_stream.csv
│   ├── tariff_metrics_stream_v2 .csv
│   └── weather_source_v2.csv
│
├── Designs/
│   ├── High level design.jpg
│   ├── Low_level_design.jpeg
│   └── Data model.jpeg
│
├── Development/
│   ├── DAG/
│   ├── Silver/
│   └── Gold/
│
├── tests/
│   └── pytest.py
│
├── assets/
│   ├── bronze.png
│   ├── silver.png
│   └── gold.png
│
└── README.md

👨‍💻 Team

Energy Consumption Forecasting Pipeline

Team Members
Bharathi R
Neha
Sakthivel
Venkatesh
Balaji
Maneendra 