# ⚡ Energy Consumption Forecasting Pipeline (Azure + Databricks)

## 📌 Project Overview

The **Energy Consumption Forecasting Pipeline** is a scalable data engineering solution designed to collect, process, validate, and analyze energy-related data for consumption analysis and forecasting.

The project uses **Azure Data Factory, Azure Data Lake Storage Gen2, Databricks, PySpark, Delta Lake, dbt, Apache Airflow, Databricks SQL, and Python** to build an automated data pipeline.

The pipeline follows the **Medallion Architecture** to organize data through different processing stages and produces reliable, analytics-ready data for forecasting and dashboard reporting.

The pipeline ensures:

- Reliable ingestion of energy-related data
- Data cleaning and standardization
- Data quality validation
- Structured data transformation
- Analytics-ready data for forecasting
- Automated pipeline orchestration
- Pipeline monitoring and failure notifications

---

## 🎯 Project Objective

- Build a scalable data pipeline for processing energy consumption data
- Ingest energy-related data from multiple sources
- Store raw data in Azure Data Lake Storage Gen2
- Process data using Databricks and PySpark
- Apply data cleaning and standardization using dbt
- Organize data using Medallion Architecture
- Build fact and dimension tables for analytics
- Validate data quality using PyTest
- Automate workflow execution using Apache Airflow
- Provide pipeline failure notifications through Slack
- Generate analytics and dashboards for energy consumption analysis and forecasting

---

## 📊 Dataset

### Dataset Source

The project uses **energy-related datasets** representing data collected from households, devices, electrical grids, weather sources, and tariff systems.

### Datasets Used

- **Energy Usage** → Household energy consumption, power readings, demand, and usage timestamps
- **Device Metrics** → Device category, power consumption, runtime, efficiency, temperature, and device performance
- **Grid Load** → Grid voltage, current, load, transformer load, capacity, demand forecast, and reserve margin
- **Tariff Metrics** → Unit rates, peak/off-peak rates, billing information, taxes, subsidies, and monthly bills
- **Weather Data** → Temperature, humidity, rainfall, wind speed, pressure, solar radiation, and weather conditions

These datasets together represent a **real-world energy monitoring and consumption environment** and help analyze the different factors that influence energy consumption.

---

## 🛠️ Technologies Used

| Technology | Purpose |
|------------|---------|
| **Python** | Data processing, scripting, and testing |
| **PySpark** | Distributed data processing |
| **Azure Data Factory** | Data ingestion and data movement |
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

## 🏗️ Lakehouse Architecture

The project follows a **Medallion Architecture** with Bronze, Silver, and Gold layers.


The architecture is designed to move data from raw ingestion to cleaned and analytics-ready datasets.

---

## 🔄 ETL Pipeline

The overall pipeline follows this flow:

```text
Source Data
     ↓
Azure Data Factory
     ↓
Azure Data Lake Storage Gen2
     ↓
Databricks Bronze
     ↓
dbt Silver
     ↓
Gold Layer
     ↓
Databricks SQL
     ↓
Dashboards & Analytics
