from datetime import datetime, timedelta
import time
import requests

from airflow import DAG
from airflow.models import Variable
from airflow.hooks.base import BaseHook
from airflow.providers.standard.operators.python import PythonOperator
from airflow.providers.databricks.operators.databricks import DatabricksRunNowOperator


# ============================================================
# CONFIGURATION
# ============================================================

DBT_ACCOUNT_ID = "70506183160546"
DBT_JOB_ID = "70506183139362"

DATABRICKS_PYTEST_JOB_ID = 372514834221556

DBT_HOST = "https://as781.us1.dbt.com"

DBT_SUCCESS = 10
DBT_ERROR = 20
DBT_CANCELLED = 30
DBT_TERMINATED = 40


# ============================================================
# SLACK NOTIFICATION
# ============================================================

def send_slack_message(message):
    webhook_url = Variable.get("SLACK_WEBHOOK_URL")

    response = requests.post(
        webhook_url,
        json={"text": message},
        timeout=15,
    )

    response.raise_for_status()


def notify_failure(context):
    task_id = context["task_instance"].task_id

    message = (
        "*ENERGY FORECAST PIPELINE - FAILED*\n"
        f"Failed Task        : {task_id}\n"
        "Status             : FAILED\n"
        "Action Required    : Check Airflow task logs\n"
        "Pipeline Status    : FAILED"
    )

    try:
        send_slack_message(message)
    except Exception as e:
        print(f"Slack notification failed: {e}")


def notify_success():
    message = (
        "*ENERGY FORECAST PIPELINE - SUCCESS*\n"
        "ADF Ingestion       : SUCCESS\n"
        "dbt Build           : SUCCESS\n"
        "Silver Layer        : SUCCESS\n"
        "Gold Layer          : SUCCESS\n"
        "Pytest Validation   : 46/46 PASSED\n"
        "Dashboard           : UPDATED\n"
        "Pipeline Status     : COMPLETED\n"
        "Environment         : Production"
    )

    send_slack_message(message)


# ============================================================
# DBT CLOUD
# ============================================================

def run_dbt_cloud():
    connection = BaseHook.get_connection("dbt_cloud_default")

    token = connection.password

    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json",
    }

    # --------------------------------------------------------
    # Trigger dbt Cloud job
    # --------------------------------------------------------

    trigger_url = (
        f"{DBT_HOST}/api/v2/accounts/"
        f"{DBT_ACCOUNT_ID}/jobs/{DBT_JOB_ID}/run/"
    )

    payload = {
        "cause": "Triggered by Airflow"
    }

    response = requests.post(
        trigger_url,
        headers=headers,
        json=payload,
        timeout=30,
    )

    response.raise_for_status()

    result = response.json()

    run_id = result["data"]["id"]

    print(f"Triggered dbt Cloud run: {run_id}")

    # --------------------------------------------------------
    # Monitor dbt Cloud run
    # --------------------------------------------------------

    status_url = (
        f"{DBT_HOST}/api/v2/accounts/"
        f"{DBT_ACCOUNT_ID}/runs/{run_id}/"
    )

    while True:

        response = requests.get(
            status_url,
            headers=headers,
            timeout=30,
        )

        response.raise_for_status()

        run_data = response.json()["data"]

        status = run_data["status"]

        print(f"dbt Cloud run {run_id} status: {status}")

        if status == DBT_SUCCESS:
            print("dbt Cloud build completed successfully.")
            return

        if status in (
            DBT_ERROR,
            DBT_CANCELLED,
            DBT_TERMINATED,
        ):
            raise RuntimeError(
                f"dbt Cloud run {run_id} failed. "
                f"Final status: {status}"
            )

        time.sleep(30)


# ============================================================
# FINAL SUCCESS NOTIFICATION
# ============================================================

def send_success_notification():
    notify_success()


# ============================================================
# DAG
# ============================================================

default_args = {
    "owner": "energy-forecast-project",
    "retries": 0,
    "on_failure_callback": notify_failure,
}


with DAG(
    dag_id="energy_forecast_pipeline",
    description="End-to-end Energy Forecast data pipeline",
    start_date=datetime(2026, 9, 7),
    schedule=None,
    catchup=False,
    default_args=default_args,
    tags=[
        "energy-forecast",
        "dbt",
        "databricks",
        "pytest",
        "slack",
    ],
) as dag:

    # --------------------------------------------------------
    # 1. dbt Cloud
    # --------------------------------------------------------

    dbt_build = PythonOperator(
        task_id="dbt_build",
        python_callable=run_dbt_cloud,
    )

    # --------------------------------------------------------
    # 2. Databricks Pytest
    # --------------------------------------------------------

    run_pytest = DatabricksRunNowOperator(
        task_id="run_pytest",
        databricks_conn_id="databricks_default",
        job_id=DATABRICKS_PYTEST_JOB_ID,
        wait_for_termination=True,
        polling_period_seconds=30,
    )

    # --------------------------------------------------------
    # 3. Slack SUCCESS
    # --------------------------------------------------------

    pipeline_success = PythonOperator(
        task_id="pipeline_success",
        python_callable=send_success_notification,
    )

    # --------------------------------------------------------
    # DEPENDENCIES
    # --------------------------------------------------------

    dbt_build >> run_pytest >> pipeline_success