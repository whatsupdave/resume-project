from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator
from datetime import datetime, timedelta

# DAG Config
DAG_NAME = "weather_monitor"
DAG_DESCRIPTION = "Gets weather data"

# S3 Config
S3_BUCKER_NAME = ""
S3_FOLDER_NAME = ""
S3_FILE_NAME = ""

default_args = {
    "owner": "Eivydas",
    "depends_on_past": False,
    "start_date": datetime(2023, 9, 25),
    "email_on_failure": False,
    "email": "kantautaseivydas@gmail.com",
    "email_on_retry": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=3),
    # "on_failure_callback": slack_failure_callback
}

with DAG(
    DAG_NAME,
    default_args=default_args,
    description=DAG_DESCRIPTION,
    tags=["placeholder"],
    start_date=datetime(2023, 9, 25),
    schedule="0 5 * * *",
    max_active_runs=1,
    catchup=False,
) as dag:

    t1_empy = EmptyOperator(task_id="empty")
