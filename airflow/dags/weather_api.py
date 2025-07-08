from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator
from datetime import datetime, timedelta

# DAG Config
DAG_NAME = "weather_monitor"
DAG_DESCRIPTION = "Gets weather data"

# S3 Config
s3_bucket_name = ""
s3_folder_name = ""
s3_file_name = ""

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
    tags=["daily", "alerting"],
    start_date=datetime(2023, 9, 25),
    schedule="0 5 * * *",
    max_active_runs=1,
    catchup=False,
) as dag:

    t1_get_data_and_write = EmptyOperator(
        task_id="get_data_and_write_to_s3",
        python_callable="get_data_and_write_to_s3",
        op_kwargs={"todays_date": "{{ data_interval_end | ds_nodash }}"},
    )
