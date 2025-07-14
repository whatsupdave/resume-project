from airflow import DAG

from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.operators.s3 import S3CreateObjectOperator
from datetime import datetime, timedelta
from typing import Dict, List, Tuple, Any, Optional, Union

import requests
from airflow.hooks.base import BaseHook


def get_lat_lon_for_city(city: str = "Vilnius") -> tuple[float, float]:
    """
    Gets latitude and longitude for the specified city.

    Args:
        city: City name to get coordinates for (default: "Vilnius")

    Returns:
        tuple[float, float]: Latitude and longitude coordinates
    """

    conn = BaseHook.get_connection(f"openweathermap_default")
    api_key = conn.password
    host = conn.host
    endpoint = f"http://{host}/geo/1.0/direct?q={city}&limit=1&appid={api_key}"

    try:
        response = requests.get(endpoint)
        response.raise_for_status()
    except requests.exceptions.RequestException as e:
        raise RuntimeError(f"API Call Error: {e}") from e

    data = response.json()
    if not data:
        raise ValueError(f"No location found for {city}")

    lat = data[0]["lat"]
    lon = data[0]["lon"]

    return lat, lon


def get_weather_for_city(**context) -> str:
    """
    Gets weather forecast for the city using coordinates from previous task.

    Args:
        **context: Airflow context dictionary

    Returns:
        str: JSON string containing weather forecast data
    """

    lat, lon = context["ti"].xcom_pull(key="return_value", task_ids="get_lat_lon")

    conn = BaseHook.get_connection("openweathermap_default")
    api_key = conn.password
    host = conn.host

    endpoint = (
        f"http://{host}/data/2.5/forecast?cnt=10&lat={lat}&lon={lon}&appid={api_key}"
    )

    try:
        response = requests.get(url=endpoint)
        response.raise_for_status()
    except requests.exceptions.RequestException as e:
        raise RuntimeError(f"API Call Error: {e}") from e

    weather_data = response.text

    return weather_data

def transform_weather_data():
    print()

# DAG Config
DAG_NAME = "weather_monitor"
DAG_DESCRIPTION = "Gets weather data"

# S3 Config
S3_BUCKET_NAME = "openweather-api-data"
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

    get_lat_lon = PythonOperator(
        task_id="get_lat_lon",
        python_callable=get_lat_lon_for_city,
    )

    get_weather_data = PythonOperator(
        task_id="get_weather_for_city",
        python_callable=get_weather_for_city,
    )

    upload_raw_data_to_s3 = S3CreateObjectOperator(
        task_id='upload_raw_data_to_s3',
        s3_bucket=S3_BUCKET_NAME,
        s3_key='raw_data/{{ data_interval_start | ds }}_Vilnius.json',
        data="{{ ti.xcom_pull(task_ids='get_weather_for_city', key='return_value') }}",
        replace=True,
        aws_conn_id="aws_default"
    )

    get_lat_lon >> get_weather_data >> upload_raw_data_to_s3
