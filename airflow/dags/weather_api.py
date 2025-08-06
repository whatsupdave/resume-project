from airflow import DAG

from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.operators.s3 import S3CreateObjectOperator
from airflow.providers.snowflake.transfers.copy_into_snowflake import (
    CopyFromExternalStageToSnowflakeOperator,
)
from datetime import datetime, timedelta
from typing import Dict, List, Tuple, Any, Optional, Union
import json
import pandas as pd

import requests
from airflow.hooks.base import BaseHook
from airflow.utils.task_group import TaskGroup
from airflow.exceptions import AirflowException


# DAG Config
DAG_NAME = "weather_monitor"
DAG_DESCRIPTION = "Gets weather data"

# S3 Config
S3_BUCKET_NAME = "openweather-api-data"
S3_RAW_DATA_FOLDER = "raw_data"
S3_CLEANED_DATA_FOLDER = "transformed_data"
S3_FILE_NAME = ""


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

    lat, lon = context["ti"].xcom_pull(
        key="return_value", task_ids="extract_and_load_data.get_lat_lon"
    )

    conn = BaseHook.get_connection("openweathermap_default")
    api_key = conn.password
    host = conn.host

    endpoint = f"http://{host}/data/2.5/forecast?cnt=10&lat={lat}&lon={lon}&units=metric&appid={api_key}"

    try:
        response = requests.get(url=endpoint)
        response.raise_for_status()
    except requests.exceptions.RequestException as e:
        raise RuntimeError(f"API Call Error: {e}") from e

    weather_data = response.text

    return weather_data


def transform_weather_data(**context: Any) -> str:
    """
    Transforms weather forecast for the city using data from previous task.

    Args:
        **context: Airflow context dictionary

    Returns:
        str: Weather data formatted as CSV string with columns:
             - dt: Unix timestamp
             - dt_txt: Human-readable datetime string
             - main_temp: Temperature value
             - weather_description: Weather condition description
             - city_name: Name of the city
             - city_country: Name of the country
    """
    try:
        weather_data = context["ti"].xcom_pull(
            key="return_value", task_ids="extract_and_load_data.get_weather_for_city"
        )

        if not weather_data:
            raise AirflowException("No weather data received from upstream task")

        dataset = json.loads(weather_data)

        df = pd.json_normalize(dataset["list"])

        df["weather_description"] = df["weather"].apply(
            lambda x: x[0]["description"] if x else None
        )
        df["weather_main"] = df["weather"].apply(lambda x: x[0]["main"] if x else None)

        city_info = dataset["city"]
        df["city_name"] = city_info["name"]
        df["city_country"] = city_info["country"]

        df = df[
            [
                "dt",
                "dt_txt",
                "main.temp",
                "weather_description",
                "city_name",
                "city_country",
            ]
        ].rename(columns={"main.temp": "main_temp"})

        csv = df.to_csv(index=False)

        return csv

    except Exception as e:
        raise AirflowException(f"Weather data transformation failed: {e}")


def extract_task_group() -> TaskGroup:
    """
    Gets required parameters needed to  call Openweathermap API, gets the data & uploads raw data to S3 bucker

    Returns:
        TaskGroup containing Openweathermap API data processing tasks
    """
    with TaskGroup(group_id="extract_and_load_data") as extract_tasks:
        get_lat_lon = PythonOperator(
            task_id="get_lat_lon",
            python_callable=get_lat_lon_for_city,
        )

        get_weather_data = PythonOperator(
            task_id="get_weather_for_city",
            python_callable=get_weather_for_city,
        )

        load_raw_data_to_s3 = S3CreateObjectOperator(
            task_id="load_raw_data_to_s3",
            s3_bucket=S3_BUCKET_NAME,
            s3_key="raw_data/{{ data_interval_start | ds }}_Vilnius.json",
            data="{{ ti.xcom_pull(task_ids='get_weather_for_city', key='return_value') }}",
            replace=True,
            aws_conn_id="aws_default",
        )

        get_lat_lon >> get_weather_data >> load_raw_data_to_s3

    return extract_tasks


def load_task_group() -> TaskGroup:
    """
    Gets cleaned data from upstream task XCOM, then loads to Amazon S3 and Snowflake weather_data bucket

    Returns:
        TaskGroup containing cleaned data load tasks
    """
    with TaskGroup(group_id="load_data_to_s3_and_snowflake") as load_tasks:
        load_transformed_data_to_s3 = S3CreateObjectOperator(
            task_id="load_transformed_data_to_s3",
            s3_bucket=S3_BUCKET_NAME,
            s3_key="transformed_data/{{ data_interval_start | ds }}_Vilnius.csv",
            data="{{ ti.xcom_pull(task_ids='transform_weather_data', key='return_value') }}",
            replace=True,
            aws_conn_id="aws_default",
        )

        load_into_table = CopyFromExternalStageToSnowflakeOperator(
            task_id="load_clean_data_into_table",
            snowflake_conn_id="snowflake_conn_id",
            files=["{{ data_interval_start | ds }}_Vilnius.csv"],
            table="weather_data",
            stage="openweather_transformed_stage",
            file_format="(type='CSV', field_delimiter=',', skip_header=1)",
            dag=dag,
        )

        load_transformed_data_to_s3 >> load_into_table

    return load_tasks


default_args = {
    "owner": "Eivydas",
    "depends_on_past": False,
    "start_date": datetime(2025, 7, 7),
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
    start_date=datetime(2025, 7, 7),
    schedule="0 5 * * *",
    max_active_runs=1,
    catchup=False,
) as dag:

    extract_and_upload_raw_data_to_s3 = extract_task_group()

    transform_data = PythonOperator(
        task_id="transform_weather_data",
        python_callable=transform_weather_data,
    )

    load_data_to_s3_and_snowflake = load_task_group()

    (
        extract_and_upload_raw_data_to_s3
        >> transform_data
        >> load_data_to_s3_and_snowflake
    )
