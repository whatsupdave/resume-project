import json
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional, Tuple, Union

import pandas as pd
from pydantic import BaseModel, ValidationError

from airflow import DAG
from airflow.exceptions import AirflowException
from airflow.operators.python import PythonOperator
from airflow.utils.task_group import TaskGroup

from airflow.providers.amazon.aws.operators.s3 import S3CreateObjectOperator
from airflow.providers.http.operators.http import HttpOperator
from airflow.providers.snowflake.transfers.copy_into_snowflake import (
    CopyFromExternalStageToSnowflakeOperator,
)

# DAG Config
DAG_NAME = "weather_monitor"
DAG_DESCRIPTION = "Gets weather data"

# S3 Config
S3_BUCKET_NAME = "openweather-api-data"
S3_RAW_DATA_FOLDER = "raw_data"
S3_CLEANED_DATA_FOLDER = "transformed_data"
S3_FILE_NAME = ""


class ColumnsCheck(BaseModel):
    city_country: str
    city_name: str
    dt: int
    dt_txt: str
    main_temp: float
    weather_description: str


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

        if len(df) == 0:
            raise AirflowException("No weather records in API response")

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

        temp_min, temp_max = df["main_temp"].min(), df["main_temp"].max()
        if temp_min < -50 or temp_max > 60:
            raise AirflowException(
                f"Unrealistic temperatures: {temp_min}°C to {temp_max}°C"
            )

        for _, row in df.iterrows():
            try:
                ColumnsCheck(**row.to_dict())
            except ValidationError as e:
                raise AirflowException(f"Row validation failed: {e}")

        csv = df.to_csv(index=False)
        return csv

    except Exception as e:
        raise AirflowException(f"Weather data transformation failed: {e}")


def extract_task_group(city: str) -> TaskGroup:
    """
    Gets required parameters needed to  call Openweathermap API, does the call to the API & uploads raw data to S3 bucker
    Args:
        Name of the city to get weather data. Default is Vilnius

    Returns:
        TaskGroup containing Openweathermap API data processing tasks
    """
    with TaskGroup(group_id=f"{city}_extract_and_load_data") as extract_tasks:

        get_lat_lon = HttpOperator(
            http_conn_id="openweathermap_default",
            task_id="get_lat_lon",
            method="GET",
            endpoint="geo/1.0/direct",
            response_filter=lambda response: [
                response.json()[0]["lat"],
                response.json()[0]["lon"],
            ],
            data={
                "q": f"{city}",
                "limit": 1,
                "appid": "{{ conn.openweathermap_default.password }}",
            },
            do_xcom_push=True,
        )

        get_weather_data = HttpOperator(
            http_conn_id="openweathermap_default",
            task_id="get_weather_for_city",
            method="GET",
            endpoint="data/2.5/forecast",
            data={
                "cnt": 10,
                "lat": f"{{{{ ti.xcom_pull(key='return_value', task_ids='{city}.{city}_extract_and_load_data.get_lat_lon')[0] }}}}",
                "lon": f"{{{{ ti.xcom_pull(key='return_value', task_ids='{city}.{city}_extract_and_load_data.get_lat_lon')[1] }}}}",
                "units": "metric",
                "appid": "{{ conn.openweathermap_default.password }}",
            },
            do_xcom_push=True,
        )

        load_raw_data_to_s3 = S3CreateObjectOperator(
            task_id="load_raw_data_to_s3",
            s3_bucket=S3_BUCKET_NAME,
            s3_key=S3_RAW_DATA_FOLDER
            + "{{ data_interval_end | ds }}"
            + f"_{city}.json",
            data="{{ ti.xcom_pull(task_ids='get_weather_for_city', key='return_value') }}",
            replace=True,
            aws_conn_id="aws_default",
        )

        get_lat_lon >> get_weather_data >> load_raw_data_to_s3

    return extract_tasks


def load_task_group(city: str) -> TaskGroup:
    """
    Gets cleaned data from upstream task XCOM, then loads to Amazon S3 and Snowflake weather_data bucket

    Returns:
        TaskGroup containing cleaned data load tasks
    """
    with TaskGroup(group_id=f"{city}_load_data_to_s3_and_snowflake") as load_tasks:
        load_transformed_data_to_s3 = S3CreateObjectOperator(
            task_id="load_transformed_data_to_s3",
            s3_bucket=S3_BUCKET_NAME,
            # s3_key="transformed_data/{{ data_interval_end | ds }}_Vilnius.csv",
            s3_key=S3_CLEANED_DATA_FOLDER
            + "{{ data_interval_end | ds }}_"
            + f"_{city}.csv",
            data="{{ ti.xcom_pull(task_ids='transform_weather_data', key='return_value') }}",
            replace=True,
            aws_conn_id="aws_default",
        )

        load_into_table = CopyFromExternalStageToSnowflakeOperator(
            task_id="load_clean_data_into_table",
            snowflake_conn_id="snowflake_conn_id",
            files=["{{ data_interval_end | ds }}_Vilnius.csv"],
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

    cities = ["Vilnius", "Riga", "Tallinn"]

    for city in cities:
        with TaskGroup(group_id=f"{city}") as source_group:
            extract_and_upload_raw_data_to_s3 = extract_task_group(city)

            transform_data = PythonOperator(
                task_id=f"{city}_transform_weather_data",
                python_callable=transform_weather_data,
            )

            load_data_to_s3_and_snowflake = load_task_group(city)

        (
            extract_and_upload_raw_data_to_s3
            >> transform_data
            >> load_data_to_s3_and_snowflake
        )
