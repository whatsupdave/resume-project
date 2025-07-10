from airflow import DAG

# from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator
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
    endpoint = (
        f"http://{host}/geo/1.0/direct?q={city}&limit=1&appid={api_key}"
    )

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


def get_weather_for_city(lat: str, lon: str) -> Dict[str, any]:
    """
    Gets latitude and longitude for the specified city.

    Args:
        lat: city latitude
        lon: city longitude

    Returns:
        Dict[str, any]: Dictionary with weather values for city
    """
    conn = BaseHook.get_connection(f"openweathermap_default")
    api_key = conn.password
    host = conn.host
    endpoint = f"http://{host}/data/2.5/forecast?cnt=10&lat={lat}&lon={lon}&appid={api_key}"

    try:
        response = requests.get(url=endpoint)
        response.raise_for_status()
    except requests.exceptions.RequestException as e:
        raise RuntimeError(f"API Call Error: {e}") from e

    return response.json()


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
