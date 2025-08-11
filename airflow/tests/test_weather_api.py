"""
Unit tests for weather_api DAG functions.

This module contains pytest tests for the transform_weather_data function,
including success and failure scenarios with mocked XCom data.
"""

import json
import os
import sys

import pytest

sys.path.append(os.path.join(os.path.dirname(__file__), "..", "dags"))
from weather_api import transform_weather_data  # pylint: disable=import-error,wrong-import-position

from airflow.exceptions import AirflowException


@pytest.mark.parametrize(
    "mock_data,expected",
    [
        (
            json.dumps(
                {
                    "cod": "200",
                    "message": 0,
                    "cnt": 10,
                    "list": [
                        {
                            "dt": 1754632800,
                            "main": {
                                "temp": 15.57,
                                "feels_like": 15.19,
                                "temp_min": 15.57,
                                "temp_max": 17.02,
                                "pressure": 1020,
                                "sea_level": 1020,
                                "grnd_level": 1002,
                                "humidity": 77,
                                "temp_kf": -1.45,
                            },
                            "weather": [
                                {
                                    "id": 800,
                                    "main": "Clear",
                                    "description": "clear sky",
                                    "icon": "01d",
                                }
                            ],
                            "clouds": {"all": 0},
                            "wind": {"speed": 2.77, "deg": 186, "gust": 5.8},
                            "visibility": 10000,
                            "pop": 0,
                            "sys": {"pod": "d"},
                            "dt_txt": "2025-08-08 06:00:00",
                        }
                    ],
                    "city": {
                        "id": 593116,
                        "name": "Vilnius",
                        "coord": {"lat": 54.687, "lon": 25.2829},
                        "country": "LT",
                        "population": 542366,
                        "timezone": 10800,
                        "sunrise": 1754620903,
                        "sunset": 1754676443,
                    },
                }
            ),
            "success",
        ),
        ("None", "exception"),
    ],
)
def test_transform_weather_data(mocker, mock_data, expected):
    """
    Test transform_weather_data function with various scenarios.
    
    Args:
        mocker: Pytest mock fixture
        mock_data: Mocked API response data
        expected: Expected test outcome ('success' or 'exception')
    """
    mock_ti = mocker.Mock()
    mock_ti.xcom_pull.return_value = mock_data
    mock_context = {"ti": mock_ti}
    if expected == "exception":
        with pytest.raises(AirflowException):
            transform_weather_data(city='Vilnius', **mock_context)
    else:
        result_csv = transform_weather_data(city='Vilnius', **mock_context)
        assert "main_temp" in result_csv
        assert len(result_csv.split("\n")) > 1
