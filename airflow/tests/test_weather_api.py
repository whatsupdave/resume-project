import sys
import os
sys.path.append(os.path.join(os.path.dirname(__file__), '..', 'dags'))
from weather_api import transform_weather_data

def test_transform_weather_data():
    print()