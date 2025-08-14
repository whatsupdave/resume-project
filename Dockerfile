FROM apache/airflow:2.9.2
COPY airflow/requirements/requirements.txt /requirements.txt
RUN pip install -r /requirements.txt