from airflow.decorators import dag, task
from airflow.operators.email_operator import EmailOperator

from datetime import datetime
from typing import Dict
import requests
import logging

API = "https://api.open-meteo.com/v1/forecast?latitude=52.52&longitude=13.41&current_weather=True"
@dag(schedule_interval='@daily', start_date=datetime(2019, 12, 2), catchup=False)
def store_weather_alert():

    @task(task_id='get_weather', retries=2)
    def get_current_weather() -> Dict[str, float]:
        return requests.get(API).json()['current_weather']

    @task(multiple_outputs=True)
    def compute_data(response: Dict[str, float]) -> Dict[str, float]:
        logging.info(response)
        return {'temperature': response['temperature'],
                'windspeed': response['windspeed']}

    @task
    def store_data(data: Dict[str, float]):
        logging.info(f"Store: {data['temperature']} with change {data['windspeed']}")

    email_alert = EmailOperator(
        task_id='email_alert',
        to='hello@example.com',
        subject='Dag completed',
        html_content='the dag has finished'
    )

    store_data(compute_data(get_current_weather())) >> email_alert

dag = store_weather_alert()