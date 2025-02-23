from airflow import DAG
from datetime import datetime
from airflow.providers.http.sensors.http import HttpSensor
from airflow.providers.http.operators.http import SimpleHttpOperator
import json
from airflow.operators.python import PythonOperator


def transform_weather_data():
    








default_args = {
    'owner':'Abdelrahman_DE',
    'depends_on_past':False,
    'start_date':datetime(2025, 2, 20),
    'email_on_failure':False,
    'retries':None,
} 

with DAG(
    dag_id='weather dag'
    default_args=default_args,
    schedule_interval='@daily',
    catchup=False,
) as dag:
    
    # is_weather_api_ready = HttpSensor(
    #     task_id='is_weather_api_ready',
    #     http_conn_id='weather_api',
    #     endpoint='api.openweathermap.org/data/2.5/weather?q=London,uk&APPID=307d7a184f724cf89e4d3f08449b918b',
        
    # )
    
    ingest_weather_data = SimpleHttpOperator(
        task_id='ingest_weather_data',
        endpoint='api.openweathermap.org/data/2.5/weather?q=London,uk&APPID=307d7a184f724cf89e4d3f08449b918b',
        method='GET',
        response_check=lambda r: json.load(r.text),
        log_response=True,
    )
    
    transform_weather_data = PythonOperator(
        task_id='transform_weather_data',
        python_callable='transform_weather_data'
    )
    
    load_weather_data_to_s3_bucket = PythonOperator(
        task_id='load_weather_data_to_s3_bucket',
        python_callable='load_data_to_s3_bucket',
        
    )