import json
import pathlib
import airflow 
import requests
import requests.exceptions as requests_exceptions
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

dag = DAG(
    dag_id = 'download-rocket-launches',
    start_date = airflow.utils.dates.days_ago(12),
    schedule_interval = None,
)

download_launches = BashOperator(
    task_id = 'download-launches',
    bash_comand = "curl -o /tmp/launches.json -L 'https://ll.thespacedevs.com/2.0.0/launch/upcoming'",
    dag = dag,
)

def getPictures():
    pathlib.Path('/temp/images').mkdir(exist_ok= True, parents= True)

    with open('tmp/launches.json') as f:
        launches = json.load(f)
        images_urls = [launch['image'] for launch in launches['results']]
        for image_url in images_urls:
            try:
                response = requests.get(image_url)
                image_filename = image_url.split('/')[-1]
                target_file = f'/tmp/images/{image_filename}'
                with open(target_file,'wb') as ff:
                    ff.write(response.content)
                print(f'Downloaded {image_url} to {target_file}')
            except requests_exceptions.MissingSchema:
                print(f'{image_url} appears to be an invalid URL.')
            except requests_exceptions.ConnectionError:
                print(f'Could not connect to {image_url}')

get_pictures = PythonOperator(
    task_id = 'get_picture',
    python_collable = getPictures,
    dag = dag
)

notify = BashOperator(
    task_id = 'notify',
    bash_command = 'echo "there are now $(ls /tmp/images/ | wc -l) images."',
    dag = dag,
)

download_launches >> get_pictures >> notify 
