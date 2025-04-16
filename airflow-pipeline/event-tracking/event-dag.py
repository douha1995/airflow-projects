import datetime as dt
from pathlib import Path
import pandas as pd
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

dag = DAG(
    dag_id="01_unscheduled",
    start_date=dt.datetime(2025, 2, 1),
    end_date=dt.datetime(2025, 2, 30),
    #schedule_interval="@daily", 
    schedule_interval=dt.timedelta(days=3), 

)

fetch_events = BashOperator(
    task_id="fetch_events",
    bash_command=(
        "mkdir -p /data/events && "
        "curl -o /data/events/events{{ds}}.json "
        "https://localhost:5000/events" 
        "start_date={{ds}}&"
        "end_date"={{next_ds}}
    ), 
    dag=dag,
)

def _calculate_stats(**context):
    """Calculates event statistics."""
    input_path = context["templates_dict"]["input_path"]
    output_path = context["templates_dict"]["output_path"]
    
    Path(output_path).parent.mkdir(exist_ok=True)
    events = pd.read_json(input_path) 
    stats = events.groupby(["date", "user"]).size().reset_index()
    stats.to_csv(output_path, index=False) 
 
 
calculate_stats = PythonOperator(
    task_id="calculate_stats",
    python_callable=_calculate_stats,
    templates_dict={
        "input_path": "/data/events/{{ds}}",
        "output_path": "/data/stats/{{ds}}.csv",
    },
    dag=dag,
)

fetch_events >> calculate_stats


