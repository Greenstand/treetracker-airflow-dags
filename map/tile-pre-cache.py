from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.utils.dates import days_ago

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': days_ago(2),
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0
}

dag = DAG(
    'tile-pre-request-container',
    default_args=default_args,
    schedule_interval='*/5 * * * *',
    description='cURLs specific zoom levels and organization maps to refresh cache storage for most visited map views'
)

t1 = BashOperator(
    task_id='tile-pre-request-container',
    bash_command='./scripts/precache.sh',
    dag=dag
)