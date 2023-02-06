from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.utils.dates import days_ago
from lib.utils import on_failure_callback

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': days_ago(2),
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'on_failure_callback': on_failure_callback, # needs to be set in default_args to work correctly: https://github.com/apache/airflow/issues/26760
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