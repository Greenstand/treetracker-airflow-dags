from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from datetime import datetime

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date':datetime(2022,10,14),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries' : 1,
}

dag = DAG('pre_populate_leader_board', default_args=default_args)

t1 = BashOperator(
    task_id='populate_leader_board',
    bash_command='python3 ./leaderboard.py', 
    dag=dag
)

t1