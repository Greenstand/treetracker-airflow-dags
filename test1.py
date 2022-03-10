from datetime import datetime, timedelta
from textwrap import dedent
from pprint import pprint
from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
import psycopg2.extras
from airflow.utils.dates import days_ago

# These args will get passed on to each operator
# You can override them on a per-task basis during operator initialization
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0,
    'retry_delay': timedelta(minutes=5),
    # 'queue': 'bash_queue',
    # 'pool': 'backfill',
    # 'priority_weight': 10,
    # 'end_date': datetime(2016, 1, 1),
    # 'wait_for_downstream': False,
    # 'dag': dag,
    # 'sla': timedelta(hours=2),
    # 'execution_timeout': timedelta(seconds=300),
    # 'on_failure_callback': some_function,
    # 'on_success_callback': some_other_function,
    # 'on_retry_callback': another_function,
    # 'sla_miss_callback': yet_another_function,
    # 'trigger_rule': 'all_success'
}
with DAG(
    'test1',
    default_args=default_args,
    description='to test update',
    schedule_interval=None,
    start_date=days_ago(2),
    catchup=False,
    tags=['earnings', 'freetown'],
) as dag:

    t1 = BashOperator(
        task_id='print_date',
        bash_command='date',
    )

    postgresConnId = "postgres_default"

    def create_new_person_records(ds, **kwargs):
      print("xxxxxxxxxxx ") 
      # import print_time from lib/utils.py
      from lib.utils import print_time

      print_time("xxxhahah")
        # db = PostgresHook(postgres_conn_id=postgresConnId)
        # conn = db.get_conn()  
        # cursor = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        # try:
        #     # conn.commit()
        #     print("xxxxxxxxxxxxxxxxxxxxxxxxxx")
        #     return 0
        # except Exception as e:
        #     print("get error when exec SQL:", e)
        #     print("SQL result:", updateCursor.query)
        #     raise ValueError('Error executing query')
        #     return 1

    create_new_person_records = PythonOperator(
        task_id='create_new_person_records',
        python_callable=create_new_person_records,
        )


    create_new_person_records >> t1
