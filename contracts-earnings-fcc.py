from datetime import datetime, timedelta
from textwrap import dedent
from pprint import pprint
from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
import psycopg2.extras
from lib.contracts_earnings_fcc import contract_earnings_fcc

from lib.planter_entity import planter_entity

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
    'contract-earnings-fcc',
    default_args=default_args,
    description='Calculate earnings for FCC planters',
    schedule_interval= '@daily',
    start_date=datetime(2021, 1, 1),
    catchup=False,
    tags=['earnings', 'freetown'],
) as dag:

    t1 = BashOperator(
        task_id='print_date',
        bash_command='date',
    )

    postgresConnId = "postgres_default"

    def create_new_person_records(ds, **kwargs):
        db = PostgresHook(postgres_conn_id=postgresConnId)
        conn = db.get_conn()  
        planter_entity(conn)
        return 1


    def earnings_report(ds, **kwargs):
        db = PostgresHook(postgres_conn_id=postgresConnId)
        conn = db.get_conn()
        print("db:", conn)
        contract_earnings_fcc(conn)
        return 1

    create_new_person_records = PythonOperator(
        task_id='create_new_person_records',
        python_callable=create_new_person_records,
        )

    earnings_report = PythonOperator(
        task_id='earnings_report',
        python_callable=earnings_report,
        )


    create_new_person_records >> earnings_report >> t1
