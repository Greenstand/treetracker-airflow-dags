# updated at Tue May 31 2022 09:37:26 GMT+0800
from datetime import datetime, timedelta
from textwrap import dedent
from pprint import pprint
from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.models import Variable
import psycopg2.extras
from lib.messaging import create_authors
from lib.utils import on_failure_callback

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
    'on_failure_callback': on_failure_callback, # needs to be set in default_args to work correctly: https://github.com/apache/airflow/issues/26760
    # 'on_success_callback': some_other_function,
    # 'on_retry_callback': another_function,
    # 'sla_miss_callback': yet_another_function,
    # 'trigger_rule': 'all_success'
}
with DAG(
    'create-authors',
    default_args=default_args,
    description='Create messaging system users for approved organizations',
    schedule_interval='@hourly',
    start_date=datetime(2021, 1, 1),
    catchup=False,
    tags=['messaging'],
) as dag:

    t1 = BashOperator(
        task_id='print_date',
        bash_command='date',
    )

    postgresConnId = "postgres_default"

    def create_authors_wrap(ds, **kwargs):
        DISABLE_ORGANIZATION_FILTER = Variable.get("AUTHOR_CREATION_DISABLE_ORGANIZATION_FILTER", default_var=None)
        db = PostgresHook(postgres_conn_id=postgresConnId)
        conn = db.get_conn()  
        create_authors(conn, DISABLE_ORGANIZATION_FILTER)
        return 1

    create_authors_task = PythonOperator(
        task_id='create_authors',
        python_callable=create_authors_wrap,
        )

    create_authors_task >> t1

    
