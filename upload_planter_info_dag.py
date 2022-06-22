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
from airflow.models import Variable
import lib.utils
import lib.upload_planter_info

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
    'earing_export',
    default_args=default_args,
    description='earing_export version 1',
    schedule_interval='@daily',
    start_date=days_ago(2),
    catchup=False,
    tags=['CKAN', 'freetown'],
) as dag:

    t1 = BashOperator(
        task_id='print_date',
        bash_command='date',
    )

    postgresConnId = "postgres_default"

    def earing_export_wrap(ds, **kwargs):
      from lib.utils import print_time
      db = PostgresHook(postgres_conn_id=postgresConnId)
      conn = db.get_conn()  
      try:
          date = datetime.now().strftime("%Y-%m-%d")
          print("date:", date)
          FTP_HOST = Variable.get("FTP_HOST")
          FTP_USER = Variable.get("FTP_USER")
          FTP_PASSWORD = Variable.get("FTP_PASSWORD")
          lib.upload_planter_info.upload_planter_info(
            conn, 
            FTP_HOST,
            FTP_USER,
            FTP_PASSWORD,
            True
          )
          return 0
      except Exception as e:
          print("get error when execute ftp upload:", e)
          raise ValueError('Error executing')

    earing_export_task = PythonOperator(
        task_id='earing_export',
        python_callable=earing_export_wrap,
        )


    earing_export_task >> t1
