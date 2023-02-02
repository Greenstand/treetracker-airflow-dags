from datetime import datetime, timedelta
from textwrap import dedent
from pprint import pprint
import time
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
import psycopg2.extras
from lib.contracts_earnings_fcc import contract_earnings_fcc
from lib.pre_request import pre_request

from lib.planter_entity import planter_entity
from airflow.models import Variable
from lib.pre_request_map_clusters import pre_request_map_clusters

# These args will get passed on to each operator
# You can override them on a per-task basis during operator initialization
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 20,
    'retry_delay': timedelta(seconds=1),
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
    'pre_request_map_client',
    default_args=default_args,
    description='Rre-request the all map client pages, version 1.0',
    schedule_interval= '*/5 * * * *',
    start_date=datetime(2021, 1, 1),
    max_active_runs=1,
    catchup=False,
    tags=['reporting', 'map'],
) as dag:

    t1 = BashOperator(
        task_id='print_date',
        bash_command='date',
    )

    def pre_request_job(ds, **kwargs):
        print("do pre request job:")
        prefix = "http://treetracker-web-map-client-beta-ambassador-svc.webmap.svc.cluster.local"
        urls = [
            "/top", 
            "/organizations/fcctrees",
            "/organizations/1642",
            "/organizations/freetown",
            "/organizations/178",
            "/organizations/TheHaitiTreeProject",
            "/organizations/194",
            "/organizations/rejuvenate_umhlaba",
            "/organizations/552",
            "/organizations/TreesThatFeedFoundation",
            "/organizations/952",
            "/organizations/echo",
            "/organizations/775",
            "/organizations/bosque-la-tigra",
            "/organizations/1332",
            "/organizations/bondy",
            "/organizations/1331",
            "/organizations/forestfocus",
            "/organizations/1341",
            "/organizations/inception",
            "/organizations/1335",
            "/organizations/piesces",
            "/organizations/1447",
            "/organizations/leadfoundation",
            "/organizations/11",
            "/wallets/FinorX",
        ]
        for url in urls:
            urlreal = f"{prefix}/{url}"
            print(f"request: {urlreal}")
            begin_time = datetime.now()
            pre_request(urlreal)
            end_time = datetime.now()
            print(f"request: took {end_time - begin_time}")
            # sleep for 1 minute
            time.sleep(60)

    pre_request_map_cluster_job = PythonOperator(
        task_id='pre_request_map_client',
        python_callable=pre_request_job,
        )

    pre_request_map_cluster_job >> t1

