from datetime import datetime, timedelta
from textwrap import dedent
from pprint import pprint
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
    'pre_request_map_clusters',
    default_args=default_args,
    description='Rre-request the all map cluster version 2.1',
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
        pre_request_map_clusters("http://treetracker-tile-server.tile-server.svc.cluster.local")
        return 1

    pre_request_map_cluster_job = PythonOperator(
        task_id='pre_request_map_cluster',
        python_callable=pre_request_job,
        )

    pre_request_map_cluster_job >> t1

