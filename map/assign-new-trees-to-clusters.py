from datetime import timedelta

# The DAG object; we'll need this to instantiate a DAG
from airflow import DAG
# Operators; we need this to operate!
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from lib.assign_new_trees_to_cluster import assign_new_trees_to_cluster
# These args will get passed on to each operator
# You can override them on a per-task basis during operator initialization
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': days_ago(2),
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    # 'schedule_interval': '@hourly',
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
    'assign_tree_to_cluster',
    default_args=default_args,
    description='earing_export version 1',
    schedule_interval= '@hourly',
    start_date=days_ago(2),
    max_active_runs=1,
    catchup=False,
    tags=['map'],
) as dag:

    postgresConnId = "postgres_default"
    def assign_tree(ds, **kwargs):
        from lib.utils import print_time
        db = PostgresHook(postgres_conn_id=postgresConnId)
        conn = db.get_conn()  
        assign_new_trees_to_cluster(conn, True);

    t1 = BashOperator(
        task_id='print_date',
        bash_command='date',
    )

    t1 >> assign_tree
