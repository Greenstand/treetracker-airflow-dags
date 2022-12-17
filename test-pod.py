from datetime import datetime, timedelta
from textwrap import dedent
from pprint import pprint
from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.providers.cncf.kubernetes.operators.kubernetes_pod import KubernetesPodOperator
import psycopg2.extras

# These args will get passed on to each operator
# You can override them on a per-task basis during operator initialization
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email': ['x6i4h0c1i4v9l5t6@greenstand.slack.com'],
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 2,
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
    'test-pod',
    default_args=default_args,
    description='test pod',
    schedule_interval= "@daily",
    start_date=datetime(2021, 1, 1),
    catchup=False,
    tags=['test'],
) as dag:

    t1 = BashOperator(
        task_id='print_date',
        bash_command='date',
    )

    postgresConnId = "postgres_default"

    write_xcom = KubernetesPodOperator(
        namespace="default",
        image="alpine",
        cmds=["sh", "-c", "mkdir -p /airflow/xcom/;echo '[1,2,3,4]' > /airflow/xcom/return.json"],
        name="write-xcom",
        do_xcom_push=True,
        is_delete_operator_pod=True,
        in_cluster=True,
        task_id="write-xcom",
        get_logs=True,
    )

    pod_task_xcom_result = BashOperator(
        bash_command="echo \"{{ task_instance.xcom_pull('write-xcom')[0] }}\"",
        task_id="pod_task_xcom_result",
    )

    write_xcom >> pod_task_xcom_result