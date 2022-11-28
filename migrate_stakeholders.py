from datetime import datetime, timedelta
from textwrap import dedent
from pprint import pprint
from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.operators.bash import BashOperator
from airflow.operators.docker_operator import DockerOperator
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
    'migrate-stakeholders',
    default_args=default_args,
    description='Migrate entities from legacy db',
    schedule_interval= None,
    catchup=False,
    tags=['migration'],
) as dag:

t1 = BashOperator(
    task_id='print_date',
    bash_command='date',
)

postgresConnId = "postgres_default"
db = PostgresHook(postgres_conn_id=postgresConnId)
conn = db.get_conn()  

t2 = DockerOperator(
    task_id='migrate_stakeholders',
    image='greenstand/domain-migration-scripts:1.0.0',
    api_version='auto',
    auto_remove=True,
    environment={
        'DATABASE_URL': conn
    },
    command='npm run migrate-stakeholders',
    docker_url='unix://var/run/docker.sock',
    network_mode='bridge'
)


t2 >> t1
