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
from airflow.models import Variable
from lib.utils import on_failure_callback

# These args will get passed on to each operator
# You can override them on a per-task basis during operator initialization
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email': ['x6i4h0c1i4v9l5t6@greenstand.slack.com'],
    'email_on_failure': True,
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
    'stakeholder-migration',
    default_args=default_args,
    description='migrate entity to stakeholder',
    schedule_interval= None,
    start_date=datetime(2021, 1, 1),
    catchup=False,
    tags=['stakeholder','domain'],
) as dag:

    t1 = BashOperator(
        task_id='print_date',
        bash_command='date',
    )

    postgresConnId = "postgres_default"
    db = PostgresHook(postgres_conn_id=postgresConnId)
    conn = db.get_uri()
    # Airflow Variable DATABASE_PASSWORD is masked in the Airflow Variables UI and logs but not when actually used
    # https://airflow.apache.org/docs/apache-airflow/stable/administration-and-deployment/security/secrets/mask-sensitive-values.html
    # tested working in the https://github.com/Greenstand/treetracker-airflow-dags/blob/main/test1.py DAG
    environments = {
        'DATABASE_URL': "postgresql://" + Variable.get("DATABASE_LOGIN") + ":" + Variable.get("DATABASE_PASSWORD") + "@" + Variable.get("DATABASE"),
        'NODE_TLS_REJECT_UNAUTHORIZED': '0',
    }

    image = 'greenstand/domain-migration-scripts:1.0.1'
    namespace = 'airflow'
    
    migrate_stakeholders = KubernetesPodOperator(
        namespace=namespace,
        image=image,
        cmds=["sh", "-c", "node v1Tov2Migrations/migrate_stakeholders"],
        name="airflow-k8s-pod",
        do_xcom_push=False,
        is_delete_operator_pod=True,
        in_cluster=True,
        task_id="k8s-pod-migrate_stakeholders",
        get_logs=True,
        env_vars=environments
    )

    migrate_entity_ids_to_stakeholder = KubernetesPodOperator(
        namespace=namespace,
        image=image,
        cmds=["sh", "-c", "node v1Tov2Migrations/migrate_entity_ids_to_stakeholder"],
        name="airflow-k8s-pod",
        do_xcom_push=False,
        is_delete_operator_pod=True,
        in_cluster=True,
        task_id="k8s-pod-migrate_entity_ids_to_stakeholder",
        get_logs=True,
        env_vars=environments
    )

    migrate_stakeholders >> migrate_entity_ids_to_stakeholder >> t1
