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
    # 'on_failure_callback': some_function,
    # 'on_success_callback': some_other_function,
    # 'on_retry_callback': another_function,
    # 'sla_miss_callback': yet_another_function,
    # 'trigger_rule': 'all_success'
}
with DAG(
    'domain-migration',
    default_args=default_args,
    description='migrate legacy data to new domain db',
    schedule_interval= "@daily",
    start_date=datetime(2021, 1, 1),
    catchup=False,
    tags=['db','domain'],
) as dag:

    t1 = BashOperator(
        task_id='print_date',
        bash_command='date',
    )

    postgresConnId = "postgres_default"
    db = PostgresHook(postgres_conn_id=postgresConnId)
    conn = db.get_uri()  
    environments = {
        'DATABASE_URL': Variable.get("DATABASE_URL"),
        'NODE_TLS_REJECT_UNAUTHORIZED': '0',
    }

    image = 'greenstand/domain-migration-scripts:1.0.0'

    namespace = 'airflow'

    migrate_trees = KubernetesPodOperator(
        namespace=namespace,
        image=image,
        cmds=["sh", "-c", "node v1Tov2Migrations/migrate_trees"],
        name="airflow-k8s-pod",
        do_xcom_push=False,
        is_delete_operator_pod=True,
        in_cluster=True,
        task_id="k8s-pod-trees",
        get_logs=True,
        env_vars=environments
    )

    migrate_planter_info = KubernetesPodOperator(
        namespace=namespace,
        image=image,
        cmds=["sh", "-c", "node v1Tov2Migrations/migrate_planter_info"],
        name="airflow-k8s-pod",
        do_xcom_push=False,
        is_delete_operator_pod=True,
        in_cluster=True,
        task_id="k8s-pod-planter",
        get_logs=True,
        env_vars=environments
    )

    
    migrate_device_configurations = KubernetesPodOperator(
        namespace=namespace,
        image=image,
        cmds=["sh", "-c", "node v1Tov2Migrations/migrate_device_configurations"],
        name="airflow-k8s-pod",
        do_xcom_push=False,
        is_delete_operator_pod=True,
        in_cluster=True,
        task_id="k8s-pod-migrate_device_configurations",
        get_logs=True,
        env_vars=environments
    )


    migrate_species = KubernetesPodOperator(
        namespace=namespace,
        image=image,
        cmds=["sh", "-c", "node v1Tov2Migrations/migrate_species"],
        name="airflow-k8s-pod",
        do_xcom_push=False,
        is_delete_operator_pod=True,
        in_cluster=True,
        task_id="k8s-pod-migrate_species",
        get_logs=True,
        env_vars=environments
    )

    
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

    
    migrate_raw_capture = KubernetesPodOperator(
        namespace=namespace,
        image=image,
        cmds=["sh", "-c", "node v1Tov2Migrations/migrate_raw_capture"],
        name="airflow-k8s-pod",
        do_xcom_push=False,
        is_delete_operator_pod=True,
        in_cluster=True,
        task_id="k8s-pod-migrate_raw_capture",
        get_logs=True,
        env_vars=environments
    )

    migrate_approved_capture = KubernetesPodOperator(
        namespace=namespace,
        image=image,
        cmds=["sh", "-c", "node v1Tov2Migrations/migrate_approved_capture"],
        name="airflow-k8s-pod",
        do_xcom_push=False,
        is_delete_operator_pod=True,
        in_cluster=True,
        task_id="k8s-pod-migrate_approved_capture",
        get_logs=True,
        env_vars=environments
    )


    migrate_planter_info >> migrate_raw_capture

    migrate_device_configurations >> migrate_raw_capture

    migrate_stakeholders >> migrate_entity_ids_to_stakeholder >> migrate_raw_capture

    migrate_species >> migrate_raw_capture

    migrate_raw_capture >> migrate_approved_capture >> migrate_trees >> t1
