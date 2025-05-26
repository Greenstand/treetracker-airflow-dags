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
    'migrate-captures',
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
    # Airflow Variable DATABASE_PASSWORD is masked in the Airflow Variables UI and logs but not when actually used
    # https://airflow.apache.org/docs/apache-airflow/stable/administration-and-deployment/security/secrets/mask-sensitive-values.html
    # tested working in the https://github.com/Greenstand/treetracker-airflow-dags/blob/main/test1.py DAG
    environments = {
        'DATABASE_URL': "postgresql://" + Variable.get("DATABASE_LOGIN") + ":" + Variable.get("DATABASE_PASSWORD") + "@" + Variable.get("DATABASE"),
        'NODE_TLS_REJECT_UNAUTHORIZED': '0',
    }

    image = 'greenstand/domain-migration-scripts:1.2.8'
    namespace = 'airflow'
    
    migrate_trees = KubernetesPodOperator(
        namespace=namespace,
        image=image,
        cmds=["sh", "-c", "npm run migrate-trees"],
        name="airflow-k8s-pod",
        do_xcom_push=False,
        is_delete_operator_pod=True,
        in_cluster=True,
        task_id="k8s-pod-migrate_trees",
        get_logs=True,
        env_vars=environments
    )

    migrate_planter_info = KubernetesPodOperator(
        namespace=namespace,
        image=image,
        cmds=["sh", "-c", "npm run migrate-planters"],
        name="airflow-k8s-pod",
        do_xcom_push=False,
        is_delete_operator_pod=True,
        in_cluster=True,
        task_id="k8s-pod-migrate_planter_info",
        get_logs=True,
        env_vars=environments
    )
    
    migrate_tags = KubernetesPodOperator(
        namespace=namespace,
        image=image,
        cmds=["sh", "-c", "npm run migrate-tags"],
        name="airflow-k8s-pod",
        do_xcom_push=False,
        is_delete_operator_pod=True,
        in_cluster=True,
        task_id="k8s-pod-migrate_tags",
        get_logs=True,
        env_vars=environments
    )

    
    migrate_device_configurations = KubernetesPodOperator(
        namespace=namespace,
        image=image,
        cmds=["sh", "-c", "npm run migrate-devices"],
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
        cmds=["sh", "-c", "npm run migrate-species"],
        name="airflow-k8s-pod",
        do_xcom_push=False,
        is_delete_operator_pod=True,
        in_cluster=True,
        task_id="k8s-pod-migrate_species",
        get_logs=True,
        env_vars=environments
    )
    
    migrate_raw_capture = KubernetesPodOperator(
        namespace=namespace,
        image=image,
        cmds=["sh", "-c", "npm run migrate-raw-captures"],
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
        cmds=["sh", "-c", "npm run migrate-captures"],
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

    migrate_species >> migrate_raw_capture
    
    migrate_tags >> migrate_raw_capture

    migrate_raw_capture >> migrate_approved_capture >> migrate_trees >> t1
