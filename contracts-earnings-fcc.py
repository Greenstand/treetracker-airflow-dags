from datetime import datetime, timedelta
from textwrap import dedent
from pprint import pprint

# The DAG object; we'll need this to instantiate a DAG
from airflow import DAG

from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook

# Operators; we need this to operate!
from airflow.operators.bash import BashOperator

from airflow.operators.python import PythonOperator

# These args will get passed on to each operator
# You can override them on a per-task basis during operator initialization
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
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
    'contract-earnings-fcc',
    default_args=default_args,
    description='Calculate earnings for FCC planters',
    schedule_interval= '* * * * *',
    #schedule_interval= '@hourly',
    start_date=datetime(2021, 1, 1),
    catchup=False,
    tags=['earnings'],
) as dag:

    t1 = BashOperator(
        task_id='print_date',
        bash_command='date',
    )


# example for PostgresOperator
#    t2 = PostgresOperator(
#        task_id="get_planters",
#        postgres_conn_id="postgres_default",
#        sql="SELECT * FROM planter LIMIT 1;",
#    )

    def earnings_report(ds, **kwargs):
        db = PostgresHook(postgres_conn_id="postgres_default")
        conn = db.get_conn()
        print("db:", conn)
        cursor = conn.cursor()
#        cursor.execute("SELECT * FROM planter LIMIT 1;")
#        print("cursor:", cursor);
#        result = cursor.fetchall();
#        print("result:", result);
        try:
            cursor.execute("""
INSERT INTO earnings.earnings(
  id, 
  contract_id, 
  funder_id, 
  currency, 
  calculated_at, 
  consolidation_id, 
  consolidation_period_start, 
  consolidation_period_end, 
  payment_confirmation_method, 
  status, 
  active, 
  amount, 
  worker_id)
SELECT 
  uuid_generate_v1(), 
  uuid_generate_v1(),
  uuid_generate_v1(), 
  'USD', 
  NOW(), 
  uuid_generate_v1(), 
  NOW(),
  NOW(),
  'batch',
  'calculated',
  true,
  planted.amount AS amount, 
  /*p.person_id AS worker_id*/'dca241c2-321b-11ec-9d1b-a22d6120144d' AS worker_id 
  FROM (
    SELECT t.planter_id, count(*) * 0.1 amount FROM trees t
    WHERE t.time_created >= NOW() - INTERVAL '1 HOUR'
    GROUP BY t.planter_id
  ) planted
LEFT JOIN planter p
ON planted.planter_id = p.id
LIMIT 1;
            """);
            print("SQL result:", cursor.query)
            conn.commit()
            print("SQL result:", cursor.query)
            return 0
        except Exception as e:
            print("get error when exec SQL:", e)
            return 1

    earnings_report = PythonOperator(
        task_id='earnings_report',
        python_callable=earnings_report,
        )


    earnings_report >> t1
