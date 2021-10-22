from datetime import datetime, timedelta
from textwrap import dedent
from pprint import pprint
from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
import psycopg2.extras

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

    def earnings_report(ds, **kwargs):
        db = PostgresHook(postgres_conn_id="postgres_default")
        conn = db.get_conn()
        print("db:", conn)
        cursor = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        try:
            cursor.execute("""
SELECT COUNT(tree_id) capture_count,
COUNT(tree_id) * .02 earnings,
person_id,
stakeholder_uuid,
MIN(time_created) consolidation_start_date,
MAX(time_created) consolidation_end_date,
ARRAY_AGG(tree_id) tree_ids
FROM (
SELECT trees.id tree_id, person_id, time_created,
stakeholder_uuid,
rank() OVER (
  PARTITION BY person_id
  ORDER BY time_created ASC
)
FROM trees
JOIN planter
ON trees.planter_id = planter.id
JOIN entity
ON entity.id = planter.person_id
WHERE person_id IN (
  SELECT person_id
  FROM trees
  JOIN planter
  ON trees.planter_id = planter.id
  WHERE person_id IS NOT NULL
  AND earnings_id IS NULL
  GROUP BY person_id
  HAVING count(trees.id) > 200
)
AND earnings_id IS NULL
AND planter.organization_id IN (178)
) rank_filter
WHERE RANK <= 200
GROUP BY person_id, stakeholder_uuid
ORDER BY person_id;
            """);
            print("SQL result:", cursor.query)
            for row in cursor:
                #do something with every single row here
                #optionally print the row
                print(row)
                print(row['earnings'])
                print(row['stakeholder_uuid'])

                updateCursor = conn.cursor()
                updateCursor.execute("""
INSERT INTO earnings.earnings(
  worker_id,
  contract_id,
  funder_id,
  currency,
  amount,
  calculated_at,
  consolidation_id,
  consolidation_period_start,
  consolidation_period_end,
  status
  )
VALUES(
  %s,
  %s,
  %s,
  %s,
  %s,
  NOW(),
  %s,
  %s,
  %s,
  'calculated'
)
RETURNING *""", (row['stakeholder_uuid'], row['stakeholder_uuid'], row['stakeholder_uuid'], 'USD', row['earnings'], row['stakeholder_uuid'], row['consolidation_start_date'], row['consolidation_end_date']))
                print("SQL result:", updateCursor.query)

                earningsId = updateCursor.fetchone()[0]
                print(earningsId)
                updateCursor.execute("""
UPDATE trees
SET earnings_id = %s
WHERE id = ANY(%s)
""", (earningsId, row['tree_ids']))

            conn.commit()
            return 0
        except Exception as e:
            print("get error when exec SQL:", e)
            print("SQL result:", updateCursor.query)
            raise ValueError('Error executing query')
            return 1

    earnings_report = PythonOperator(
        task_id='earnings_report',
        python_callable=earnings_report,
        )


    earnings_report >> t1
