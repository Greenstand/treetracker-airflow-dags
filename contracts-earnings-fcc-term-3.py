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
    'contract-earnings-fcc-term-3',
    default_args=default_args,
    description='Calculate earnings for FCC planters',
    schedule_interval= None,
    start_date=datetime(2021, 1, 1),
    catchup=False,
    tags=['earnings', 'freetown'],
) as dag:

    t1 = BashOperator(
        task_id='print_date',
        bash_command='date',
    )

    postgresConnId = "postgres_default"

    def create_new_person_records(ds, **kwargs):
        db = PostgresHook(postgres_conn_id=postgresConnId)
        conn = db.get_conn()  
        cursor = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        try:
            cursor.execute("""
              SELECT *
              FROM planter
              JOIN
              (
                SELECT regexp_replace((trim(lower(first_name)) ||  trim(lower(last_name))), '[ .-]', '', 'g') as name_key, count(*)
                FROM planter
                WHERE
                planter.organization_id IN (
                  select entity_id from getEntityRelationshipChildren(178)
                )
                GROUP BY name_key
                HAVING count(*) = 1
                ORDER BY name_key
              ) eligible_records
              ON regexp_replace((trim(lower(first_name)) ||  trim(lower(last_name))), '[ .-]', '', 'g') = eligible_records.name_key
              WHERE planter.organization_id IN (
                select entity_id from getEntityRelationshipChildren(178)
              )
              AND person_id IS NULL
            """);
            print("SQL result:", cursor.query)
            for row in cursor:
                #do something with every single row here
                #optionally print the row
                print(row)

                updateCursor = conn.cursor()
                updateCursor.execute("""
                  INSERT INTO entity
                  (type, first_name, last_name, email, phone)
                  values
                  ('p', %s, %s, %s, %s)
                  RETURNING *
                """, ( row['first_name'], row['last_name'], row['email'], row['phone'] ) );

                personId = updateCursor.fetchone()[0];
                print(personId)
                updateCursor.execute("""
                  UPDATE planter
                  SET person_id = %s
                  WHERE id = %s
                """, (personId, row['id']) )

            conn.commit()
            return 0
        except Exception as e:
            print("get error when exec SQL:", e)
            print("SQL result:", updateCursor.query)
            raise ValueError('Error executing query')
            return 1


    def earnings_report(ds, **kwargs):
        db = PostgresHook(postgres_conn_id=postgresConnId)
        conn = db.get_conn()
        print("db:", conn)
        cursor = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        try:
           
            # these hard coded values are placeholders for the upcoming contracts system
            freetown_stakeholder_uuid = "2a34fa81-0683-4d25-94b9-24843ceec3c4"
            freetown_base_contract_uuid = "483a1f4e-0c52-4b53-b917-5ff4311ded26"
            freetown_base_contract_consolidation_uuid = "a2dc79ec-4556-4cc5-bff1-2dbb5fd35b51"

            cursor.execute("""
              SELECT COUNT(tree_id) capture_count,
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
                AND earnings_id IS NULL
                AND planter.organization_id IN (
                  select entity_id from getEntityRelationshipChildren(178)
                )
                AND time_created >= TO_TIMESTAMP(
                  '2022-02-01 00:00:00',
                  'YYYY-MM-DD HH24:MI:SS'
                )
                AND time_created <  TO_TIMESTAMP(
                  '2022-04-01 00:00:00',
                  'YYYY-MM-DD HH24:MI:SS'
                )
                AND trees.approved = true
                AND trees.active = true
              ) rank
              GROUP BY person_id, stakeholder_uuid
              ORDER BY person_id;
            """);
            print("SQL result:", cursor.query)
            for row in cursor:
                print(row)

                #calculate the earnings based on FCC logic
                multiplier = (row['capture_count'] - row['capture_count'] % 100) / 10 / 100
                if multiplier > 1: 
                  multiplier = 1
                print( "multiplier " + str(multiplier) )

                maxPayout = 1200000
                earningsCurrency = 'SLL'
                earnings = multiplier * maxPayout

                updateCursor = conn.cursor()
                updateCursor.execute("""
                  INSERT INTO earnings.earnings(
                    worker_id,
                    contract_id,
                    funder_id,
                    currency,
                    amount,
                    calculated_at,
                    consolidation_rule_id,
                    consolidation_period_start,
                    consolidation_period_end,
                    status,
                    captures_count
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
                    'calculated',
                    %s,
                  )
                  RETURNING *
              """, ( row['stakeholder_uuid'],
                     freetown_base_contract_uuid,
                     freetown_stakeholder_uuid,
                     earningsCurrency, 
                     earnings,
                     freetown_base_contract_consolidation_uuid,
                     row['consolidation_start_date'],
                     row['consolidation_end_date'],
                     row['capture_count']
                     ))
                print("SQL result:", updateCursor.query)

                earningsId = updateCursor.fetchone()[0]
                print(earningsId)
                updateCursor.execute("""
                  UPDATE trees
                  SET earnings_id = %s
                  WHERE id = ANY(%s)
                """, 
                (earningsId, 
                row['tree_ids']))

            conn.commit()
            return 0
        except Exception as e:
            print("get error when exec SQL:", e)
            print("SQL result:", updateCursor.query)
            raise ValueError('Error executing query')
            return 1

    create_new_person_records = PythonOperator(
        task_id='create_new_person_records',
        python_callable=create_new_person_records,
        )

    earnings_report = PythonOperator(
        task_id='earnings_report',
        python_callable=earnings_report,
        )


    create_new_person_records >> earnings_report >> t1
