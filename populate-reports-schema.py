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
    'populate-reporting-schema',
    default_args=default_args,
    description='Populate the reporting schema',
    schedule_interval= "@daily",
    start_date=datetime(2021, 1, 1),
    catchup=False,
    tags=['reporting'],
) as dag:

    t1 = BashOperator(
        task_id='print_date',
        bash_command='date',
    )

    postgresConnId = "postgres_default"

    def populate_reporting_schema(ds, **kwargs):
        db = PostgresHook(postgres_conn_id=postgresConnId)
        conn = db.get_conn()  
        updateCursor = conn.cursor()
        cursor = conn.cursor('capture_reporting_cursor', cursor_factory=psycopg2.extras.RealDictCursor)
        try:
            cursor.execute("""
              SELECT
              trees.uuid AS capture_uuid,
              planter.first_name AS planter_first_name,
              planter.last_name AS planter_last_name,
              planter.phone AS planter_identifier,
              trees.time_created AS capture_created_at,
              trees.note AS note,
              trees.lat AS lat,
              trees.lon AS lon,
              trees.approved AS approved,
              planting_organization.stakeholder_uuid AS planting_organization_uuid,
              planting_organization.name AS planting_organization_name,
              tree_species.name AS species
              FROM trees
              JOIN planter
              ON planter.id = trees.planter_id
              LEFT JOIN entity AS planting_organization
              ON planting_organization.id = trees.planting_organization_id
              LEFT JOIN tree_species
              ON trees.species_id = tree_species.id
              WHERE trees.active = true
              AND planter_identifier IS NOT NULL
              AND planting_organization.id IN getEntityRelationshipChildren(178);
            """);
            print("SQL result:", cursor.query)

            updateCursor.execute("""
              DELETE FROM reporting.capture_denormalized
            """)

            for row in cursor:
                #do something with every single row here
                #optionally print the row
               # print(row)

                updateCursor.execute("""
                  INSERT INTO reporting.capture_denormalized
                  (capture_uuid, planter_first_name, planter_last_name, planter_identifier,
                   capture_created_at, lat, lon, note, approved, 
                   planting_organization_uuid, planting_organization_name,
                   species )
                  values
                  (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                  RETURNING *
                """, ( 
                row['capture_uuid'], row['planter_first_name'], row['planter_last_name'], row['planter_identifier'],
                row['capture_created_at'], row['lat'], row['lon'], row['note'], row['approved'],
                row['planting_organization_uuid'], row['planting_organization_name'], 
                row['species']
                ) );

            conn.commit()
            return 0
        except Exception as e:
            print("get error when exec SQL:", e)
            print("SQL result:", updateCursor.query)
            raise ValueError('Error executing query')
            return 1


    populate_reporting_schema = PythonOperator(
        task_id='populate_reporting_schema',
        python_callable=populate_reporting_schema,
        )



    populate_reporting_schema >> t1
