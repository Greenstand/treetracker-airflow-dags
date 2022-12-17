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
    'populate-tree-reporting-schema',
    default_args=default_args,
    description='Populate the tree_denormalized table in the reporting schema',
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

    def populate_tree_reporting_schema(ds, **kwargs):
        db = PostgresHook(postgres_conn_id=postgresConnId)
        conn = db.get_conn()  
        updateCursor = conn.cursor()
        cursor = conn.cursor('tree_reporting_cursor', cursor_factory=psycopg2.extras.RealDictCursor)
        try:
            cursor.execute("""
             SELECT
                tree.id AS tree_uuid,
                tree.created_at AS tree_created_at,
                ga.first_name AS planter_first_name,
                ga.last_name AS planter_last_name,
                ga.wallet AS planter_identifier,
                tree.lat AS lat,
                tree.lon AS lon,
                tc.note AS note,
                planting_organization.id AS planting_organization_uuid,
                planting_organization.org_name AS planting_organization_name,
                hs.scientific_name AS species
              FROM treetracker.tree
              JOIN treetracker.capture tc
                ON tc.id = tree.latest_capture_id
              JOIN treetracker.grower_account ga
                ON ga.id = tc.grower_account_id
              LEFT JOIN stakeholder.stakeholder planting_organization
                ON planting_organization.id = ga.organization_id
              LEFT JOIN herbarium.species hs
                ON hs.id = tree.species_id
              WHERE tree.status = 'active';
            """);
            print("SQL result:", cursor.query)

            updateCursor.execute("""
              DELETE FROM reporting.tree_denormalized
            """)

            for row in cursor:
                #do something with every single row here
                #optionally print the row
               # print(row)

                updateCursor.execute("""
                  INSERT INTO reporting.tree_denormalized
                  (
                    tree_uuid,
                    tree_created_at,
                    planter_first_name,
                    planter_last_name,
                    planter_identifier,
                    lat,
                    lon,
                    note,
                    planting_organization_uuid,
                    planting_organization_name,
                    species
                  )
                  values
                  (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                  RETURNING *
                """, ( 
                        row['tree_uuid'], 
                        row['tree_created_at'], 
                        row['planter_first_name'], 
                        row['planter_last_name'], 
                        row['planter_identifier'],
                        row['lat'], 
                        row['lon'], 
                        row['note'], 
                        row['planting_organization_uuid'], 
                        row['planting_organization_name'], 
                        row['species']
                ) );

            conn.commit()
            return 0
        except Exception as e:
            print("get error when exec SQL:", e)
            print("SQL result:", updateCursor.query)
            raise ValueError('Error executing query')


    populate_tree_reporting_schema = PythonOperator(
        task_id='populate_tree_reporting_schema',
        python_callable=populate_tree_reporting_schema,
        )



    populate_tree_reporting_schema >> t1
