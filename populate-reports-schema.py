from datetime import datetime, timedelta
from textwrap import dedent
from pprint import pprint
from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
import psycopg2.extras
from lib.utils import on_failure_callback

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
    'on_failure_callback': on_failure_callback, # needs to be set in default_args to work correctly: https://github.com/apache/airflow/issues/26760
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
              trees.planting_organization_id AS tree_organization_uuid,
              planter.first_name AS planter_first_name,
              planter.last_name AS planter_last_name,
              COALESCE(planter.phone, planter.email) AS planter_identifier,
              planter.gender AS gender,
              trees.time_created AS capture_created_at,
              trees.note AS note,
              trees.lat AS lat,
              trees.lon AS lon,
              trees.approved AS approved,
              planting_organization.stakeholder_uuid AS planting_organization_uuid,
              planting_organization.name AS planting_organization_name,
              tree_species.name AS species,
              region.name AS catchment,
              tc.tree_id AS tree_id
              FROM trees
              JOIN planter
              ON planter.id = trees.planter_id
              LEFT JOIN entity AS planting_organization
              ON planting_organization.id = planter.organization_id
              LEFT JOIN tree_species
              ON trees.species_id = tree_species.id
              LEFT JOIN treetracker.capture AS tc
              ON trees.uuid = tc.id::text
              LEFT JOIN (
                SELECT region.name, region.geom
                FROM region
                JOIN region_type
                ON region_type.id = region.type_id
                WHERE region_type.type = 'fcc_catchments'
              ) region
              ON ST_WITHIN(trees.estimated_geometric_location, region.geom) 
              WHERE trees.active = true
              AND planter_identifier IS NOT NULL
              AND planter.organization_id IN (SELECT entity_id from getEntityRelationshipChildren(178))
              --- AND trees.id = 827280
              ;
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
                   species, catchment, gender, tree_organization_uuid, tree_id  )
                  values
                  (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                  RETURNING *
                """, ( 
                row['capture_uuid'], row['planter_first_name'], row['planter_last_name'], row['planter_identifier'],
                row['capture_created_at'], row['lat'], row['lon'], row['note'], row['approved'],
                row['planting_organization_uuid'], row['planting_organization_name'], 
                row['species'],
                row['catchment'], row['gender'], row['tree_organization_uuid'], row['tree_id']
                ) );

            conn.commit()
            return 0
        except Exception as e:
            print("get error when exec SQL:", e)
            print("SQL result:", updateCursor.query)
            raise ValueError('Error executing query')


    populate_reporting_schema = PythonOperator(
        task_id='populate_reporting_schema',
        python_callable=populate_reporting_schema,
        )



    populate_reporting_schema >> t1