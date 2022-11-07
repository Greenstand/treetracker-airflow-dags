# This DAG refreshs the webmap database organization_location materialzied view.

from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
import psycopg2.extras
from airflow.utils.dates import days_ago


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
}

with DAG(
    dag_id='refresh_webmap_db_org_location_view',
    default_args=default_args,
    description='Refresh the material view in the webmap database.',
    schedule_interval="@daily",
    start_date=days_ago(2),
    catchup=False,
    tags=['webmap'],
) as dag:

    # Connect to the TreeTracker Database
    try:
        postgresConnId = "postgres_default"
        db = PostgresHook(postgres_conn_id=postgresConnId)
        conn = db.get_conn()
        print('Connected to database')
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)

    # Python function to refresh the views
    def view_refresh(conn):
        sql = 'REFRESH MATERIALIZED VIEW webmap.organization_location'

        try:
            # Setup the cursor to execute SQL statements
            cur = conn.cursor()

            # Execute the SQL statement
            cur.execute(sql)

            # Close the connection
            cur.close()

        except (Exception, psycopg.DatabaseError) as error:
            print(error)

    task = PythonOperator(
        task_id='refresh_webmap_organization_location',
        python_callable=view_refresh,
    )

    task

    print('Webmap Organization Location Refreshed')



