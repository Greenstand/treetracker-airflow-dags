# This DAG refreshs the webmap database planter_location materialzied view.

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
    dag_id='refresh_webmap_db_planter_location_view',
    default_args=default_args,
    description='Refresh the material view in the webmap database.',
    schedule_interval="@daily",
    start_date=days_ago(2),
    catchup=False,
    tags=['webmap'],
) as dag:

    postgresConnId = "postgres_default"

    # Python function to refresh the views
    def view_refresh(ds, **kwargs):
        
        db = PostgresHook(postgres_conn_id=postgresConnId)
        conn = db.get_conn()
        
        sql = 'REFRESH MATERIALIZED VIEW webmap.planter_location'

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
        task_id='refresh_webmap_planter_location',
        python_callable=view_refresh,
    )

    task
