from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.utils.dates import days_ago
from datetime import datetime
import psycopg2
import os


def query_species_leaderboard():
    try:
        # Connect to the PostgreSQL database
        connection = psycopg2.connect(database_url)
        cursor = connection.cursor()

        # Write the SQL query to generate the leaderboard
        query = """
        SELECT species, COUNT(*) as count
        FROM trees
        GROUP BY species
        ORDER BY count DESC;
        """
        
        cursor.execute(query)
        result = cursor.fetchall()

        # Print the leaderboard results to the log
        for row in result:
            print(f"Species: {row[0]}, Count: {row[1]}")
        

        cursor.close()
        connection.close()

    except Exception as e:
        print(f"Error querying species leaderboard: {e}")

species_dag = DAG(
    "species",
    start_date = days_ago(7),
    description='Analyzing species data',
    schedule_interval="* * * * *",
    catchup = False
)

pyop = PythonOperator(
    task_id = "Species",
    python_callable = query_species_leaderboard,
    dag = species_dag
)
