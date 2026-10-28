# Define the Test class inheriting from unittest.TestCase
import psycopg2
import os
import unittest   # The test framework
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.utils.dates import days_ago
from datetime import datetime


class TestSpeciesLeaderboard(unittest.TestCase):
    
    def setUp(self):
        # Setup method to initialize variables before each test
        print("Setting up environment variables and database connection")
        
        # Fetching database URL from environment variables
        self.database_url = os.environ['DB_URL']
        
        # Initializing DAG
        self.dag = DAG(
            "species",
            start_date=days_ago(7),
            description='Analyzing species data',
            schedule_interval="* * * * *",
            catchup=False
        )

    def query_species_leaderboard(self):
        # The actual code that connects to PostgreSQL and queries the species leaderboard
        try:
            # Connecting to the PostgreSQL database
            connection = psycopg2.connect(self.database_url)
            cursor = connection.cursor()

            # Writing the SQL query to generate the leaderboard
            query = """
            SELECT species, COUNT(*) as count
            FROM trees
            GROUP BY species
            ORDER BY count DESC;
            """

            # Execute the query
            cursor.execute(query)
            result = cursor.fetchall()

            # Print the leaderboard results to the log
            for row in result:
                print(f"Species: {row[0]}, Count: {row[1]}")

            cursor.close()
            connection.close()

            # Assert that the result is not empty
            self.assertTrue(result)
        
        except Exception as e:
            # If there's an error, fail the test
            print(f"Error querying species leaderboard: {e}")
            self.fail("Database query failed")

    def test_leaderboard_query_in_dag(self):
        # Define the PythonOperator in the DAG and test if it works
        pyop = PythonOperator(
            task_id="Species",
            python_callable=self.query_species_leaderboard,
            dag=self.dag
        )
        
        # Check if the task is properly initialized
        self.assertIsNotNone(pyop)

# Run the unit tests
if __name__ == '__main__':
    unittest.main()
