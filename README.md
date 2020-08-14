# Airflow DAGS


# manually trigger a DAG
airflow trigger_dag --conf '{"conf1": "value1"}' myfirst

# test a DAG
airflow test myfirst echo 2015-06-01 -tp '{"message":"answer"}'

