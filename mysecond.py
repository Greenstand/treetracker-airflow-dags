from datetime import timedelta

# The DAG object; we'll need this to instantiate a DAG
from airflow import DAG
# Operators; we need this to operate!
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
from airflow.utils.dates import days_ago
from lib.utils import on_failure_callback
# These args will get passed on to each operator
# You can override them on a per-task basis during operator initialization
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': days_ago(2),
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
    'on_failure_callback': on_failure_callback, # needs to be set in default_args to work correctly: https://github.com/apache/airflow/issues/26760
    # 'on_success_callback': some_other_function,
    # 'on_retry_callback': another_function,
    # 'sla_miss_callback': yet_another_function,
    # 'trigger_rule': 'all_success'
}
dag = DAG(
    'myfirst',
    default_args=default_args,
    description='First attempt to process a tree',
)

from types import SimpleNamespace

def allow_conf_testing(func):
    def wrapper(*args, **kwargs):
        if kwargs.get('test_mode', False):
            kwargs['dag_run'] = SimpleNamespace(conf=kwargs.get('params', {}))
        func(*args, **kwargs)
    return wrapper

templated_command = """
echo "hi"
"""

# t1, t2 and t3 are examples of tasks created by instantiating operators
t1 = BashOperator(
    task_id='echo',
    bash_command=templated_command,
    dag=dag,
)

@allow_conf_testing
def print_context(ds, **kwargs):
    print(kwargs)
    print(ds)
    print("Remotely received value of {} for key=message".format(kwargs['dag_run'].conf['message']))
    return 'Whatever you return gets printed in the logs'

t2 = PythonOperator(
    task_id='print_the_context',
    provide_context=True,
    python_callable=print_context,
    dag=dag,
)

