from datetime import datetime, timedelta
from textwrap import dedent
from airflow.utils.dates import days_ago

# The DAG object; we'll need this to instantiate a DAG
from airflow import DAG

# Operators; we need this to operate!
from airflow.operators.bash import BashOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
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
    # 'on_failure_callback': some_function,
    # 'on_success_callback': some_other_function,
    # 'on_retry_callback': another_function,
    # 'sla_miss_callback': yet_another_function,
    # 'trigger_rule': 'all_success'
}
with DAG(
    dag_id='create_tokens',
    default_args=default_args,
    description='to create token into wallet',
    schedule_interval=None,
    start_date=days_ago(2),
    catchup=False,
    tags=['wallet', 'treetracker'],
) as dag:

    # t1, t2 and t3 are examples of tasks created by instantiating operators
    t1 = BashOperator(
        task_id='print_date',
        bash_command='date',
    )
    
    
    # define a function
    def create_tokens(ds, **kwargs):
        walletName = kwargs['dag_run'].conf.get('walletName')    
        entityId = kwargs['dag_run'].conf.get('entityId')    
        dryRun = kwargs['dag_run'].conf.get('dryRun')    
        mintLimit = kwards['dag_run'].conf.get('mintLimit')
        # print them out
        print('walletName:', walletName)
        print('entityId:', entityId)
        print('dryRun:', dryRun)
        print('mintLimit:', mintLimit)
        # check if wallet exists
        if walletName is None:
            print('walletName is None')
            return
        if entityId is None:
            print('entityId is None')
            return  
        if dryRun is None:
            print('dryRun is None')
            return
         if mintLimit is None:
            print('mintLimit is None')
            return
        
        result = 'pending'
        db = PostgresHook(postgres_conn_id='postgres_default')
        connection = db.get_conn()
        cursor = connection.cursor(cursor_factory=psycopg2.extras.DictCursor)
        try:
            # get first row from table 'wallet' 
            cursor.execute("SELECT * FROM wallet.wallet WHERE name = '{}'".format(walletName))
            wallet = cursor.fetchone()
            # check wallet exists
            if wallet is None:
                print('Wallet not found')
                return
            print('Wallet found', wallet)
            
            remaining = True
            
            for i in range(1, 100000):
                # if remaining is false, then we are done
                if not remaining:
                    break
                
                # fetch rows from table 'trees'
                cursor.execute("""
                    select id, uuid, token_id from trees
                    where
                        planter_id IN (
                            select id from planter 
                            where 
                                organization_id IN ( 
                                    select entity_id from getEntityRelationshipChildren({}) 
                                ) 
                            ) 
                            AND active = true 
                            AND approved = true 
                            AND token_id IS NULL 
                            LIMIT .format(mintLimit)
                    """.format(entityId))
                trees = cursor.fetchall()
                
                print('Trees found', len(trees))
                
                # check trees length < mintLimit
                if len(trees) < mintLimit:
                    print('Less tokens than the mintLimit will be minted')
                    remaining = False
                
                # for each tree, create a token
                for capture in trees:
                    print('capture', capture)

                    tokenData = {
                        'tree_id': capture['id'],
                        'capture_id': capture['uuid'],
                        'wallet_id': wallet['id'],
                    }
                    
                    print('tokenData', tokenData)

                    # create token
                    cursor.execute("""
                        INSERT INTO wallet.token (
                            capture_id,
                            wallet_id
                        ) VALUES (
                            '{}',
                            '{}'
                        ) RETURNING id
                    """.format(tokenData['capture_id'], tokenData['wallet_id']))
                    
                    token = cursor.fetchone()
                    print('token', token)
                    print('token[id]', token['id'])
                    
                    # update tree with token id
                    cursor.execute("""
                        UPDATE trees SET token_id = '{}' WHERE id = {}
                    """.format(token['id'], capture['id']))
                    
                    print('Token created: {}'.format(token))

            # if dryRun is false, then commit
            if not dryRun:
                connection.commit()
                print('Commit')
                result = 'success'
            else:
                print('Dry run, not committing')
                result = 'dry run'
        except Exception as e:
            print(e)
            result = 'error'
        finally:
            cursor.close()
            connection.close()
            print('result', result)

        # check result value, if success, return true, else return false
        if result == 'success':
            return 0
        else:
            return 1
    
    create_tokens_task = PythonOperator(
        task_id='create_tokens',
        python_callable=create_tokens,
        provide_context=True
    )
        


    t1 >> create_tokens_task
