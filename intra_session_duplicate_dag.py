from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook

from lib.intra_session_duplicate import get_duplicate_detection_sql
from lib.utils import on_failure_callback, print_time


postgres_conn_id = "postgres_default"
UPDATE_BATCH_SIZE = 1000
UPDATE_DUPLICATES_SQL = """
    UPDATE raw_capture
    SET status = 'rejected',
        rejection_reason = 'duplicate_intra_session',
        updated_at = NOW()
    WHERE id = ANY(%s)
      AND status = 'unprocessed'
"""


def _get_duplicate_id(row):
    if isinstance(row, dict):
        return row['duplicate_id']
    return row[1]


def _get_session_id(row):
    if isinstance(row, dict):
        return row['session_id']
    return row[2]


def run_intra_session_duplicate_check(ds=None, **kwargs):
    print_time('Starting intra-session duplicate detection')
    conn = None
    cursor = None

    try:
        hook = PostgresHook(postgres_conn_id=postgres_conn_id)
        conn = hook.get_conn()
        cursor = conn.cursor()

        # Query adjacent unprocessed captures.
        cursor.execute(get_duplicate_detection_sql())
        duplicate_rows = cursor.fetchall()
        duplicate_ids = list(dict.fromkeys(
            _get_duplicate_id(row) for row in duplicate_rows
        ))
        duplicate_session_ids = {
            _get_session_id(row) for row in duplicate_rows
        }
        print_time(
            f'Found {len(duplicate_ids)} duplicate captures across '
            f'{len(duplicate_session_ids)} sessions'
        )

        updated_captures = 0
        # Update duplicates in bounded batches.
        for index in range(0, len(duplicate_ids), UPDATE_BATCH_SIZE):
            duplicate_id_batch = duplicate_ids[index:index + UPDATE_BATCH_SIZE]
            cursor.execute(UPDATE_DUPLICATES_SQL, (duplicate_id_batch,))
            updated_captures += cursor.rowcount

        conn.commit()
        print_time(f'Marked {updated_captures} duplicate captures as rejected')
        return {
            'duplicate_candidates': len(duplicate_ids),
            'updated_captures': updated_captures,
        }
    except Exception as error:
        if conn is not None:
            conn.rollback()
        print_time(f'Intra-session duplicate detection failed: {error}')
        raise
    finally:
        if cursor is not None:
            cursor.close()
        if conn is not None:
            conn.close()


default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0,
    'retry_delay': timedelta(minutes=5),
    'on_failure_callback': on_failure_callback,
}


with DAG(
    'intra_session_duplicate_detection',
    default_args=default_args,
    description='Detect and reject duplicate raw captures within a session',
    schedule_interval='@hourly',
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=['quality', 'raw_capture', 'duplicate'],
) as dag:
    detect_and_flag_duplicates = PythonOperator(
        task_id='detect_and_flag_duplicates',
        python_callable=run_intra_session_duplicate_check,
    )
