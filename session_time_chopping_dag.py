from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from lib.utils import on_failure_callback
import psycopg2
import psycopg2.extras
import uuid
from typing import List

psycopg2.extras.register_uuid()


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
    'session-time-chopping',
    default_args=default_args,
    description='The session time chopping job computes the session_segment for each raw_capture in a session',
    schedule_interval= "@daily",
    start_date=datetime(2021, 1, 1),
    catchup=False,
    tags=['capture_matching'],
) as dag:

    t1 = BashOperator(
        task_id='print_date',
        bash_command='date',
    )

    postgresConnId = "postgres_default"

    def get_session_segment_insert_sql(geom_points: List[str]) -> str:
        # remove duplicates
        geom_set = set(geom_points)
        geom_unique_points = list(geom_set)
        if(len(geom_unique_points) > 2):
            return f"INSERT INTO field_data.session_segment (id, session_id, starts_at, ends_at, processed_at, convex_hull) \
            VALUES (%s,%s,%s,%s,%s,(SELECT ST_ConvexHull(ST_Collect(ARRAY[{', '.join(geom_unique_points)}]))))"
        elif(len(geom_unique_points) == 2):
            # geom_points has a length of 2. Necessary because the query computes to a linestring not a polygon in this case
            return f"""INSERT INTO field_data.session_segment (id, session_id, starts_at, ends_at, processed_at, convex_hull) 
            VALUES (%s,%s,%s,%s,%s,(SELECT ST_Buffer(ST_MakeLine({geom_unique_points[0]}, {geom_unique_points[1]}), 0.01))
            )"""
        else:
            return f"""
                INSERT INTO field_data.session_segment (id, session_id, starts_at, ends_at, processed_at, convex_hull)
                VALUES (%s,%s,%s,%s,%s,(SELECT ST_Buffer({geom_unique_points[0]}, 0.01)))
            """

    def chop_session(ds, **kwargs):
        db = PostgresHook(postgres_conn_id=postgresConnId)
        conn = db.get_conn()  
        
        create_cursor = conn.cursor()
        update_cursor = conn.cursor()
        session_cursor = conn.cursor(
            "session_cursor", cursor_factory=psycopg2.extras.RealDictCursor
        )
        raw_capture_cursor = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        st_distance_cursor = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        try:
            session_cursor.execute(
                """
                SELECT id FROM field_data.session s
                WHERE s.processed_at is null and created_at < now() - interval '24 hours';
            """
            )

            for session in session_cursor:
                print('processing session', session["id"])
                raw_capture_cursor.execute(
                    """
                        SELECT id, captured_at, lat, lon FROM field_data.raw_capture rc WHERE rc.session_id = %s ORDER BY captured_at asc;
                    """,
                    (session["id"],),
                )

                raw_captures = raw_capture_cursor.fetchall()
                if len(raw_captures):
                    if len(raw_captures) > 1:
                        raw_captures_segments = []
                        first_raw_capture = raw_captures.pop(0)
                        session_start_time = first_raw_capture["captured_at"]
                        previous_raw_capture = first_raw_capture
                        current_session_segment = {
                            "id": uuid.uuid4(),
                            "session_id": session["id"],
                            "starts_at": session_start_time,
                        }
                        raw_captures_segments.append({
                            'id': first_raw_capture["id"],
                            'lat': first_raw_capture["lat"],
                            'lon': first_raw_capture["lon"],
                        })
                        for current_raw_capture in raw_captures:
                            time_distance = current_raw_capture["captured_at"] - previous_raw_capture["captured_at"]
                            time_distance_seconds = time_distance.total_seconds()
                            
                            st_distance_cursor.execute(
                                """
                                    SELECT ST_Distance(ST_GeogFromText('SRID=4326;POINT(%s %s)'), ST_GeogFromText('SRID=4326;POINT(%s %s)'));
                                """,
                                (
                                    current_raw_capture["lon"],
                                    current_raw_capture["lat"],
                                    previous_raw_capture["lon"],
                                    previous_raw_capture["lat"],
                                ),
                            )
                            location_distance = st_distance_cursor.fetchone()
                            location_distance_meters = location_distance["st_distance"]
                            if (
                                time_distance_seconds > 7200
                                or location_distance_meters > 100
                            ):
                                current_session_segment["ends_at"] = previous_raw_capture["captured_at"]
                                # create old session_segment
                                geom_points = [f"ST_GeomFromText('POINT({raw_capture['lon']} {raw_capture['lat']})',4326)" for raw_capture in raw_captures_segments]
                                session_segment_insert_sql = get_session_segment_insert_sql(geom_points=geom_points)
                                create_cursor.execute(session_segment_insert_sql,
                                    (
                                        current_session_segment["id"],
                                        current_session_segment["session_id"],
                                        current_session_segment["starts_at"],
                                        current_session_segment["ends_at"],
                                        datetime.now(),
                                    ),
                                )
                                # update raw_capture session_segment_id
                                update_cursor.execute(
                                    """
                                    UPDATE field_data.raw_capture SET session_segment_id = %s WHERE id in %s;
                                """,
                                    (
                                        current_session_segment["id"],
                                        tuple(list(map(lambda x: x['id'], raw_captures_segments))),
                                    ),
                                )
                                # start new session_segment
                                current_session_segment = {
                                    "id": uuid.uuid4(),
                                    "session_id": session["id"],
                                    "starts_at": current_raw_capture["captured_at"],
                                }
                                # empty raw_capture_id_session_segment_ids
                                raw_captures_segments.clear()
                            
                            raw_captures_segments.append({
                                'id': current_raw_capture["id"],
                                'lat': current_raw_capture["lat"],
                                'lon': current_raw_capture["lon"],
                            })
                            previous_raw_capture = current_raw_capture

                        geom_points = [f"ST_GeomFromText('POINT({raw_capture['lon']} {raw_capture['lat']})',4326)" for raw_capture in raw_captures_segments]
                        session_segment_insert_sql = get_session_segment_insert_sql(geom_points=geom_points)
                        create_cursor.execute(session_segment_insert_sql,
                                (
                                    current_session_segment["id"],
                                    current_session_segment["session_id"],
                                    current_session_segment["starts_at"],
                                    raw_captures[-1]['captured_at'],
                                    datetime.now(),
                                ),
                            )
                        
                        update_cursor.execute(
                            """
                                UPDATE field_data.raw_capture SET session_segment_id = %s WHERE id in %s;
                            """,
                                (
                                    current_session_segment["id"],
                                    tuple(list(map(lambda x: str(x['id']), raw_captures_segments))),
                                ),
                            )
                        
                        update_cursor.execute(
                            """
                                UPDATE field_data.session SET processed_at = %s WHERE id = %s;
                            """,
                                (
                                    datetime.now(),
                                    session["id"],
                                ),
                            )
                    elif len(raw_captures) == 1:
                        single_raw_capture = raw_captures[0]
                        current_session_segment = {
                            "id": uuid.uuid4(),
                            "session_id": session["id"],
                            "starts_at": single_raw_capture["captured_at"],
                            "ends_at": single_raw_capture["captured_at"],
                        }
                        create_cursor.execute(
                            """
                                INSERT INTO field_data.session_segment (id, session_id, starts_at, ends_at, processed_at, convex_hull)
                                VALUES (%s,%s,%s,%s,%s,(SELECT ST_Buffer(ST_GeomFromText('POINT(%s %s)',4326), 0.01)))
                            """,
                                (
                                    current_session_segment["id"],
                                    current_session_segment["session_id"],
                                    current_session_segment["starts_at"],
                                    current_session_segment["ends_at"],
                                    datetime.now(),
                                    single_raw_capture['lon'],
                                    single_raw_capture['lat'],
                                ),
                            )
                        
                        update_cursor.execute(
                            """
                                UPDATE field_data.raw_capture SET session_segment_id = %s WHERE id = %s;
                            """,
                                (
                                    current_session_segment["id"],
                                    single_raw_capture["id"],
                                ),
                            )
                        
                        update_cursor.execute(
                            """
                                UPDATE field_data.session SET processed_at = %s WHERE id = %s;
                            """,
                                (
                                    datetime.now(),
                                    session["id"],
                                ),
                            )
                else:
                    print('no raw captures found for', session["id"])
                
                print('session processed')

            conn.commit()
            return 0
        except psycopg2.Error as e:
            # Print the error message
            print("Error message:", e.diag.message)
        except Exception as e:
            print("get error when exec SQL:", e)
            raise ValueError("Error executing query")



    chop_session = PythonOperator(
        task_id='chop_session',
        python_callable=chop_session,
    )



    chop_session >> t1