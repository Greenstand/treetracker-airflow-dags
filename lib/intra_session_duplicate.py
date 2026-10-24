import math
from datetime import datetime, timezone


DISTANCE_THRESHOLD_METERS = 5.0
TIME_DELTA_THRESHOLD_SECONDS = 20.0
EARTH_RADIUS_METERS = 6371000.0


def haversine_distance(lat1, lon1, lat2, lon2) -> float:
    """Calculate the great-circle distance between two points in meters."""
    lat1_radians = math.radians(float(lat1))
    lon1_radians = math.radians(float(lon1))
    lat2_radians = math.radians(float(lat2))
    lon2_radians = math.radians(float(lon2))

    delta_lat = lat2_radians - lat1_radians
    delta_lon = lon2_radians - lon1_radians
    a = (
        math.sin(delta_lat / 2) ** 2
        + math.cos(lat1_radians)
        * math.cos(lat2_radians)
        * math.sin(delta_lon / 2) ** 2
    )
    a = min(1.0, max(0.0, a))
    return EARTH_RADIUS_METERS * 2 * math.asin(math.sqrt(a))


def _parse_captured_at(value) -> datetime:
    if isinstance(value, datetime):
        captured_at = value
    elif isinstance(value, str):
        captured_at = datetime.fromisoformat(value.replace('Z', '+00:00'))
    else:
        raise TypeError('captured_at must be a datetime or ISO-8601 string')

    if captured_at.tzinfo is not None:
        captured_at = captured_at.astimezone(timezone.utc).replace(tzinfo=None)
    return captured_at


def detect_intra_session_duplicates(captures: list[dict]) -> list[dict]:
    """Return captures that duplicate the directly preceding capture."""
    ordered_captures = sorted(
        captures,
        key=lambda capture: _parse_captured_at(capture['captured_at']),
    )
    duplicates = []

    for primary, duplicate in zip(ordered_captures, ordered_captures[1:]):
        primary_captured_at = _parse_captured_at(primary['captured_at'])
        duplicate_captured_at = _parse_captured_at(duplicate['captured_at'])
        time_delta_seconds = (
            duplicate_captured_at - primary_captured_at
        ).total_seconds()

        if time_delta_seconds > TIME_DELTA_THRESHOLD_SECONDS:
            continue

        distance_meters = haversine_distance(
            primary['lat'],
            primary['lon'],
            duplicate['lat'],
            duplicate['lon'],
        )
        if distance_meters > DISTANCE_THRESHOLD_METERS:
            continue

        duplicates.append({
            'primary_id': primary['id'],
            'duplicate_id': duplicate['id'],
            'distance_meters': distance_meters,
            'time_delta_seconds': time_delta_seconds,
        })

    return duplicates


def get_duplicate_detection_sql(session_id: str = None) -> str:
    """Return SQL for finding adjacent duplicate captures."""
    session_filter = ''
    if session_id is not None:
        escaped_session_id = str(session_id).replace("'", "''")
        session_filter = f"\n      AND session_id = '{escaped_session_id}'"

    return f"""
WITH ordered_captures AS (
    SELECT
        id,
        session_id,
        lat,
        lon,
        captured_at,
        LAG(id) OVER capture_order AS primary_id,
        LAG(lat) OVER capture_order AS primary_lat,
        LAG(lon) OVER capture_order AS primary_lon,
        LAG(captured_at) OVER capture_order AS primary_captured_at
    FROM raw_capture
    WHERE status = 'unprocessed'
      AND session_id IS NOT NULL{session_filter}
    WINDOW capture_order AS (
        PARTITION BY session_id
        ORDER BY captured_at, id
    )
),
time_candidates AS (
    SELECT
        *,
        EXTRACT(EPOCH FROM captured_at - primary_captured_at) AS time_delta_seconds
    FROM ordered_captures
    WHERE primary_id IS NOT NULL
      AND EXTRACT(EPOCH FROM captured_at - primary_captured_at)
          BETWEEN 0 AND {TIME_DELTA_THRESHOLD_SECONDS}
)
SELECT
    primary_id,
    id AS duplicate_id,
    session_id,
    ST_Distance(
        ST_SetSRID(ST_MakePoint(lon, lat), 4326)::geography,
        ST_SetSRID(ST_MakePoint(primary_lon, primary_lat), 4326)::geography
    ) AS distance_meters,
    time_delta_seconds
FROM time_candidates
WHERE ST_DWithin(
    ST_SetSRID(ST_MakePoint(lon, lat), 4326)::geography,
    ST_SetSRID(ST_MakePoint(primary_lon, primary_lat), 4326)::geography,
    {DISTANCE_THRESHOLD_METERS}
)
ORDER BY session_id, captured_at, id
""".strip()
