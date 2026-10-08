from datetime import datetime, timedelta
import unittest

import intra_session_duplicate


class Test_IntraSessionDuplicate(unittest.TestCase):
    start_time = datetime(2026, 1, 1, 12, 0, 0)

    def capture(self, capture_id, seconds, distance_meters):
        return {
            'id': capture_id,
            'lat': distance_meters / 111320.0,
            'lon': 0.0,
            'captured_at': self.start_time + timedelta(seconds=seconds),
        }

    def test_normal_movement_is_not_duplicate(self):
        captures = [
            self.capture('capture-1', 0, 0),
            self.capture('capture-2', 30, 15),
        ]

        result = intra_session_duplicate.detect_intra_session_duplicates(captures)

        self.assertEqual(result, [])

    def test_rapid_capture_in_same_location_is_duplicate(self):
        captures = [
            self.capture('capture-1', 0, 0),
            self.capture('capture-2', 5, 1),
        ]

        result = intra_session_duplicate.detect_intra_session_duplicates(captures)

        self.assertEqual(len(result), 1)
        self.assertEqual(result[0]['primary_id'], 'capture-1')
        self.assertEqual(result[0]['duplicate_id'], 'capture-2')
        self.assertAlmostEqual(result[0]['time_delta_seconds'], 5.0)
        self.assertLessEqual(result[0]['distance_meters'], 5.0)

    def test_same_location_after_time_window_is_not_duplicate(self):
        captures = [
            self.capture('capture-1', 0, 0),
            self.capture('capture-2', 120, 2),
        ]

        result = intra_session_duplicate.detect_intra_session_duplicates(captures)

        self.assertEqual(result, [])

    def test_rapid_capture_outside_distance_window_is_not_duplicate(self):
        captures = [
            self.capture('capture-1', 0, 0),
            self.capture('capture-2', 10, 50),
        ]

        result = intra_session_duplicate.detect_intra_session_duplicates(captures)

        self.assertEqual(result, [])

    def test_three_capture_chain_marks_second_and_third_as_duplicates(self):
        captures = [
            self.capture('capture-3', 10, 1),
            self.capture('capture-1', 0, 0),
            self.capture('capture-2', 5, 0.5),
        ]

        result = intra_session_duplicate.detect_intra_session_duplicates(captures)

        self.assertEqual(len(result), 2)
        self.assertEqual(
            [(item['primary_id'], item['duplicate_id']) for item in result],
            [
                ('capture-1', 'capture-2'),
                ('capture-2', 'capture-3'),
            ],
        )

    def test_haversine_distance_uses_meters(self):
        result = intra_session_duplicate.haversine_distance(0, 0, 0, 1)

        self.assertAlmostEqual(result, 111194.93, places=2)

    def test_sql_uses_postgis_filters_and_thresholds(self):
        sql = intra_session_duplicate.get_duplicate_detection_sql()

        self.assertIn("WHERE status = 'unprocessed'", sql)
        self.assertIn('AND session_id IS NOT NULL', sql)
        self.assertIn('LAG(id) OVER capture_order', sql)
        self.assertIn(
            'EXTRACT(EPOCH FROM captured_at - primary_captured_at)',
            sql,
        )
        self.assertIn('BETWEEN 0 AND 20.0', sql)
        self.assertIn('ST_DWithin(', sql)
        self.assertIn('ST_MakePoint(lon, lat)', sql)
        self.assertIn('5.0', sql)

    def test_sql_can_filter_one_session(self):
        sql = intra_session_duplicate.get_duplicate_detection_sql('session-1')

        self.assertIn("AND session_id = 'session-1'", sql)


if __name__ == '__main__':
    unittest.main()
