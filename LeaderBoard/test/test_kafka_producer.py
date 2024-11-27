import unittest
from unittest.mock import MagicMock, patch
from datetime import datetime
import json

from Intuit.LeaderBoard.database import insert_score, get_top_scores
from Intuit.LeaderBoard.src.api_service import app
from Intuit.LeaderBoard.src.kafka_producer import publish_score, produce_test_scores


class TestKafkaProducer(unittest.TestCase):
    def setUp(self):
        self.mock_producer = MagicMock()
        self.mock_producer.produce = MagicMock()
        self.patcher = patch('kafka_producer.producer', self.mock_producer)
        self.patcher.start()

    def tearDown(self):
        self.patcher.stop()

    def test_publish_score(self):
        timestamp = datetime.now()
        publish_score('Alice', 1500, timestamp)
        expected_message = json.dumps({
            "player_name": "Alice",
            "score": 1500,
            "timestamp": timestamp.isoformat()
        }).encode('utf-8')
        self.mock_producer.produce.assert_called_once_with('game-scores-topic', expected_message)

    def test_produce_test_scores(self):
        with patch('kafka_producer.publish_score') as mock_publish:
            produce_test_scores(num_players=5)
            self.assertEqual(mock_publish.call_count, 5)


class TestDatabase(unittest.TestCase):
    @patch('database.conn')
    def test_insert_score(self, mock_conn):
        mock_cursor = MagicMock()
        mock_conn.cursor.return_value.__enter__.return_value = mock_cursor

        insert_score('Bob', 2000)

        mock_cursor.execute.assert_called_once()
        mock_conn.commit.assert_called_once()

    @patch('database.conn')
    def test_get_top_scores(self, mock_conn):
        mock_cursor = MagicMock()
        mock_conn.cursor.return_value.__enter__.return_value = mock_cursor
        mock_cursor.fetchall.return_value = [
            ('Alice', 1000, datetime(2023, 1, 1, 12, 0)),
            ('Bob', 1000, datetime(2023, 1, 1, 12, 5)),
            ('Charlie', 900, datetime(2023, 1, 1, 12, 10))
        ]

        result = get_top_scores(limit=3)

        self.assertEqual(len(result), 3)
        self.assertEqual(result[0][0], 'Alice')  # First place due to earlier timestamp
        self.assertEqual(result[1][0], 'Bob')  # Second place due to later timestamp
        mock_cursor.execute.assert_called_once()


class TestAPIService(unittest.TestCase):
    def setUp(self):
        self.app = app.test_client()

    @patch('api_service.r')
    def test_top_scores_endpoint(self, mock_redis):
        mock_data = json.dumps([
            {"player_name": "Alice", "score": 1000, "timestamp": "2023-01-01T12:00:00"},
            {"player_name": "Bob", "score": 900, "timestamp": "2023-01-01T12:05:00"}
        ])
        mock_redis.get.return_value = mock_data

        response = self.app.get('/top-scores')

        self.assertEqual(response.status_code, 200)
        data = json.loads(response.data)
        self.assertEqual(len(data), 2)
        self.assertEqual(data[0]['player_name'], 'Alice')
        self.assertEqual(data[0]['score'], 1000)

    @patch('api_service.r')
    def test_top_scores_endpoint_no_data(self, mock_redis):
        mock_redis.get.return_value = None

        response = self.app.get('/top-scores')

        self.assertEqual(response.status_code, 404)
        data = json.loads(response.data)
        self.assertEqual(data, [])


if __name__ == '__main__':
    unittest.main()