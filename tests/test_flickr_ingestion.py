import unittest
from unittest.mock import patch, mock_open, MagicMock
from flickr_ingestion import (
    load_config,
    initialize_flickr_api,
    initialize_kafka_producer,
    load_bloom_filter,
    save_bloom_filter,
    save_min_taken_date,
    load_min_taken_date,
    handle_api_error_code,
    fetch_photos_with_retry
)

class TestFlickrIngestion(unittest.TestCase):

    @patch("builtins.open", new_callable=mock_open, read_data='{"flickr": {}, "kafka": {}, "bloom_filter": {}, "limits": {}}')
    def test_load_config(self, mock_file):
        """Test loading configuration from file."""
        config = load_config("./../config/config.json")
        mock_file.assert_called_once_with("./../config/config.json", "r")
        self.assertIsInstance(config, dict)

    @patch("flickrapi.FlickrAPI")
    def test_initialize_flickr_api(self, mock_flickr_api):
        """Test initializing Flickr API."""
        config = {"flickr": {"api_key": "dummy_key", "api_secret": "dummy_secret"}}
        flickr = initialize_flickr_api(config)
        mock_flickr_api.assert_called_once_with("dummy_key", "dummy_secret", format='parsed-json')
        self.assertIsNotNone(flickr)

    @patch("flickr_ingestion.Producer")
    def test_initialize_kafka_producer(self, mock_producer):
        """Test initializing Kafka Producer."""
        config = {"kafka": {"bootstrap_servers": "localhost:9092"}}
        producer = initialize_kafka_producer(config)
        mock_producer.assert_called_once_with({'bootstrap.servers': 'localhost:9092'})
        self.assertIsNotNone(producer)

    @patch("builtins.open", new_callable=mock_open)
    @patch("pickle.load", return_value="dummy_bloom_filter")
    def test_load_existing_bloom_filter(self, mock_pickle_load, mock_file):
        """Test loading an existing Bloom filter from file."""
        config = {"bloom_filter": {"initial_capacity": 1000, "error_rate": 0.01}}
        bloom_filter = load_bloom_filter("dummy_file.bf", config)
        mock_file.assert_called_once_with("dummy_file.bf", "rb")
        mock_pickle_load.assert_called_once()
        self.assertEqual(bloom_filter, "dummy_bloom_filter")

    @patch("builtins.open", new_callable=mock_open)
    @patch("pickle.dump")
    def test_save_bloom_filter(self, mock_pickle_dump, mock_file):
        """Test saving a Bloom filter to file."""
        bloom_filter = MagicMock()
        save_bloom_filter(bloom_filter, "dummy_file.bf")
        mock_file.assert_called_once_with("dummy_file.bf", "wb")
        mock_pickle_dump.assert_called_once_with(bloom_filter, mock_file())

    @patch("builtins.open", new_callable=mock_open)
    def test_save_min_taken_date(self, mock_file):
        """Test saving the minimum taken date."""
        last_response = {
            'photos': {
                'photo': [{'datetaken': '2022-01-01 00:00:00'}]
            }
        }
        save_min_taken_date(last_response, "dummy_date_file.txt")
        mock_file.assert_called_once_with("dummy_date_file.txt", "w")
        mock_file().write.assert_called_once_with("2022-01-01 00:00:00")

    @patch("builtins.open", new_callable=mock_open, read_data="2022-01-01 00:00:00")
    def test_load_min_taken_date(self, mock_file):
        """Test loading the minimum taken date from file."""
        min_date = load_min_taken_date("dummy_date_file.txt", "2020-01-01 00:00:00")
        mock_file.assert_called_once_with("dummy_date_file.txt", "r")
        self.assertEqual(min_date, "2022-01-01 00:00:00")

    def test_handle_api_error_code_retryable(self):
        """Test handling retryable API error_log codes."""
        result = handle_api_error_code(10)  # Retryable error_log
        self.assertTrue(result)

    def test_handle_api_error_code_non_retryable(self):
        """Test handling non-retryable API error_log codes."""
        result = handle_api_error_code(100)  # Non-retryable error_log
        self.assertFalse(result)


if __name__ == '__main__':
    unittest.main()
