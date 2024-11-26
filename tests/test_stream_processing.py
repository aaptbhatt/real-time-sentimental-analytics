import unittest
from unittest.mock import patch, MagicMock
from pyspark.sql import SparkSession
from stream_processing import (initialize_spark, load_config, define_schema,
                               read_from_kafka, parse_kafka_data, process_tags,
                               apply_watermark_and_window, add_window_columns, write_to_console, write_to_parquet)


class TestStreamProcessing(unittest.TestCase):

    @patch('builtins.open', unittest.mock.mock_open(read_data='{"kafka": {"bootstrap_servers": "localhost:9092", "topic": "test_topic"}}'))
    def test_load_config(self):
        """Test loading configuration from JSON file."""
        config = load_config('./../config/config.json')
        self.assertIn('kafka', config)
        self.assertEqual(config['kafka']['bootstrap_servers'], 'localhost:9092')

    def test_define_schema(self):
        """Test schema definition."""
        schema = define_schema()
        self.assertIsNotNone(schema)
        self.assertEqual(len(schema), 6)  # Check that the schema has the correct number of fields

    # @patch('stream_processing.SparkSession.readStream')
    # def test_read_from_kafka(self, mock_read_stream):
    #     """Test reading from Kafka."""
    #     mock_df = MagicMock()
    #     mock_read_stream.return_value = mock_df
    #
    #     spark = MagicMock(SparkSession)
    #     config = {"kafka": {"bootstrap_servers": "localhost:9092", "topic": "test_topic"}}
    #     df = read_from_kafka(spark, config)
    #
    #     mock_read_stream.format.assert_called_once_with('kafka')
    #     mock_read_stream.option.assert_any_call("kafka.bootstrap.servers", "localhost:9092")
    #     mock_read_stream.option.assert_any_call("subscribe", "test_topic")
    #     self.assertEqual(df, mock_df)

    @patch('stream_processing.from_json')
    def test_parse_kafka_data(self, mock_from_json):
        """Test parsing Kafka data."""
        mock_df = MagicMock()
        mock_parsed_df = MagicMock()
        mock_from_json.return_value = mock_parsed_df

        schema = define_schema()
        result_df = parse_kafka_data(mock_df, schema)

        mock_df.select.assert_called_once()
        self.assertEqual(result_df, mock_df.select().select())

    # @patch('stream_processing.split')
    # @patch('stream_processing.explode')
    # @patch('stream_processing.to_timestamp')
    # def test_process_tags(self, mock_to_timestamp, mock_explode, mock_split):
    #     """Test processing and exploding tags."""
    #     mock_df = MagicMock()
    #
    #     # Ensure chaining of withColumn calls returns itself
    #     mock_df.withColumn.return_value = mock_df
    #
    #     processed_df = process_tags(mock_df)
    #
    #     mock_df.withColumn.assert_called()
    #     self.assertEqual(processed_df, mock_df)

    @patch('stream_processing.window')
    @patch('stream_processing.col')
    def test_apply_watermark_and_window(self, mock_col, mock_window):
        """Test applying watermark and windowing to tags."""
        mock_df = MagicMock()
        config = {
            'streaming': {
                'watermark': '5 minutes',
                'window_duration': '10 minutes',
                'slide_interval': '2 minutes'
            }
        }

        mock_df.withWatermark.return_value = mock_df
        mock_df.groupBy.return_value = mock_df
        mock_df.count.return_value = mock_df
        mock_df.withColumnRenamed.return_value = mock_df

        result_df = apply_watermark_and_window(mock_df, config)
        mock_df.withWatermark.assert_called_once_with("timestamp", "5 minutes")
        mock_df.groupBy.assert_called_once()
        self.assertEqual(result_df, mock_df)

    @patch('stream_processing.col')
    def test_add_window_columns(self, mock_col):
        """Test adding window_start and window_end columns."""
        mock_df = MagicMock()

        # Chaining `withColumn` and returning `mock_df`
        mock_df.withColumn.return_value = mock_df

        windowed_df = add_window_columns(mock_df)

        mock_df.withColumn.assert_called()
        self.assertEqual(windowed_df, mock_df)

    # @patch('stream_processing.write_to_console')
    # def test_write_to_console(self, mock_write_to_console):
    #     """Test writing the output to the console."""
    #     mock_df = MagicMock()
    #
    #     # Mocking the chaining of PySpark writeStream calls
    #     mock_df.writeStream.format.return_value = mock_df.writeStream  # Mock chaining for format
    #     mock_df.writeStream.start.return_value = mock_df.writeStream  # Mock chaining for start
    #
    #     write_to_console(mock_df)
    #     mock_df.writeStream.format.assert_called_once_with("console")
    #     mock_df.writeStream.start.assert_called_once()

    # @patch('stream_processing.write_to_parquet')
    # def test_write_to_parquet(self, mock_write_to_parquet):
    #     """Test writing output to parquet with checkpointing."""
    #     mock_df = MagicMock()
    #     config = {
    #         'output': {
    #             'parquet_path': './output/parquet',
    #             'checkpoint_location': './output/checkpoint'
    #         }
    #     }
    #
    #     # Mocking the chaining of PySpark writeStream calls
    #     mock_df.writeStream.format.return_value = mock_df.writeStream  # Mock chaining for format
    #     mock_df.writeStream.option.return_value = mock_df.writeStream  # Mock chaining for option
    #     mock_df.writeStream.start.return_value = mock_df.writeStream  # Mock chaining for start
    #
    #     write_to_parquet(mock_df, config)
    #     mock_df.writeStream.format.assert_called_once_with("parquet")
    #     mock_df.writeStream.option.assert_any_call("checkpointLocation", './output/checkpoint')
    #     mock_df.writeStream.start.assert_called_once()


if __name__ == "__main__":
    unittest.main()
