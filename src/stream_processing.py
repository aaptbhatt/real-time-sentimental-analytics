import json
import logging
from pyspark.sql import SparkSession
from pyspark.sql.functions import explode, split, col, window, to_timestamp, from_json
from pyspark.sql.types import StructType, StructField, StringType
from utils import setup_logging

def load_config(config_path="./../config/config.json"):
    """Load configuration from JSON file."""
    try:
        with open(config_path, "r") as config_file:
            logging.info(f"Loading configuration from {config_path}")
            return json.load(config_file)
    except FileNotFoundError as e:
        logging.error(f"Configuration file not found: {e}")
        raise
    except json.JSONDecodeError as e:
        logging.error(f"Error decoding JSON from the configuration file: {e}")
        raise

def initialize_spark(config):
    """Initialize Spark session with Kafka and other necessary configurations."""
    try:
        logging.info("Initializing Spark session.")
        return SparkSession.builder \
            .appName("Top Tags Streaming") \
            .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.2.0") \
            .getOrCreate()
    except Exception as e:
        logging.error(f"Error initializing Spark session: {e}")
        raise

def define_schema():
    """Define the schema for the incoming Kafka JSON data."""
    try:
        logging.info("Defining schema for incoming data.")
        return StructType([
            StructField("photo_id", StringType()),
            StructField("owner", StringType()),
            StructField("tags", StringType()),
            StructField("lat", StringType()),
            StructField("lon", StringType()),
            StructField("date_taken", StringType())
        ])
    except Exception as e:
        logging.error(f"Error defining schema: {e}")
        raise

def read_from_kafka(spark, config):
    """Read streaming data from Kafka based on the configuration."""
    try:
        logging.info(f"Reading data from Kafka topic: {config['kafka']['topic']}")
        return spark.readStream \
            .format("kafka") \
            .option("kafka.bootstrap.servers", config['kafka']['bootstrap_servers']) \
            .option("subscribe", config['kafka']['topic']) \
            .load() \
            .selectExpr("CAST(value AS STRING) as json_str")
    except Exception as e:
        logging.error(f"Error reading data from Kafka: {e}")
        raise

def parse_kafka_data(df, schema):
    """Parse the JSON data coming from Kafka."""
    try:
        logging.info("Parsing Kafka JSON data.")
        return df.select(from_json("json_str", schema).alias("data")).select("data.*")
    except Exception as e:
        logging.error(f"Error parsing Kafka data: {e}")
        raise

def process_tags(df):
    """Process and explode the tags for individual counting."""
    try:
        logging.info("Processing and exploding tags.")
        return df.withColumn("timestamp", to_timestamp("date_taken", "yyyy-MM-dd HH:mm:ss")) \
            .withColumn("tag", explode(split(col("tags"), " ")))
    except Exception as e:
        logging.error(f"Error processing tags: {e}")
        raise

def apply_watermark_and_window(tags_df, config):
    """Apply watermarking and windowing to the data for tag counting."""
    try:
        logging.info("Applying watermark and window functions.")
        watermark_duration = config['streaming']['watermark']
        window_duration = config['streaming']['window_duration']
        slide_interval = config['streaming']['slide_interval']

        return tags_df.withWatermark("timestamp", watermark_duration) \
            .groupBy(
            window(col("timestamp"), window_duration, slide_interval),
            col("lat"),
            col("lon"),
            col("tag")
        ).count().withColumnRenamed("count", "tag_count")
    except Exception as e:
        logging.error(f"Error applying watermark and windowing: {e}")
        raise

def add_window_columns(windowed_tags):
    """Add window_start and window_end columns to the windowed DataFrame."""
    try:
        logging.info("Adding window start and end columns.")
        return windowed_tags.withColumn("window_start", col("window").start.cast("string")) \
            .withColumn("window_end", col("window").end.cast("string"))
    except Exception as e:
        logging.error(f"Error adding window columns: {e}")
        raise

def write_to_console(windowed_tags):
    """Write streaming data to the console for debugging."""
    try:
        logging.info("Writing data to console.")
        return windowed_tags.writeStream.format("console") \
            .outputMode("complete") \
            .option("truncate", False) \
            .start()
    except Exception as e:
        logging.error(f"Error writing data to console: {e}")
        raise

def write_to_parquet(windowed_tags, config):
    """Write the windowed streaming data to Parquet files."""
    try:
        parquet_output_path = config['output']['parquet_path']
        checkpoint_location = config['output']['checkpoint_location']
        logging.info(f"Writing data to Parquet at {parquet_output_path} with checkpointing at {checkpoint_location}.")
        return windowed_tags.writeStream \
            .format("parquet") \
            .outputMode("append") \
            .option("checkpointLocation", checkpoint_location) \
            .start(parquet_output_path)
    except Exception as e:
        logging.error(f"Error writing data to Parquet: {e}")
        raise

def main():
    try:
        # Load the configuration
        config = load_config()

        # Setup logging (use a different log file for streaming)
        setup_logging(config, log_file_name="./../streaming.log")

        # Initialize Spark session
        spark = initialize_spark(config)

        # Define schema for incoming data
        schema = define_schema()

        # Read data from Kafka
        kafka_df = read_from_kafka(spark, config)

        # Parse Kafka JSON data
        parsed_df = parse_kafka_data(kafka_df, schema)

        # Process tags
        tags_df = process_tags(parsed_df)

        # Apply watermark and windowing for tag counting
        windowed_tags = apply_watermark_and_window(tags_df, config)

        # Add window_start and window_end columns
        windowed_tags = add_window_columns(windowed_tags)

        # Optionally write data to console (for debugging)
        write_to_console(windowed_tags)

        # Write the results to Parquet
        parquet_stream = write_to_parquet(windowed_tags, config)

        # Wait for the streaming process to complete
        logging.info("Starting streaming process.")
        parquet_stream.awaitTermination()

    except Exception as e:
        logging.error(f"An unexpected error occurred in the streaming process: {e}")
        raise

if __name__ == "__main__":
    main()
