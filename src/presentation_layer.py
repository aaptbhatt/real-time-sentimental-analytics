import json
from pyspark.sql import SparkSession
import logging


def load_config(config_path="./../config/config.json"):
    """Load configuration from JSON file."""
    with open(config_path, "r") as config_file:
        return json.load(config_file)


def initialize_spark():
    """Initialize the Spark session."""
    return SparkSession.builder \
        .appName("Read Parquet Data") \
        .getOrCreate()


def read_parquet_data(spark, parquet_file_path):
    """Read Parquet files into a Spark DataFrame."""
    logging.info(f"Reading Parquet files from {parquet_file_path}")
    return spark.read.parquet(parquet_file_path)


def register_temp_view(df, view_name="photo_tags"):
    """Register DataFrame as a temporary view for SQL queries."""
    logging.info(f"Registering DataFrame as temporary view: {view_name}")
    df.createOrReplaceTempView(view_name)


def execute_query(spark):
    """Execute SQL query to get top 5 tags for each location."""
    query = """
        SELECT window_start, lat, lon, tag, tag_count
        FROM (
            SELECT *, ROW_NUMBER() OVER (PARTITION BY window_start, lat, lon ORDER BY tag_count DESC) as rank
            FROM photo_tags
        ) ranked
        WHERE rank <= 5
        ORDER BY lat, lon, window_start DESC
    """
    logging.info("Executing SQL query to find top 5 tags per location")
    return spark.sql(query)


def write_results_to_csv(result_df, output_path):
    """Write the result DataFrame to CSV format."""
    logging.info(f"Writing results to {output_path}")
    result_df.write.mode("overwrite").csv(output_path)


def display_results(result_df):
    """Display the query results on the console."""
    logging.info("Displaying query results on the console")
    result_df.show(truncate=False)


def main():
    # Initialize logging
    logging.basicConfig(level=logging.INFO)

    # Load the configuration file
    config = load_config()

    # Initialize Spark session
    spark = initialize_spark()

    # Read Parquet files into a DataFrame
    parquet_file_path = config['output']['parquet_path']
    df = read_parquet_data(spark, parquet_file_path)

    # Show the schema of the DataFrame for debugging
    logging.info("Showing DataFrame schema")
    df.printSchema()

    # Register DataFrame as a temporary view
    register_temp_view(df)

    # Execute SQL query to get top 5 tags for each location
    result_df = execute_query(spark)

    # Display the result on the console
    display_results(result_df)

    # Write the result DataFrame to CSV
    final_output_path = config['output']['final_results_path']
    write_results_to_csv(result_df, final_output_path)


if __name__ == "__main__":
    main()
