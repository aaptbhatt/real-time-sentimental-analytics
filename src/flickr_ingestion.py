import flickrapi
import json
import time
import logging
import pickle
from confluent_kafka import Producer
from pybloom_live import ScalableBloomFilter
from utils import setup_logging

def load_config(config_path="./../config/config.json"):
    """Load configuration from JSON file."""
    with open(config_path, "r") as config_file:
        return json.load(config_file)


def initialize_flickr_api(config):
    """Initialize the Flickr API with API key and secret."""
    return flickrapi.FlickrAPI(config['flickr']['api_key'], config['flickr']['api_secret'], format='parsed-json')


def initialize_kafka_producer(config):
    """Initialize Kafka producer with the given configuration."""
    return Producer({'bootstrap.servers': config['kafka']['bootstrap_servers']})


def load_bloom_filter(bloom_filter_file, config):
    """Load or create a new Bloom filter for deduplication."""
    try:
        with open(bloom_filter_file, "rb") as bf_file:
            logging.info("Loaded existing Bloom filter.")
            return pickle.load(bf_file)
    except (FileNotFoundError, pickle.UnpicklingError):
        logging.info("Created new Bloom filter or reset corrupted Bloom filter.")
        return ScalableBloomFilter(
            initial_capacity=config['bloom_filter']['initial_capacity'],
            error_rate=config['bloom_filter']['error_rate']
        )


def save_bloom_filter(bloom_filter, bloom_filter_file):
    """Save Bloom filter to a file."""
    with open(bloom_filter_file, "wb") as bf_file:
        pickle.dump(bloom_filter, bf_file)
    logging.info("Bloom filter saved to disk.")


def save_min_taken_date(last_response, date_file_path="last_min_taken_date.txt"):
    """Save the last date used for filtering to a file."""
    if last_response and 'photos' in last_response and last_response['photos']['photo']:
        last_photo_date = last_response['photos']['photo'][-1]['datetaken']
        with open(date_file_path, "w") as date_file:
            date_file.write(last_photo_date)
            logging.info(f"Updated min_taken_date to {last_photo_date}")


def load_min_taken_date(date_file_path, default_min_taken_date):
    """Load the minimum taken date from file, or use the default if file is missing."""
    try:
        with open(date_file_path, "r") as date_file:
            min_taken_date = date_file.read().strip()
            logging.info(f"Loaded min_taken_date from file: {min_taken_date}")
            return min_taken_date
    except FileNotFoundError:
        logging.info(f"No previous min_taken_date found. Using default: {default_min_taken_date}")
        return default_min_taken_date


def delivery_report(err, msg):
    """Callback function for Kafka message delivery."""
    if err:
        logging.error(f'Message delivery failed: {err}')
    else:
        logging.info(f'Message delivered to {msg.topic()} [{msg.partition()}]')


def handle_api_error_code(error_code):
    """Handle Flickr API error_log codes and determine whether to retry."""
    retryable_errors = [10, 105, 106]  # Retryable errors
    non_retryable_errors = [100, 111, 112, 114, 115, 116]  # Non-retryable errors

    if error_code in retryable_errors:
        logging.warning(f"API error_log {error_code}: Retryable error_log encountered.")
        return True
    elif error_code in non_retryable_errors:
        logging.error(f"API error_log {error_code}: Non-retryable error_log encountered.")
        return False
    else:
        logging.error(f"API error_log {error_code}: Unhandled error_log encountered.")
        return False


def fetch_photos_with_retry(flickr, bloom_filter, producer, config, min_taken_date):
    """Fetch photos from Flickr API with retry mechanism and handle the results."""
    for attempt in range(1, config['limits']['retry_attempts'] + 1):
        try:
            logging.info(f"Attempt {attempt} of {config['limits']['retry_attempts']} to fetch photos.")
            response = flickr.photos.search(
                lat=config['flickr']['lat'],
                lon=config['flickr']['lon'],
                radius=config['flickr']['radius'],
                geo_context=config['flickr']['geo_context'],
                extras=config['flickr']['extras'],
                sort=config['flickr']['sort'],
                min_taken_date=min_taken_date
            )

            if response.get('stat') == 'fail':
                if not handle_api_error_code(response['code']) or attempt == config['limits']['retry_attempts']:
                    return None
                time.sleep(config['limits']['retry_delay_seconds'])
                continue

            process_photos(response, bloom_filter, producer, config)
            return response

        except Exception as e:
            logging.error(f"Error fetching photos: {e}")
            if attempt < config['limits']['retry_attempts']:
                time.sleep(config['limits']['retry_delay_seconds'])
            else:
                logging.error("All retry attempts failed.")
                return None


def process_photos(response, bloom_filter, producer, config):
    """Process photos and send new ones to Kafka."""
    if 'photos' in response and 'photo' in response['photos']:
        new_photos_count = 0
        for photo in response['photos']['photo']:
            photo_id = photo['id']
            if photo_id not in bloom_filter:
                photo_event = {
                    'photo_id': photo_id,
                    'owner': photo['owner'],
                    'tags': photo.get('tags', ''),
                    'lat': config['flickr']['lat'],
                    'lon': config['flickr']['lon'],
                    'date_taken': photo.get('datetaken')
                }
                producer.produce(config['kafka']['topic'], value=json.dumps(photo_event), callback=delivery_report)
                producer.flush()
                bloom_filter.add(photo_id)
                new_photos_count += 1
        logging.info(f"Added {new_photos_count} new photos to Kafka.")
    else:
        logging.warning("No photos found in the response.")


def main():
    """Main function to fetch photos from Flickr and send them to Kafka."""
    config = load_config()

    # Setup logging (use a different log file for ingestion)
    setup_logging(config, log_file_name="./../ingestion.log")

    flickr = initialize_flickr_api(config)
    producer = initialize_kafka_producer(config)
    bloom_filter = load_bloom_filter(config['bloom_filter']['bloom_filter_file'], config)

    min_taken_date = load_min_taken_date("./../last_min_taken_date.txt", config['flickr']['default_min_taken_date'])

    api_call_count = 0
    while api_call_count < config['limits']['max_api_calls']:
        last_response = fetch_photos_with_retry(flickr, bloom_filter, producer, config, min_taken_date)
        api_call_count += 1
        time.sleep(config['limits']['api_call_interval'])

    save_bloom_filter(bloom_filter, config['bloom_filter']['bloom_filter_file'])
    save_min_taken_date(last_response)


if __name__ == "__main__":
    main()
