#!/bin/bash

# Exit immediately if a command exits with a non-zero status
set -e

# Function to handle errors
error_handler() {
    echo "Error occurred in script at line: $1"
    TIMESTAMP=$(date +"%Y%m%d_%H%M%S")
    echo "Saving error log to error_log/error_${TIMESTAMP}.txt"
    mv error_log/error_log.txt error_log/error_${TIMESTAMP}.txt
}

# Trap any error_log, and call the error_log handler
trap 'error_handler $LINENO' ERR

echo "--------Run flickr_ingestion.py----------"
#python3 src/flickr_ingestion 2> error_log/error_log.txt
python3 flickr_ingestion.py

sleep 5

echo "--------Run stream_processing.py----------"
#python3 src/stream_processing.py 2>> error_log/error_log.txt
python3 stream_processing.py
sleep 60

echo "--------Run presentation_layer.py----------"
python3 presentation_layer.py


echo "All scripts executed successfully!"
