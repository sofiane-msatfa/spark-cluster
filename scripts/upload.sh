#!/bin/bash

# Répertoires
OUTPUT_DIR="$HOME/data/output"
COMPLETED_DIR="$OUTPUT_DIR/completed"
ERROR_DIR="$OUTPUT_DIR/error"
HDFS_DATA_DIR="/staging"

# Function to create directories if they don't exist
create_directories() {
    mkdir -p "$OUTPUT_DIR"
    mkdir -p "$COMPLETED_DIR"
    mkdir -p "$ERROR_DIR"
}

create_hdfs_data_directory() {
    if ! hdfs dfs -mkdir -p "$HDFS_DATA_DIR"; then
        echo "Failed to create HDFS data directory $HDFS_DATA_DIR"
        return 1
    fi
    return 0
}

process_csv_file() {
    local csv_file=$1
    hdfs dfs -put "$csv_file" $HDFS_DATA_DIR

    if [ $? -eq 0 ]; then
        mv "$csv_file" "$COMPLETED_DIR"
    else
        mv "$csv_file" "$ERROR_DIR"
    fi
}

# Main script execution
create_directories
create_hdfs_data_directory

for csv_file in "$OUTPUT_DIR"/*.csv; do
    process_csv_file "$csv_file"
done

echo "Tous les fichiers CSV ont été traités avec succès"
exit 0
