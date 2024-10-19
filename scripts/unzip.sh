#!/bin/bash

# Define the source, destination, and done directories
OUTPUT_DIR="$HOME/data/output"
INPUT_DIR="$HOME/data/input"
COMPLETED_DIR="$INPUT_DIR/completed"
ERROR_DIR="$INPUT_DIR/error"

# Function to create directories if they don't exist
create_directories() {
    mkdir -p "$OUTPUT_DIR"
    mkdir -p "$COMPLETED_DIR"
    mkdir -p "$ERROR_DIR"
}

# Function to process a single zip file
process_zip_file() {
    local zip_file=$1
    local base_name=$(basename "$zip_file" .zip)
    local temp_dir=$(mktemp -d)
    
    unzip "$zip_file" -d "$temp_dir"

    find "$temp_dir" -name "*.csv" -exec mv {} "$OUTPUT_DIR"/$base_name.csv \;
    
    if [ $? -eq 0 ]; then
        mv "$zip_file" "$COMPLETED_DIR"
    else
        mv "$zip_file" "$ERROR_DIR"
    fi
    
    rm -rf "$temp_dir"
}

# Main script execution
create_directories

for zip_file in "$INPUT_DIR"/*.zip; do
    process_zip_file "$zip_file"
done

echo "Tous les fichiers ZIP ont été traités avec succès"
exit 0