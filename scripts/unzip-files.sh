#!/bin/bash

# Define the source, destination, and done directories
SOURCE_DIR="$HOME/data/pending"
DEST_DIR="$HOME/data/output"
DONE_DIR="$HOME/data/done"

# Create the destination and done directories if they don't exist
mkdir -p "$DEST_DIR"
mkdir -p "$DONE_DIR"

# Loop through all zip files in the source directory
for zip_file in "$SOURCE_DIR"/*.zip; do
    # Get the base name of the zip file (without extension)
    base_name=$(basename "$zip_file" .zip)
    
    # Create a temporary directory to unzip the files
    temp_dir=$(mktemp -d)
    
    # Unzip the file into the temporary directory
    unzip "$zip_file" -d "$temp_dir"
    
    # Find the Excel file in the temporary directory
    excel_file=$(find "$temp_dir" -name "*.csv" -print -quit)
    
    # If an Excel file is found, move and rename it
    if [[ -n "$excel_file" ]]; then
        mv "$excel_file" "$DEST_DIR/$base_name.csv"
    fi
    
    # Remove the temporary directory
    rm -rf "$temp_dir"
    
    # Move the processed zip file to the done directory
    mv "$zip_file" "$DONE_DIR/"
done