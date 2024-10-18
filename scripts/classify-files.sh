#!/bin/bash

# Define the source directory
SOURCE_DIR="$HOME/data/output"

# Loop through each CSV file in the source directory
for file in "$SOURCE_DIR"/*.csv; do
    # Extract the base name of the file
    base_name=$(basename "$file")
    
    # Use regex to match the pattern <ANNEE>_<NUMERO_MOIS>
    if [[ $base_name =~ ([0-9]{4})_([0-9]{2})\.csv$ ]]; then
        year=${BASH_REMATCH[1]}
        month=${BASH_REMATCH[2]}

        echo "Classifying $base_name to /data/${year}_${month}"
        
        # Create the target directory in HDFS if it doesn't exist
        target_dir="/data/${year}_${month}"
        hdfs dfs -mkdir -p "$target_dir"
        
        # Copy the file to the target directory in HDFS
        hdfs dfs -copyFromLocal "$file" "$target_dir/"
    fi
done

# TODO: Moove the files to the archive directory