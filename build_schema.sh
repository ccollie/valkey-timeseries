#!/usr/bin/env bash

# Exit the script if any command fails
set -e

SCRIPT_DIR=$(pwd)
SCHEMA_DIR="$SCRIPT_DIR/src/fanout/schema"
FLATC_OUTPUT_DIR="$SCRIPT_DIR/src/fanout/request"

echo "Script Directory: $SCRIPT_DIR"
echo "Schema Directory: $SCHEMA_DIR"

# Check if flatc is installed
if ! command -v flatc &> /dev/null; then
    echo "Error: flatc compiler not found. Please install flatbuffers."
    echo "Visit https://flatbuffers.dev/flatbuffers_guide_building.html for installation instructions."
    exit 1
fi

echo "Found flatc: $(which flatc)"
echo "flatc version: $(flatc --version)"

# Create output directory if it doesn't exist
mkdir -p "$FLATC_OUTPUT_DIR"

# Find all .fbs files in the schema directory
FBS_FILES=$(find "$SCHEMA_DIR" -name "*.fbs" -type f)

if [ -z "$FBS_FILES" ]; then
    echo "No .fbs schema files found in $SCHEMA_DIR"
    exit 0
fi

echo "Found the following schema files:"
echo "$FBS_FILES"
echo

# Compile each schema file
echo "Compiling schema files..."
for file in $FBS_FILES; do
    echo "Processing: $file"
    flatc --rust -o "$FLATC_OUTPUT_DIR" "$file"
done

echo "Schema compilation complete. Generated Rust files in $FLATC_OUTPUT_DIR"