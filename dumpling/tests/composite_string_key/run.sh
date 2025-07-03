#!/bin/sh
#
# Copyright 2024 PingCAP, Inc. Licensed under Apache-2.0.

set -eu

run_sql "drop database if exists composite_string_key"
run_sql "create database composite_string_key DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin"
export DUMPLING_TEST_DATABASE=composite_string_key

for data in "$DUMPLING_BASE_NAME"/data/*; do
  run_sql_file "$data"
done

# Run dumpling with --rows parameter to force chunking
run_dumpling --rows 5

for file_path in "$DUMPLING_BASE_NAME"/data/*; do
  base_name=$(basename "$file_path")
  table_name="${base_name%.sql}"
  
  # For chunked tables, there might be multiple files
  # Check that at least one chunk file exists
  file_should_exist "$DUMPLING_OUTPUT_DIR/composite_string_key.$table_name.000000000.sql"
  
  # Combine all chunk files and compare with expected result
  combined_file="$DUMPLING_OUTPUT_DIR/combined_$table_name.sql"
  cat "$DUMPLING_OUTPUT_DIR"/composite_string_key.$table_name.*.sql > "$combined_file"
  
  # Remove the SQL headers from combined file for comparison
  sed '/^\/\*!/d' "$combined_file" > "$combined_file.clean"
  sed '/^\/\*!/d' "$DUMPLING_BASE_NAME/result/$table_name.sql" > "$DUMPLING_BASE_NAME/result/$table_name.sql.clean"
  
  # Compare the cleaned files (ignore trailing whitespace differences)
  diff -b "$DUMPLING_BASE_NAME/result/$table_name.sql.clean" "$combined_file.clean"
  
  # Cleanup
  rm -f "$combined_file" "$combined_file.clean" "$DUMPLING_BASE_NAME/result/$table_name.sql.clean"
done