#!/bin/sh
#
# Copyright 2024 PingCAP, Inc. Licensed under Apache-2.0.

set -eu

# Configure TiDB server charset settings
run_sql "set global character_set_server=utf8mb4"
run_sql "set global character_set_client=utf8mb4"
run_sql "set global character_set_connection=utf8mb4"
run_sql "set global character_set_results=utf8mb4"
run_sql "set global collation_server=utf8mb4_bin"
run_sql "set global collation_connection=utf8mb4_bin"

run_sql "drop database if exists composite_string_key"
run_sql "create database composite_string_key DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin"
export DUMPLING_TEST_DATABASE=composite_string_key

for data in "$DUMPLING_BASE_NAME"/data/*; do
  run_sql_file "$data"
done

# Run dumpling with --rows parameter to force chunking
# With --rows 5 and tables containing 10 rows, we expect 2 chunk files
# Each chunk should contain a complete INSERT statement with ~5 rows
run_dumpling --rows 5

for file_path in "$DUMPLING_BASE_NAME"/data/*; do
  base_name=$(basename "$file_path")
  table_name="${base_name%.sql}"
  
  # Count the number of chunk files created
  chunk_count=$(ls -1 "$DUMPLING_OUTPUT_DIR"/composite_string_key.$table_name.*.sql 2>/dev/null | wc -l)
  
  # For tables with 10 rows and --rows 5, we expect 2 chunks
  if [ "$chunk_count" -lt 2 ]; then
    echo "ERROR: Expected at least 2 chunks for $table_name, but found $chunk_count"
    exit 1
  fi
  
  # Verify each chunk file contains a complete INSERT statement
  for chunk_file in "$DUMPLING_OUTPUT_DIR"/composite_string_key.$table_name.*.sql; do
    if ! grep -q "INSERT INTO \`$table_name\` VALUES" "$chunk_file"; then
      echo "ERROR: Chunk file $chunk_file does not contain a complete INSERT statement"
      exit 1
    fi
  done
  
  # Collect all data from chunks and verify completeness
  # This is just to ensure all data is exported, not to verify the format
  temp_file="$DUMPLING_OUTPUT_DIR/temp_$table_name.sql"
  for chunk_file in "$DUMPLING_OUTPUT_DIR"/composite_string_key.$table_name.*.sql; do
    # Extract only the data rows (remove headers and INSERT INTO line)
    sed -n '/INSERT INTO/,/;$/p' "$chunk_file" | sed '1s/INSERT INTO .* VALUES//' | sed 's/;$//' | tr -d '\n' | sed 's/,(/\n(/g' | grep -v '^$' >> "$temp_file"
  done
  
  # Sort both files for comparison (data order might differ between chunks)
  sort "$temp_file" > "$temp_file.sorted"
  sed -n '/INSERT INTO/,/;$/p' "$DUMPLING_BASE_NAME/result/$table_name.sql" | sed '1s/INSERT INTO .* VALUES//' | sed 's/;$//' | tr -d '\n' | sed 's/,(/\n(/g' | grep -v '^$' | sort > "$DUMPLING_BASE_NAME/result/$table_name.sorted"
  
  # Compare sorted data to ensure all rows are exported
  if ! diff -b "$temp_file.sorted" "$DUMPLING_BASE_NAME/result/$table_name.sorted"; then
    echo "ERROR: Data mismatch for $table_name"
    exit 1
  fi
  
  # Cleanup
  rm -f "$temp_file" "$temp_file.sorted" "$DUMPLING_BASE_NAME/result/$table_name.sorted"
  
  echo "Table $table_name: Successfully validated $chunk_count chunks"
done