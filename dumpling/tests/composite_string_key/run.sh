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
# With --rows 5, tables will be split into multiple chunks
# Each chunk should contain a complete INSERT statement with ~5 rows
run_dumpling --rows 5

for file_path in "$DUMPLING_BASE_NAME"/data/*; do
  base_name=$(basename "$file_path")
  table_name="${base_name%.sql}"
  
  # Count the number of chunk files created
  chunk_count=$(ls -1 "$DUMPLING_OUTPUT_DIR"/composite_string_key.$table_name.*.sql 2>/dev/null | wc -l)
  
  # Determine expected chunks based on table row counts
  # With --rows 5:
  # comp_str_case_0: 10 rows -> 2 chunks
  # comp_str_case_1: 10 rows -> 2 chunks  
  # comp_str_case_2: 12 rows -> 3 chunks
  # comp_str_case_3: 12 rows -> 3 chunks
  case "$table_name" in
    "comp_str_case_0" | "comp_str_case_1")
      expected_chunks=2
      ;;
    "comp_str_case_2" | "comp_str_case_3")
      expected_chunks=3
      ;;
    *)
      echo "ERROR: Unknown table $table_name"
      exit 1
      ;;
  esac
  
  if [ "$chunk_count" -ne "$expected_chunks" ]; then
    echo "ERROR: Expected $expected_chunks chunks for $table_name, but found $chunk_count"
    exit 1
  fi
  
  # Compare each chunk file with the expected result file
  for chunk_file in "$DUMPLING_OUTPUT_DIR"/composite_string_key.$table_name.*.sql; do
    chunk_basename=$(basename "$chunk_file")
    # Extract the chunk number from the filename (e.g., composite_string_key.comp_str_case_0.000000000.sql -> comp_str_case_0.000000000.sql)
    expected_file="${chunk_basename#composite_string_key.}"
    
    if [ ! -f "$DUMPLING_BASE_NAME/result/$expected_file" ]; then
      echo "ERROR: Expected result file $DUMPLING_BASE_NAME/result/$expected_file not found"
      exit 1
    fi
    
    # Compare the chunk with expected result
    if ! diff -B -w "$chunk_file" "$DUMPLING_BASE_NAME/result/$expected_file"; then
      echo "ERROR: Chunk file $chunk_file does not match expected result $expected_file"
      exit 1
    fi
  done
  
  echo "Table $table_name: Successfully validated $chunk_count chunks"
done