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

# Refresh row statistics so EXPLAIN returns accurate estimates — the
# streaming chunker falls back to direct COUNT(*) when EXPLAIN under-
# estimates, but running ANALYZE up-front keeps this integration test
# deterministic regardless of MySQL/InnoDB stats-refresh timing.
for table in comp_str_case_0 comp_str_case_1 comp_str_case_2 comp_str_case_3; do
  run_sql "analyze table composite_string_key.$table"
done

# Run dumpling with --rows parameter to force chunking
# With --rows 5, tables will be split into multiple chunks
# Each chunk should contain a complete INSERT statement with ~5 rows
run_dumpling --rows 5

for file_path in "$DUMPLING_BASE_NAME"/data/*; do
  base_name=$(basename "$file_path")
  table_name="${base_name%.sql}"

  # Count chunk files via find so the argument list handles odd characters
  # and we don't depend on `ls` output shape (shellcheck SC2012).
  chunk_count=$(find "$DUMPLING_OUTPUT_DIR" -maxdepth 1 \
      -name "composite_string_key.${table_name}.[0-9]*.sql" | wc -l | tr -d ' ')

  # Determine expected chunks based on table row counts.
  # With --rows 5:
  #   comp_str_case_0: 10 rows -> 2 chunks
  #   comp_str_case_1: 10 rows -> 2 chunks
  #   comp_str_case_2: 12 rows -> 3 chunks
  #   comp_str_case_3: 12 rows -> 3 chunks
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

  # Compare each chunk file with its expected fixture. Use plain `diff`
  # (no -B / -w) so whitespace-sensitive regressions — a dropped newline
  # at a chunk boundary, a rogue space in a quoted value, a mis-escape —
  # surface instead of being silently ignored.
  find "$DUMPLING_OUTPUT_DIR" -maxdepth 1 \
      -name "composite_string_key.${table_name}.[0-9]*.sql" -print0 \
      | while IFS= read -r -d '' chunk_file; do
    chunk_basename=$(basename "$chunk_file")
    expected_file="${chunk_basename#composite_string_key.}"

    if [ ! -f "$DUMPLING_BASE_NAME/result/$expected_file" ]; then
      echo "ERROR: Expected result file $DUMPLING_BASE_NAME/result/$expected_file not found"
      exit 1
    fi

    if ! diff "$chunk_file" "$DUMPLING_BASE_NAME/result/$expected_file"; then
      echo "ERROR: Chunk file $chunk_file does not match expected result $expected_file"
      exit 1
    fi
  done

  echo "Table $table_name: Successfully validated $chunk_count chunks"
done