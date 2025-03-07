#!/bin/sh
#
# Copyright 2024 PingCAP, Inc. Licensed under Apache-2.0.

set -eu

export DUMPLING_TEST_PORT=4000

run_sql "drop database if exists partition_table"
run_sql "create database partition_table DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin"
export DUMPLING_TEST_DATABASE=partition_table

for data in "$DUMPLING_BASE_NAME"/data/*; do
  run_sql_file "$data"
done

run_dumpling

for file_path in "$DUMPLING_BASE_NAME"/data/*; do
  base_name=$(basename "$file_path")
  table_name="${base_name%.sql}"
  file_should_exist "$DUMPLING_BASE_NAME/result/$table_name.sql"
  file_should_exist "$DUMPLING_OUTPUT_DIR/partition_table.$table_name.000000000.sql"
  file_should_exist "$DUMPLING_OUTPUT_DIR/partition_table.$table_name-schema.sql"
  diff "$DUMPLING_BASE_NAME/result/$table_name.sql" "$DUMPLING_OUTPUT_DIR/partition_table.$table_name.000000000.sql"
  diff "$DUMPLING_BASE_NAME/result/$table_name-schema.sql" "$DUMPLING_OUTPUT_DIR/partition_table.$table_name-schema.sql"
done
