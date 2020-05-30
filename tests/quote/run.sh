#!/bin/sh

set -eu

db="quo\`te-database"
run_sql "drop database if exists \`quo\`\`te-database\`"
run_sql_file "$DUMPLING_BASE_NAME/data/quo\`te-database-schema-create.sql"
export DUMPLING_TEST_DATABASE=$db

run_sql_file "$DUMPLING_BASE_NAME/data/quo\`te-database.quo\`te-table-schema.sql"
run_sql_file "$DUMPLING_BASE_NAME/data/quo\`te-database.quo\`te-table.0.sql"

run_dumpling

for file_path in "$DUMPLING_BASE_NAME"/data/*; do
  base_name=$(basename "$file_path")
  file_should_exist "$DUMPLING_BASE_NAME/data/$base_name"
  file_should_exist "$DUMPLING_OUTPUT_DIR/$base_name"
  diff "$DUMPLING_BASE_NAME/data/$base_name" "$DUMPLING_OUTPUT_DIR/$base_name"
done
