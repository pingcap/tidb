#!/bin/sh

set -eu

mkdir -p "$DUMPLING_OUTPUT_DIR"/data
cp "$DUMPLING_BASE_NAME/data/quote-database.quote-table.0.sql" "$DUMPLING_OUTPUT_DIR/data/quo\`te%2Fdatabase.quo\`te%2Ftable.0.sql"
cp "$DUMPLING_BASE_NAME/data/quote-database.quote-table-schema.sql" "$DUMPLING_OUTPUT_DIR/data/quo\`te%2Fdatabase.quo\`te%2Ftable-schema.sql"
cp "$DUMPLING_BASE_NAME/data/quote-database-schema-create.sql" "$DUMPLING_OUTPUT_DIR/data/quo\`te%2Fdatabase-schema-create.sql"

db="quo\`te/database"
run_sql "drop database if exists \`quo\`\`te/database\`"
run_sql_file "$DUMPLING_OUTPUT_DIR/data/quo\`te%2Fdatabase-schema-create.sql"
export DUMPLING_TEST_DATABASE=$db

run_sql_file "$DUMPLING_OUTPUT_DIR/data/quo\`te%2Fdatabase.quo\`te%2Ftable-schema.sql"
run_sql_file "$DUMPLING_OUTPUT_DIR/data/quo\`te%2Fdatabase.quo\`te%2Ftable.0.sql"

run_dumpling

for file_path in "$DUMPLING_OUTPUT_DIR"/data/*; do
  base_name=$(basename "$file_path")
  file_should_exist "$DUMPLING_OUTPUT_DIR/data/$base_name"
  file_should_exist "$DUMPLING_OUTPUT_DIR/$base_name"
  diff "$DUMPLING_OUTPUT_DIR/data/$base_name" "$DUMPLING_OUTPUT_DIR/$base_name"
done
