#!/bin/bash
#
# Copyright 2020 PingCAP, Inc. Licensed under Apache-2.0.

set -eu

mkdir -p "$DUMPLING_OUTPUT_DIR"/data

mysql_version=$(echo "select version()" | mysql -uroot -h127.0.0.1 -P3306 | awk 'NR==2' | awk '{print $1}')
echo "current user mysql version is $mysql_version"
if [[ $mysql_version = 5* ]]; then
  # there is a bug in mysql 5.x, see https://bugs.mysql.com/bug.php?id=96994, so we use different create db/table sql
  cp "$DUMPLING_BASE_NAME/data/quote-database.quote-table.000000000-mysql57.sql" "$DUMPLING_OUTPUT_DIR/data/quo\`te%2Fdatabase.quo\`te%2Ftable.000000000.sql"
  cp "$DUMPLING_BASE_NAME/data/quote-database.quote-table-schema-mysql57.sql" "$DUMPLING_OUTPUT_DIR/data/quo\`te%2Fdatabase.quo\`te%2Ftable-schema.sql"
  cp "$DUMPLING_BASE_NAME/data/quote-database-schema-create-mysql57.sql" "$DUMPLING_OUTPUT_DIR/data/quo\`te%2Fdatabase-schema-create.sql"
else
  cp "$DUMPLING_BASE_NAME/data/quote-database.quote-table.000000000.sql" "$DUMPLING_OUTPUT_DIR/data/quo\`te%2Fdatabase.quo\`te%2Ftable.000000000.sql"
  cp "$DUMPLING_BASE_NAME/data/quote-database.quote-table-schema.sql" "$DUMPLING_OUTPUT_DIR/data/quo\`te%2Fdatabase.quo\`te%2Ftable-schema.sql"
  cp "$DUMPLING_BASE_NAME/data/quote-database-schema-create.sql" "$DUMPLING_OUTPUT_DIR/data/quo\`te%2Fdatabase-schema-create.sql"
fi

db="quo\`te/database"
run_sql "drop database if exists \`quo\`\`te/database\`"
run_sql_file "$DUMPLING_OUTPUT_DIR/data/quo\`te%2Fdatabase-schema-create.sql"
export DUMPLING_TEST_DATABASE=$db

run_sql_file "$DUMPLING_OUTPUT_DIR/data/quo\`te%2Fdatabase.quo\`te%2Ftable-schema.sql"
run_sql_file "$DUMPLING_OUTPUT_DIR/data/quo\`te%2Fdatabase.quo\`te%2Ftable.000000000.sql"

run_dumpling

for file_path in "$DUMPLING_OUTPUT_DIR"/data/*; do
  base_name=$(basename "$file_path")
  file_should_exist "$DUMPLING_OUTPUT_DIR/data/$base_name"
  file_should_exist "$DUMPLING_OUTPUT_DIR/$base_name"
  diff "$DUMPLING_OUTPUT_DIR/data/$base_name" "$DUMPLING_OUTPUT_DIR/$base_name"
done
