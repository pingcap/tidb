#!/bin/sh

set -eu

run_sql "drop database if exists views"
run_sql "create database views"
export DUMPLING_TEST_DATABASE="views"

run_sql "create table t (a bigint, b varchar(255))"
run_sql "create definer = 'root'@'localhost' view v as select * from t;"
# insert 20 records to `t`.
i=0; while [ $i -lt 20 ]; do
  run_sql "insert into t values ($i, \"$i\")"
  i=$(( i + 1 ))
done

run_dumpling --no-views
file_not_exist "$DUMPLING_OUTPUT_DIR/views.v-schema.sql"

run_dumpling --no-views=false
diff "$DUMPLING_BASE_NAME/data/views-schema-create.sql" "$DUMPLING_OUTPUT_DIR/views-schema-create.sql"
diff "$DUMPLING_BASE_NAME/data/views.v-schema.sql" "$DUMPLING_OUTPUT_DIR/views.v-schema.sql"
