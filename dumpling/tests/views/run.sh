#!/bin/sh
#
# Copyright 2020 PingCAP, Inc. Licensed under Apache-2.0.

set -eu

run_sql "drop database if exists views"
run_sql_file "$DUMPLING_BASE_NAME/data/views-schema-create.sql"
export DUMPLING_TEST_DATABASE="views"

run_sql "create table t (a bigint, b varchar(255))"
run_sql_file "$DUMPLING_BASE_NAME/data/views.v-schema-view.sql"

# insert 20 records to `t`.
run_sql "insert into t values $(seq -s, 20 | sed 's/,*$//g' | sed 's/[0-9]*/(\0,"\0")/g')"

run_dumpling --no-views
file_not_exist "$DUMPLING_OUTPUT_DIR/views.v-schema.sql"
file_not_exist "$DUMPLING_OUTPUT_DIR/views.v-schema-view.sql"

rm -rf $DUMPLING_OUTPUT_DIR
run_dumpling --no-views=false
#diff "$DUMPLING_BASE_NAME/data/views-schema-create.sql" "$DUMPLING_OUTPUT_DIR/views-schema-create.sql"
diff "$DUMPLING_BASE_NAME/data/views.v-schema.sql" "$DUMPLING_OUTPUT_DIR/views.v-schema.sql"
diff "$DUMPLING_BASE_NAME/data/views.v-schema-view.sql" "$DUMPLING_OUTPUT_DIR/views.v-schema-view.sql"
file_not_exist "$DUMPLING_OUTPUT_DIR/views.v.000000000.sql"

# test --no-schemas
rm -rf $DUMPLING_OUTPUT_DIR
run_dumpling --no-schemas
file_not_exist "$DUMPLING_OUTPUT_DIR/views-schema-create.sql"
file_not_exist "$DUMPLING_OUTPUT_DIR/views.t-schema.sql"
file_not_exist "$DUMPLING_OUTPUT_DIR/views.v-schema.sql"
file_not_exist "$DUMPLING_OUTPUT_DIR/views.v-schema-view.sql"
file_not_exist "$DUMPLING_OUTPUT_DIR/views.v.000000000.sql"
