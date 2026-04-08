#!/bin/sh
#
# Copyright 2024 PingCAP, Inc. Licensed under Apache-2.0.

set -eu
cur=$(cd `dirname $0`; pwd)

DB_NAME="specified_table_view"
TABLE_NAME="t"
VIEW_NAME="v"

run_sql "drop database if exists \`$DB_NAME\`;"
run_sql "create database \`$DB_NAME\`;"
run_sql "create table \`$DB_NAME\`.\`$TABLE_NAME\` (a int);"
run_sql "create view \`$DB_NAME\`.\`$VIEW_NAME\` as select * from \`$DB_NAME\`.\`$TABLE_NAME\`;"

set +e
rm -rf "$DUMPLING_OUTPUT_DIR"
run_dumpling --consistency=lock -T="$DB_NAME.$TABLE_NAME,$DB_NAME.$VIEW_NAME" -L ${DUMPLING_OUTPUT_DIR}/dumpling.log
set -e

file_should_exist "$DUMPLING_OUTPUT_DIR/$DB_NAME.$TABLE_NAME-schema.sql"
file_should_exist "$DUMPLING_OUTPUT_DIR/$DB_NAME.$VIEW_NAME-schema-view.sql"

set +e
rm -rf "$DUMPLING_OUTPUT_DIR"
run_dumpling --consistency=lock -T="$DB_NAME.$TABLE_NAME,$DB_NAME.$VIEW_NAME" -L ${DUMPLING_OUTPUT_DIR}/dumpling.log
set -e

file_should_exist "$DUMPLING_OUTPUT_DIR/$DB_NAME.$TABLE_NAME-schema.sql"
file_should_exist "$DUMPLING_OUTPUT_DIR/$DB_NAME.$VIEW_NAME-schema-view.sql"
