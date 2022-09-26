#!/bin/sh
#
# Copyright 2020 PingCAP, Inc. Licensed under Apache-2.0.

set -eu
cur=$(cd `dirname $0`; pwd)

DB_NAME="non_exist_tables"
TABLE_NAME1="t1"
TABLE_NAME2="t2"

# Test for simple case.
run_sql "drop database if exists \`$DB_NAME\`;"
run_sql "create database \`$DB_NAME\`;"
run_sql "create table \`$DB_NAME\`.\`$TABLE_NAME1\` (a int);"
run_sql "insert into \`$DB_NAME\`.\`$TABLE_NAME1\` values (1), (2);"

# Test for specifying --tables-list with non-existing tables, should report an error
set +e
rm -rf "$DUMPLING_OUTPUT_DIR"
run_dumpling --consistency=lock --tables-list "$DB_NAME.$TABLE_NAME2" -L ${DUMPLING_OUTPUT_DIR}/dumpling.log
set -e

actual=$(grep -w "Error 1146: Table 'non_exist_tables.t2' doesn't exist" ${DUMPLING_OUTPUT_DIR}/dumpling.log|wc -l)
echo "expected 1 return error when specifying --tables-list with non-existing tables, actual ${actual}"
[ $actual = 1 ]
