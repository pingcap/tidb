#!/bin/sh
#
# Copyright 2020 PingCAP, Inc. Licensed under Apache-2.0.

set -eu
cur=$(cd `dirname $0`; pwd)

DB_NAME="non_exist_tables"
TABLE_NAME="tb1"
DB_NAME1="testdb"
TABLE_NAME1="tb2"

# Test for simple case.
run_sql "drop database if exists \`$DB_NAME\`;"
run_sql "create database \`$DB_NAME\`;"
run_sql "create table \`$DB_NAME\`.\`$TABLE_NAME\` (a int);"
run_sql "insert into \`$DB_NAME\`.\`$TABLE_NAME\` values (1), (2);"

# Test for specifying --tables-list with non-existing tables, should report an error
set +e
rm -rf "$DUMPLING_TEST_DIR"
run_dumpling --consistency=lock --tables-list "$DB_NAME.$TABLE_NAME1" -L ${DUMPLING_TEST_DIR}/dumpling.log
set -e

actual=$(grep -w "Error 1146: Table 'non_exist_tables.tb2' doesn't exist" ${DUMPLING_TEST_DIR}/dumpling.log|wc -l)
echo "expected 1 return error when specifying --tables-list with non-existing tables, actual ${actual}"
[ $actual = 1 ]
