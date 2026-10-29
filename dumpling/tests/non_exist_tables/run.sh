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
rm -rf "$DUMPLING_OUTPUT_DIR"
if run_dumpling --consistency=lock --tables-list "$DB_NAME.$TABLE_NAME2" -L "${DUMPLING_OUTPUT_DIR}/dumpling.log"; then
	echo "expected dumpling to fail for a non-existing table"
	exit 1
fi

actual=$(grep -F '[ERROR]' "${DUMPLING_OUTPUT_DIR}/dumpling.log" | grep -Fc "Table '$DB_NAME.$TABLE_NAME2' doesn't exist")
echo "expected 1 ERROR when specifying --tables-list with non-existing tables, actual ${actual}"
[ "$actual" = 1 ]
