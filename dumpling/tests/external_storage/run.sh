#!/bin/sh
#
# Copyright 2022 PingCAP, Inc. Licensed under Apache-2.0.

set -eu
cur=$(cd `dirname $0`; pwd)

DB_NAME="ext_storage"
TABLE_NAME="t"

# Test for simple case.
run_sql "drop database if exists \`$DB_NAME\`;"
run_sql "create database \`$DB_NAME\` DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin;"
run_sql "create table \`$DB_NAME\`.\`$TABLE_NAME\` (a int);"
run_sql "insert into \`$DB_NAME\`.\`$TABLE_NAME\` values (1), (2);"

export GO_FAILPOINTS="github.com/pingcap/tidb/dumpling/export/setExtStorage=return(\"$DUMPLING_TEST_DIR/set_by_failpoint\")"
run_dumpling -f "$DB_NAME.$TABLE_NAME" -L ${DUMPLING_OUTPUT_DIR}/dumpling.log

files=`ls $DUMPLING_TEST_DIR/set_by_failpoint | wc -l`
[ "$files" = 4 ]
