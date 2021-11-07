#!/bin/sh
#
# Copyright 2020 PingCAP, Inc. Licensed under Apache-2.0.

set -eu
cur=$(cd `dirname $0`; pwd)

DB_NAME="null_unique_key"

# drop database on mysql
run_sql "drop database if exists \`$DB_NAME\`;"

# build data on mysql
run_sql "create database \`$DB_NAME\`;"
run_sql "create table \`$DB_NAME\`.\`t\` (a int unique key, b int);"
run_sql "insert into \`$DB_NAME\`.\`t\` values (1, 2), (NULL, 1);"


# dumping
export DUMPLING_TEST_DATABASE=$DB_NAME
run_dumpling -r 1

data="NULL"
cnt=$(sed "s/$data/$data\n/g" $DUMPLING_OUTPUT_DIR/$DB_NAME.t.000000000.sql | grep -c "$data") || true
[ $cnt = 1 ]

