#!/bin/sh

set -eu
cur=$(cd `dirname $0`; pwd)

DB_NAME="empty_test"

# drop database on mysql
run_sql "drop database if exists \`$DB_NAME\`;"

# build data on mysql
run_sql "create database \`$DB_NAME\`;"

# dumping
export DUMPLING_TEST_DATABASE=$DB_NAME
run_dumpling

sql="CREATE DATABASE \`$DB_NAME\`"
cnt=$(sed "s/$sql/$sql\n/g" $DUMPLING_OUTPUT_DIR/$DB_NAME-schema-create.sql | grep -c "$sql") || true
[ $cnt = 1 ]

