#!/bin/sh
#
# Copyright 2020 PingCAP, Inc. Licensed under Apache-2.0.

set -eu
cur=$(cd `dirname $0`; pwd)

DB_NAME="chaos"
TABLE_NAME="t"

# drop database on mysql
run_sql "drop database if exists \`$DB_NAME\`;"

# build data on mysql
run_sql "create database $DB_NAME;"
run_sql "create table $DB_NAME.$TABLE_NAME (a int(255));"

# insert 100 records
run_sql "insert into $DB_NAME.$TABLE_NAME values $(seq -s, 100 | sed 's/,*$//g' | sed "s/[0-9]*/('1')/g");"

# dumping with consistency none
export DUMPLING_TEST_DATABASE=$DB_NAME
export GO_FAILPOINTS="github.com/pingcap/dumpling/v4/export/ChaosBrokenMySQLConn=1*return"
run_dumpling --consistency=none --loglevel debug

# check data record count
cnt=`grep -o "(1)" ${DUMPLING_OUTPUT_DIR}/${DB_NAME}.${TABLE_NAME}.000000000.sql|wc -l`
echo "1st records count is ${cnt}"
[ $cnt = 100 ]
export GO_FAILPOINTS=""
