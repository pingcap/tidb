#!/bin/sh
#
# Copyright 2020 PingCAP, Inc. Licensed under Apache-2.0.

set -eu
cur=$(cd `dirname $0`; pwd)

DB_NAME="conn_timeout"
TABLE_PREFIX="t"

# drop database on tidb
export DUMPLING_TEST_PORT=4000
run_sql "drop database if exists \`$DB_NAME\`;"

# drop database on mysql
export DUMPLING_TEST_PORT=3306
run_sql "drop database if exists \`$DB_NAME\`;"

# build data on mysql
run_sql "create database \`$DB_NAME\` DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin;"
for i in {1..3}; do
  run_sql "create table \`$DB_NAME\`.\`$TABLE_PREFIX$i\` (id int not null auto_increment primary key);"
  for j in {1..30}; do
    run_sql "insert into \`$DB_NAME\`.\`$TABLE_PREFIX$i\` values ();"
  done
done

# make sure the estimated count is accurate
for i in {1..3}; do
  run_sql "analyze table \`$DB_NAME\`.\`$TABLE_PREFIX$i\`"
done

# run dumpling, should not meet connection error
export GO_FAILPOINTS="github.com/pingcap/tidb/dumpling/export/SetWaitTimeout=return(2);github.com/pingcap/tidb/dumpling/export/SmallDumpChanSize=return();github.com/pingcap/tidb/dumpling/export/AtEveryRow=sleep(100)"
export DUMPLING_TEST_DATABASE=$DB_NAME
run_dumpling --loglevel debug --threads 1 --rows 1
export GO_FAILPOINTS=""
