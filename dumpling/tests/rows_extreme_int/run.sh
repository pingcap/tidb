#!/bin/sh
#
# Copyright 2020 PingCAP, Inc. Licensed under Apache-2.0.

set -eu
cur=$(cd `dirname $0`; pwd)

DB_NAME="rei"
TABLE_NAME="t"
TABLE_NAME2="t2"

# drop database on tidb
export DUMPLING_TEST_PORT=4000
run_sql "drop database if exists \`$DB_NAME\`;"

# drop database on mysql
export DUMPLING_TEST_PORT=3306
run_sql "drop database if exists \`$DB_NAME\`;"

# build data on mysql
run_sql "create database $DB_NAME DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin;"
run_sql "create table $DB_NAME.$TABLE_NAME (id int not null auto_increment primary key, a varchar(24));"
run_sql "create table $DB_NAME.$TABLE_NAME2 (id bigint unsigned not null auto_increment primary key, a varchar(24));"

# insert 100 records
run_sql_file "$cur/data/rei.t.0.sql"

# insert 100 records
run_sql_file "$cur/data/rei.t2.0.sql"

# analyze table for making sure the estimateCount is correct
run_sql "analyze table $DB_NAME.$TABLE_NAME;"
run_sql "analyze table $DB_NAME.$TABLE_NAME2;"

# dumping
# test print status
export GO_FAILPOINTS="github.com/pingcap/tidb/dumpling/export/EnableLogProgress=return()"
export DUMPLING_TEST_DATABASE=$DB_NAME
run_dumpling --rows 10 --loglevel debug -L ${DUMPLING_OUTPUT_DIR}/dumpling.log

# make sure that dumpling log contains chunks progress infomation
cnt=$(grep -w "chunks progress.*%" ${DUMPLING_OUTPUT_DIR}/dumpling.log|wc -l|awk '{$1=$1;print}')
echo "chunk progress count is ${cnt}"
[ "$cnt" -ge 1 ]

cat "$cur/conf/lightning.toml"
# use lightning import data to tidb
run_lightning $cur/conf/lightning.toml

# check mysql and tidb data
check_sync_diff $cur/conf/diff_config.toml


