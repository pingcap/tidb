#!/bin/sh
#
# Copyright 2020 PingCAP, Inc. Licensed under Apache-2.0.

set -eu
cur=$(cd `dirname $0`; pwd)

DB_NAME="ignore_generate"
TABLE_NAME="t"

# drop database on tidb
export DUMPLING_TEST_PORT=4000
run_sql "drop database if exists $DB_NAME;"

# drop database on mysql
export DUMPLING_TEST_PORT=3306
run_sql "drop database if exists $DB_NAME;"

# build data on mysql
run_sql "create database $DB_NAME;"

# build data with generate column full_name
run_sql "create table $DB_NAME.$TABLE_NAME(first_name varchar(14) NOT NULL, last_name varchar(16) NOT NULL, full_name VARCHAR(30) AS (CONCAT(first_name,'-',last_name))) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;"

# insert 100 records
run_sql "insert into $DB_NAME.$TABLE_NAME (first_name, last_name) values $(seq -s, 100 | sed 's/,*$//g' | sed "s/[0-9]*/('a', 'b')/g");"

# dumping
export DUMPLING_TEST_DATABASE=$DB_NAME
run_dumpling

cat "$cur/conf/lightning.toml"
# use lightning import data to tidb
run_lightning $cur/conf/lightning.toml

# check mysql and tidb data
check_sync_diff $cur/conf/diff_config.toml


