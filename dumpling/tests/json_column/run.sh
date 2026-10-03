#!/bin/sh
#
# Copyright 2020 PingCAP, Inc. Licensed under Apache-2.0.

set -eu
cur=$(cd `dirname $0`; pwd)

DB_NAME="json_database"
TABLE_NAME="t"

# drop database on tidb
export DUMPLING_TEST_PORT=4000
run_sql "drop database if exists \`$DB_NAME\`;"

# drop database on mysql
export DUMPLING_TEST_PORT=3306
run_sql "drop database if exists \`$DB_NAME\`;"

# build data on mysql
run_sql "create database \`$DB_NAME\` DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin;"
run_sql "create table \`$DB_NAME\`.\`$TABLE_NAME\`(id int primary key auto_increment,j json,j1 json as (json_extract(j, '$.a')),j2 json as (json_extract(j1, '$.b')),a int as (j ->> '$.a'),b char(255) as (j2 ->> '$.b'),c char(255) as (j2 ->> '\$[0].a'));"

# insert 100 records
run_sql_file "$cur/data.sql"

# make sure the estimated count is accurate
run_sql "analyze table \`$DB_NAME\`.\`$TABLE_NAME\`"

# dumping
export DUMPLING_TEST_DATABASE=$DB_NAME
run_dumpling --rows 10 --loglevel debug

cat "$cur/conf/lightning.toml"
# use lightning import data to tidb
run_lightning $cur/conf/lightning.toml

# check mysql and tidb data
check_sync_diff $cur/conf/diff_config.toml

# drop database on tidb
export DUMPLING_TEST_PORT=4000
run_sql "drop database if exists \`$DB_NAME\`;"

cat "$cur/conf/lightning.toml"
# use lightning import data to tidb
run_lightning $cur/conf/lightning.toml

# check mysql and tidb data
check_sync_diff $cur/conf/diff_config.toml
