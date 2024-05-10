#!/bin/bash
#
# Copyright 2020 PingCAP, Inc. Licensed under Apache-2.0.

set -eu
cur=$(cd `dirname $0`; pwd)

DB_NAME="e2e_foreign_key"

# drop database on tidb
export DUMPLING_TEST_PORT=4000
run_sql "drop database if exists $DB_NAME;"

# drop database on mysql
export DUMPLING_TEST_PORT=3306
run_sql "drop database if exists $DB_NAME;"

# build data on mysql
run_sql_file "$DUMPLING_BASE_NAME/data/e2e_foreign_key.sql"

# dumping
export DUMPLING_TEST_DATABASE=$DB_NAME
run_dumpling

cat "$cur/conf/lightning.toml"
# use lightning import data to tidb
run_lightning $cur/conf/lightning.toml

# check mysql and tidb data
check_sync_diff $cur/conf/diff_config.toml
