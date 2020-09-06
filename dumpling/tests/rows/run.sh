#!/bin/sh

set -eu
cur=$(cd `dirname $0`; pwd)

DB_NAME="rows"
TABLE_NAME="t"

# drop database on tidb
export DUMPLING_TEST_PORT=4000
run_sql "drop database if exists \`$DB_NAME\`;"

# drop database on mysql
export DUMPLING_TEST_PORT=3306
run_sql "drop database if exists \`$DB_NAME\`;"

# build data on mysql
run_sql "create database $DB_NAME;"
run_sql "create table $DB_NAME.$TABLE_NAME (id int not null auto_increment primary key, a varchar(24));"

# insert 100 records
run_sql_file "$cur/data/rows.t.0.sql"

# dumping
export DUMPLING_TEST_DATABASE=$DB_NAME
run_dumpling --rows 10 --loglevel debug

# the dumping result is expected to be:
# 10 files for insertion
# FIXME the result of EXPLAIN SELECT `id` FROM `rows`.`t` randomly equal to 1 or 100, this could affect on file num.
# file_num=$(find "$DUMPLING_OUTPUT_DIR" -maxdepth 1 -iname "$DB_NAME.$TABLE_NAME.*.sql" | wc -l)
# if [ "$file_num" -ne 10 ]; then
#   echo "obtain file number: $file_num, but expect: 10" && exit 1
# fi

cat "$cur/conf/lightning.toml"
# use lightning import data to tidb
run_lightning $cur/conf/lightning.toml

# check mysql and tidb data
check_sync_diff $cur/conf/diff_config.toml


