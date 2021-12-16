#!/bin/sh
#
# Copyright 2020 PingCAP, Inc. Licensed under Apache-2.0.

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
run_sql "create database \`$DB_NAME\`;"
run_sql "create table \`$DB_NAME\`.\`$TABLE_NAME\` (id int not null auto_increment primary key, a varchar(24));"

# insert 100 records
run_sql_file "$cur/data/rows.t.000000000.sql"

# make sure the estimated count is accurate
run_sql "analyze table \`$DB_NAME\`.\`$TABLE_NAME\`"

# dumping
export DUMPLING_TEST_DATABASE=$DB_NAME
run_dumpling --rows 10 --loglevel debug

# the dumping result is expected to be:
# 10 files for insertion
file_num=$(find "$DUMPLING_OUTPUT_DIR" -maxdepth 1 -iname "$DB_NAME.$TABLE_NAME.*.sql" | wc -l)
if [ "$file_num" -ne 10 ]; then
  echo "obtain file number: $file_num, but expect: 10" && exit 1
fi

cat "$cur/conf/lightning.toml"
# use lightning import data to tidb
run_lightning $cur/conf/lightning.toml

# check mysql and tidb data
check_sync_diff $cur/conf/diff_config.toml

# test dumpling with both rows and filesize
rm -rf "$DUMPLING_OUTPUT_DIR"
run_dumpling --rows 10 --filesize 100B --loglevel debug
# the dumping result is expected to be:
# 50 files for insertion
file_num=$(find "$DUMPLING_OUTPUT_DIR" -maxdepth 1 -iname "$DB_NAME.$TABLE_NAME.*.sql" | wc -l)
if [ "$file_num" -ne 50 ]; then
  echo "obtain file number: $file_num, but expect: 50" && exit 1
fi

for i in `seq 0 9`
do
  r=$(printf "%02d" $i)
  for j in `seq 0 4`
  do
    file_name="$DUMPLING_OUTPUT_DIR/$DB_NAME.$TABLE_NAME.0000000${r}000${j}.sql"
    if [ ! -f "$file_name" ]; then
      echo "file $file_name doesn't exist, which is not expected" && exit 1
    fi
  done
done

# drop database on tidb
export DUMPLING_TEST_PORT=4000
run_sql "drop database if exists \`$DB_NAME\`;"

cat "$cur/conf/lightning.toml"
# use lightning import data to tidb
run_lightning $cur/conf/lightning.toml

# check mysql and tidb data
check_sync_diff $cur/conf/diff_config.toml
