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
  run_sql "create table \`$DB_NAME\`.\`$TABLE_PREFIX$i\` (id int not null auto_increment primary key, a longtext);"
  for j in {1..50}; do
    run_sql "insert into \`$DB_NAME\`.\`$TABLE_PREFIX$i\`(a) values (repeat('a', 1000000));"
  done
done

# make sure the estimated count is accurate
for i in {1..3}; do
  run_sql "analyze table \`$DB_NAME\`.\`$TABLE_PREFIX$i\`"
done

# run dumpling
export GO_FAILPOINTS="github.com/pingcap/tidb/dumpling/export/SetWaitTimeout=return(2);github.com/pingcap/tidb/dumpling/export/SmallDumpChanSize=return();github.com/pingcap/tidb/dumpling/export/AtEveryRow=sleep(100)"
export DUMPLING_TEST_DATABASE=$DB_NAME
run_dumpling --loglevel debug --threads 1
export GO_FAILPOINTS=""

read -p 123




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
