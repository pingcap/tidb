#!/bin/bash
#
# Copyright 2019 PingCAP, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# test source dir that contains soft/hard link files

set -euE

# Populate the mydumper source
DBPATH="$TEST_DIR/sf.mydump"
RAW_PATH="${DBPATH}_tmp"

mkdir -p $DBPATH $TEST_DIR/sf.mydump_tmp
DB=linkfiles

ROW_COUNT=1000

echo "CREATE DATABASE $DB;" > "$RAW_PATH/$DB-schema-create.sql"
echo "CREATE TABLE t(s varchar(64), i INT, j TINYINT,  PRIMARY KEY(s, i));" > "$RAW_PATH/$DB.t-schema.sql"
echo "CREATE TABLE t2(i INT PRIMARY KEY, s varchar(32));" > "$RAW_PATH/$DB.t2-schema.sql"

echo "s,i,j" > "$RAW_PATH/$DB.t.0.csv"
for i in $(seq "$ROW_COUNT"); do
    echo "\"thisisastringvalues_line$i\",$i,$i" >> "$RAW_PATH/$DB.t.0.csv"
done

echo "i,s" > "$RAW_PATH/$DB.t2.0.csv"
for i in $(seq $ROW_COUNT); do
  echo "$i,\"test123ataettaet$i\"" >> "$RAW_PATH/$DB.t2.0.csv"
done

# link source files to source dir
ls $RAW_PATH | xargs -I{} ln -s $RAW_PATH/{} $DBPATH/{}

for BACKEND in tidb local; do
  if [ "$BACKEND" = 'local' ]; then
    check_cluster_version 4 0 0 'local backend' || continue
  fi

  # Start importing the tables.
  run_sql "DROP DATABASE IF EXISTS $DB"

  run_lightning -d "$DBPATH" --backend $BACKEND 2> /dev/null
  run_sql "SELECT count(*) FROM $DB.t"
  check_contains "count(*): $ROW_COUNT"

  run_sql "SELECT count(*) FROM $DB.t2"
  check_contains "count(*): $ROW_COUNT"
done
