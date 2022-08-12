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

check_cluster_version 4 0 0 'local backend' || exit 0

# enable cluster index
run_sql 'set @@global.tidb_enable_clustered_index = 1' || exit 0
# wait for global variable cache invalid
sleep 2

set -euE

# Populate the mydumper source
DBPATH="$TEST_DIR/ch.mydump"
mkdir -p $DBPATH
echo 'CREATE DATABASE ch;' > "$DBPATH/ch-schema-create.sql"
# create table with non-integer primary key, so that cluster index will be used
echo "CREATE TABLE t(s varchar(32), i INT, j TINYINT,  PRIMARY KEY(s, i));" > "$DBPATH/ch.t-schema.sql"
cat > "$DBPATH/ch.t.0.sql" << _EOF_
INSERT INTO t (s, i, j) VALUES
  ("this_is_test1", 1, 1),
  ("this_is_test2", 2, 2),
  ("this_is_test3", 3, 3),
  ("this_is_test4", 4, 4),
  ("this_is_test5", 5, 5);
_EOF_
echo 'INSERT INTO t(s, i, j) VALUES ("another test case", 6, 6);' > "$DBPATH/ch.t.1.sql"

for BACKEND in local tidb; do
  # Start importing the tables.
  run_sql 'DROP DATABASE IF EXISTS ch'

  run_lightning -d "$DBPATH" --backend $BACKEND 2> /dev/null

  run_sql 'SELECT count(*), sum(i) FROM `ch`.t'
  check_contains "count(*): 6"
  check_contains "sum(i): 21"

  # check table kv pairs. common hanle should have no extra index kv-paris
  run_sql 'ADMIN CHECKSUM TABLE `ch`.t'
  check_contains "Total_kvs: 6"
done

# restore global variables, other tests needs this to handle the _tidb_row_id column
run_sql 'set @@global.tidb_enable_clustered_index = 0'
# wait for global variable cache invalid
sleep 2
