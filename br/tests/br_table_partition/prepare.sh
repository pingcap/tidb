#!/bin/sh
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

set -eu

ROW_COUNT=100
CONCURRENCY=8

TABLE_COLUMNS='c1 INT, c2 CHAR(255), c3 CHAR(255), c4 CHAR(255), c5 CHAR(255)'

insertRecords() {
    for i in $(seq $2 $3); do
        run_sql "INSERT INTO $1 VALUES (\
            $i, \
            REPEAT(' ', 255), \
            REPEAT(' ', 255), \
            REPEAT(' ', 255), \
            REPEAT(' ', 255)\
        );"
    done
}

createTable() {
    run_sql "CREATE TABLE IF NOT EXISTS $DB.$TABLE$1 ($TABLE_COLUMNS) \
        PARTITION BY RANGE(c1) ( \
        PARTITION p0 VALUES LESS THAN (0), \
        PARTITION p1 VALUES LESS THAN ($(expr $ROW_COUNT / 2)) \
    );"
    run_sql "ALTER TABLE $DB.$TABLE$1 \
      ADD PARTITION (PARTITION p2 VALUES LESS THAN MAXVALUE);"
}

echo "load database $DB"
run_sql "CREATE DATABASE IF NOT EXISTS $DB;"
for i in $(seq $TABLE_COUNT); do
  createTable "${i}" &
done

run_sql "CREATE TABLE IF NOT EXISTS $DB.${TABLE}_Hash ($TABLE_COLUMNS) PARTITION BY HASH(c1) PARTITIONS 5;" &
# `tidb_enable_list_partition` currently only support session level variable, so we must put it in the create table sql
run_sql "set @@session.tidb_enable_list_partition = 'ON'; CREATE TABLE IF NOT EXISTS $DB.${TABLE}_List ($TABLE_COLUMNS) PARTITION BY LIST(c1) (\
    PARTITION p0 VALUES IN (2, 3, 5, 7, 11, 13, 17, 19, 23, 29, 31, 37, 41, 43, 47, 53, 59, 61, 67, 71, 73, 79, 83, 89, 97),
    PARTITION p1 VALUES IN (1, 4, 9, 16, 25, 36, 49, 64, 81, 100),
    PARTITION p2 VALUES IN (8, 18, 20, 24, 26, 30, 32, 44, 46, 50, 51, 55, 56, 58, 60, 75, 78, 80, 84, 85, 88, 90),
    PARTITION p3 VALUES IN (6, 12, 15, 22, 28, 33, 34, 38, 42, 54, 62, 63, 68, 69, 70, 74, 82, 91, 93, 94, 96, 98),
    PARTITION p4 VALUES IN (10, 14, 21, 27, 35, 39, 40, 45, 48, 52, 57, 65, 66, 72, 76, 77, 86, 87, 92, 95, 99)
)" &

wait

for i in $(seq $TABLE_COUNT); do
    for j in $(seq $CONCURRENCY); do
        insertRecords $DB.$TABLE${i} $(expr $ROW_COUNT / $CONCURRENCY \* $(expr $j - 1) + 1) $(expr $ROW_COUNT / $CONCURRENCY \* $j) &
    done
    insertRecords $DB.${TABLE}_Hash 1 $ROW_COUNT &
    insertRecords $DB.${TABLE}_List 1 $ROW_COUNT &
done
wait
