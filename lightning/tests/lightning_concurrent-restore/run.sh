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

# Populate the mydumper source
DBPATH="$TEST_DIR/restore_conc.mydump"
TABLE_COUNT=8

mkdir -p $DBPATH
echo 'CREATE DATABASE restore_conc;' > "$DBPATH/restore_conc-schema-create.sql"
for i in $(seq "$TABLE_COUNT"); do
    echo "CREATE TABLE tbl$i(i TINYINT);" > "$DBPATH/restore_conc.tbl$i-schema.sql"
    echo "INSERT INTO tbl$i VALUES (1);" > "$DBPATH/restore_conc.tbl$i.sql"
done

run_sql 'select VARIABLE_VALUE from mysql.tidb where VARIABLE_NAME = "tikv_gc_life_time"';
ORIGINAL_TIKV_GC_LIFE_TIME=$(tail -n 1 "$TEST_DIR/sql_res.$TEST_NAME.txt" | awk '{print $(NF)}')

# add a delay after increasing tikv_gc_life_time, in order to increase confilct possibility
export GO_FAILPOINTS='github.com/pingcap/tidb/pkg/lightning/backend/local/IncreaseGCUpdateDuration=sleep(200)'

# Start importing
run_sql 'DROP DATABASE IF EXISTS restore_conc'
run_lightning -d "$DBPATH"
echo "Import finished"

# Verify all data are imported
for i in $(seq "$TABLE_COUNT"); do
    run_sql "SELECT sum(i) FROM restore_conc.tbl$i;"
    check_contains 'sum(i): 1'
done

# check tikv_gc_life_time is recovered to the original value
run_sql 'select VARIABLE_VALUE from mysql.tidb where VARIABLE_NAME = "tikv_gc_life_time"';
check_contains "VARIABLE_VALUE: $ORIGINAL_TIKV_GC_LIFE_TIME"
