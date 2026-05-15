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

set -euE

# Populate the mydumper source
DBPATH="$TEST_DIR/cp.mydump"

mkdir -p $DBPATH
echo 'CREATE DATABASE cp_tsr;' > "$DBPATH/cp_tsr-schema-create.sql"
echo "CREATE TABLE tbl(i TINYINT PRIMARY KEY, j INT);" > "$DBPATH/cp_tsr.tbl-schema.sql"
# the column orders in data file is different from table schema order.
echo "INSERT INTO tbl (j, i) VALUES (3, 1),(4, 2);" > "$DBPATH/cp_tsr.tbl.sql"

# Set minDeliverBytes to a small enough number to only write only 1 row each time
# Set the failpoint to kill the lightning instance as soon as one row is written
PKG="github.com/pingcap/tidb/lightning/pkg/importer"
export GO_FAILPOINTS="$PKG/SlowDownWriteRows=sleep(1000);$PKG/FailAfterWriteRows=panic;$PKG/SetMinDeliverBytes=return(1)"
# Check after 1 row is written in tidb backend, the finished progress is updated
export GO_FAILPOINTS="${GO_FAILPOINTS};github.com/pingcap/tidb/lightning/pkg/server/PrintStatus=return()"

# Start importing the tables.
run_sql 'DROP DATABASE IF EXISTS cp_tsr'
run_sql 'DROP DATABASE IF EXISTS tidb_lightning_checkpoint_test'

set +e
run_lightning -d "$DBPATH" --backend tidb --enable-checkpoint=1 2> /dev/null
set -e
run_sql 'SELECT count(*) FROM `cp_tsr`.tbl'
check_contains "count(*): 1"

# After FailAfterWriteRows, the finished bytes is 36 as the first row size
grep "PrintStatus Failpoint" "$TEST_DIR/lightning.log" | grep -q "finished=36"

# restart lightning from checkpoint, the second line should be written successfully
# also check after restart from checkpoint, final finished equals to total
export GO_FAILPOINTS="github.com/pingcap/tidb/lightning/pkg/server/PrintStatus=return()"
set +e
run_lightning -d "$DBPATH" --backend tidb --enable-checkpoint=1 2> /dev/null
set -e

run_sql 'SELECT j FROM `cp_tsr`.tbl WHERE i = 2;'
check_contains "j: 4"
grep "PrintStatus Failpoint" "$TEST_DIR/lightning.log" | grep -q "equal=true"
