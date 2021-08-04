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
# See the License for the specific language governing permissions and
# limitations under the License.

set -eu

# Populate the mydumper source
DBPATH="$TEST_DIR/restore.mydump"
TABLE_COUNT=35

mkdir -p $DBPATH
echo 'CREATE DATABASE restore_tsr;' > "$DBPATH/restore_tsr-schema-create.sql"
for i in $(seq "$TABLE_COUNT"); do
    echo "CREATE TABLE tbl$i(i TINYINT);" > "$DBPATH/restore_tsr.tbl$i-schema.sql"
    echo "INSERT INTO tbl$i VALUES (1);" > "$DBPATH/restore_tsr.tbl$i.sql"
done

# Count OpenEngine and CloseEngine events.
# Abort if number of unbalanced OpenEngine is >= 4
export GO_FAILPOINTS='github.com/pingcap/tidb/br/pkg/lightning/backend/FailIfEngineCountExceeds=return(4)'

# Start importing
run_sql 'DROP DATABASE IF EXISTS restore_tsr'
run_lightning -d "$DBPATH"
echo "Import finished"

# Verify all data are imported
for i in $(seq "$TABLE_COUNT"); do
    run_sql "SELECT sum(i) FROM restore_tsr.tbl$i;"
    check_contains 'sum(i): 1'
done
