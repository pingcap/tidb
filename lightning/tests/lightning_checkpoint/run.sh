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

set -euE

# Populate the mydumper source
CUR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
DBPATH="$TEST_DIR/cppk.mydump"
TABLE_COUNT=9
CHUNK_COUNT=50

mkdir -p $DBPATH
echo 'CREATE DATABASE cppk_tsr;' > "$DBPATH/cppk_tsr-schema-create.sql"
INNER_QUERY='0'
OUTER_QUERY='0'
# total 64*50 = 3200 bytes
NOISE_COL_VAL="PJNKNoQE3TX3NuMQRCP0fbtYEnI9cVcVxcnr3MRpqjoaZf1DyT"
for i in {1..6}; do
    export NOISE_COL_VAL="$NOISE_COL_VAL$NOISE_COL_VAL"
done
for i in $(seq "$TABLE_COUNT"); do
    TABLE_ATTRIBUTES=""
    case $i in
        1)
            INDICES="PRIMARY KEY auto_increment"
            ;;
        2)
            INDICES="UNIQUE"
            ;;
        3)
            INDICES=", INDEX(j)"
            ;;
        4)
            INDICES=", PRIMARY KEY(i, j)"
            ;;
        5)
            INDICES=", UNIQUE KEY(j)"
            ;;
        6)
            INDICES=", PRIMARY KEY(j)"
            ;;
        7)
            INDICES="PRIMARY KEY auto_random"
            ;;
        8)
            INDICES="PRIMARY KEY auto_increment"
            TABLE_ATTRIBUTES="AUTO_ID_CACHE=1"
            ;;
        9)
            INDICES="PRIMARY KEY nonclustered auto_increment"
            TABLE_ATTRIBUTES="AUTO_ID_CACHE=1"
            ;;
        *)
            INDICES=""
            ;;
    esac
    echo "CREATE TABLE tbl$i(n text, i TINYINT, j bigint $INDICES) $TABLE_ATTRIBUTES;" > "$DBPATH/cppk_tsr.tbl$i-schema.sql"
    INNER_QUERY="$INNER_QUERY, (SELECT sum(j) FROM cppk_tsr.tbl$i) as s$i"
    OUTER_QUERY="$OUTER_QUERY + coalesce(s$i, 0)"
    for j in $(seq "$CHUNK_COUNT"); do
        echo "INSERT INTO tbl$i VALUES ('$NOISE_COL_VAL',$i,${j}000),('$NOISE_COL_VAL',$i,${j}001);" > "$DBPATH/cppk_tsr.tbl$i.$j.sql"
    done
done
PARTIAL_IMPORT_QUERY="SELECT *, $OUTER_QUERY AS s FROM (SELECT $INNER_QUERY) _"

check_cluster_version 4 0 0 'local backend'

# Set the failpoint to kill the lightning instance as soon as one table is imported
# If checkpoint does work, this should only kill 9 instances of lightnings.
SLOWDOWN_FAILPOINTS='github.com/pingcap/tidb/lightning/pkg/importer/SlowDownImport=sleep(250)'

#
# run with file checkpoint
#
run_sql 'DROP DATABASE IF EXISTS cppk_tsr'
export GO_FAILPOINTS="github.com/pingcap/tidb/lightning/pkg/importer/FailBeforeIndexEngineImported=return"
set +e
for i in $(seq 5); do
    echo "******** with file checkpoint (step $i/5) ********"
    run_lightning -d "$DBPATH" --enable-checkpoint=1 --config $CUR/config-file.toml 2> /dev/null
    [ $? -ne 0 ] || exit 1
done
set -e
export GO_FAILPOINTS=""
run_lightning -d "$DBPATH" --enable-checkpoint=1 --config $CUR/config-file.toml
run_sql "show table cppk_tsr.tbl1 next_row_id"
check_contains "NEXT_GLOBAL_ROW_ID: 50002"
run_sql "show table cppk_tsr.tbl2 next_row_id"
check_contains "NEXT_GLOBAL_ROW_ID: 63205"
run_sql "show table cppk_tsr.tbl7 next_row_id"
check_contains "NEXT_GLOBAL_ROW_ID: 50002"
run_sql "show table cppk_tsr.tbl8 next_row_id"
check_contains "NEXT_GLOBAL_ROW_ID: 50002"
run_sql "show table cppk_tsr.tbl9 next_row_id"
check_contains "NEXT_GLOBAL_ROW_ID: 50002"
check_contains "NEXT_GLOBAL_ROW_ID: 63205"

#
# run with mysql checkpoint
#
# Start importing the tables.
run_sql 'DROP DATABASE IF EXISTS cppk_tsr'
run_sql 'DROP DATABASE IF EXISTS tidb_lightning_checkpoint_test_cppk'
run_sql 'DROP DATABASE IF EXISTS `tidb_lightning_checkpoint_test_cppk.1357924680.bak`'
export GO_FAILPOINTS="github.com/pingcap/tidb/lightning/pkg/importer/FailBeforeIndexEngineImported=return"

# panic after saving index engine checkpoint status before saving table checkpoint status
set +e
for i in $(seq "$TABLE_COUNT"); do
    echo "******** with mysql checkpoint (step $i/$TABLE_COUNT) ********"
    run_lightning -d "$DBPATH" --enable-checkpoint=1 2> /dev/null
    [ $? -ne 0 ] || exit 1
done
set -e

run_sql "select concat(table_name, '|', auto_rand_base, '|', auto_incr_base, '|', auto_row_id_base) s from tidb_lightning_checkpoint_test_cppk.table_v10"
# auto-incr & auto-row-id share the same allocator, below too
check_contains 's: `cppk_tsr`.`tbl1`|0|50001|50001'
# use the estimated value of auto-row-id
check_contains 's: `cppk_tsr`.`tbl2`|0|63204|63204'
# no auto id
check_contains 's: `cppk_tsr`.`tbl4`|0|0|0'
check_contains 's: `cppk_tsr`.`tbl7`|50001|0|0'
check_contains 's: `cppk_tsr`.`tbl8`|0|50001|0'
check_contains 's: `cppk_tsr`.`tbl9`|0|50001|63204'

# at the failure of last table, all data engines are imported so finished == total
grep "print lightning status" "$TEST_DIR/lightning.log" | grep -q "equal=true"

export GO_FAILPOINTS=""
# After everything is done, there should be no longer new calls to ImportEngine
# (and thus `kill_lightning_after_one_import` will spare this final check)
echo "******** Verify checkpoint no-op ********"
run_lightning -d "$DBPATH" --enable-checkpoint=1
run_sql "$PARTIAL_IMPORT_QUERY"
check_contains "s: $(( (1000 * $CHUNK_COUNT + 1001) * $CHUNK_COUNT * $TABLE_COUNT ))"
run_sql 'SELECT count(*) FROM `tidb_lightning_checkpoint_test_cppk`.table_v10 WHERE status >= 200'
check_contains "count(*): $TABLE_COUNT"

run_sql "show table cppk_tsr.tbl1 next_row_id"
check_contains "NEXT_GLOBAL_ROW_ID: 50002"
run_sql "show table cppk_tsr.tbl2 next_row_id"
check_contains "NEXT_GLOBAL_ROW_ID: 63205"
run_sql "show table cppk_tsr.tbl7 next_row_id"
check_contains "NEXT_GLOBAL_ROW_ID: 50002"
run_sql "show table cppk_tsr.tbl8 next_row_id"
check_contains "NEXT_GLOBAL_ROW_ID: 50002"
run_sql "show table cppk_tsr.tbl9 next_row_id"
check_contains "NEXT_GLOBAL_ROW_ID: 50002"
check_contains "NEXT_GLOBAL_ROW_ID: 63205"

# Start importing the tables.
run_sql 'DROP DATABASE IF EXISTS cppk_tsr'
run_sql 'DROP DATABASE IF EXISTS tidb_lightning_checkpoint_test_cppk'
run_sql 'DROP DATABASE IF EXISTS `tidb_lightning_checkpoint_test_cppk.1357924680.bak`'

export GO_FAILPOINTS="$SLOWDOWN_FAILPOINTS;github.com/pingcap/tidb/lightning/pkg/server/SetTaskID=return(1357924680);github.com/pingcap/tidb/lightning/pkg/importer/FailIfIndexEngineImported=return(1)"

set +e
for i in $(seq "$TABLE_COUNT"); do
    echo "******** Importing Table Now (step $i/$TABLE_COUNT) ********"
    run_lightning -d "$DBPATH" --enable-checkpoint=1 2> /dev/null
    [ $? -ne 0 ] || exit 1
done
set -e

# After everything is done, there should be no longer new calls to ImportEngine
# (and thus `kill_lightning_after_one_import` will spare this final check)
echo "******** Verify checkpoint no-op ********"
run_lightning -d "$DBPATH" --enable-checkpoint=1
run_sql "$PARTIAL_IMPORT_QUERY"
check_contains "s: $(( (1000 * $CHUNK_COUNT + 1001) * $CHUNK_COUNT * $TABLE_COUNT ))"
run_sql 'SELECT count(*) FROM `tidb_lightning_checkpoint_test_cppk`.table_v10 WHERE status >= 200'
check_contains "count(*): $TABLE_COUNT"
