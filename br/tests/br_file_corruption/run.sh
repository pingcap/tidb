#!/bin/sh
#
# Copyright 2024 PingCAP, Inc.
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

set -eux

DB="$TEST_NAME"
TABLE="usertable"
CUR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)

run_sql "CREATE DATABASE $DB;"
go-ycsb load mysql -P $CUR/workload -p mysql.host=$TIDB_IP -p mysql.port=$TIDB_PORT -p mysql.user=root -p mysql.db=$DB
run_br --pd $PD_ADDR backup full -s "local://$TEST_DIR/$DB" --checksum=false

# Replace the single file manipulation with a loop over all .sst files
for filename in $(find $TEST_DIR/$DB -name "*.sst"); do
    filename_temp="${filename}_temp"
    filename_bak="${filename}_bak"
    echo "corruption" > "$filename_temp"
    cat "$filename" >> "$filename_temp"
    mv "$filename" "$filename_bak"
done

# need to drop db otherwise restore will fail because of cluster not fresh but not the expected issue
run_sql "DROP DATABASE IF EXISTS $DB;"

# file lost
export GO_FAILPOINTS="github.com/pingcap/tidb/br/pkg/utils/set-remaining-attempts-to-one=return(true)"
restore_fail=0
run_br --pd $PD_ADDR restore full -s "local://$TEST_DIR/$DB" || restore_fail=1
export GO_FAILPOINTS=""
if [ $restore_fail -ne 1 ]; then
    echo 'expect restore to fail on file lost but succeed'
    exit 1
fi
run_sql "DROP DATABASE IF EXISTS $DB;"

# file corruption
for filename in $(find $TEST_DIR/$DB -name "*.sst_temp"); do
    mv "$filename" "${filename%_temp}"
    truncate -s -11 "${filename%_temp}"
done

export GO_FAILPOINTS="github.com/pingcap/tidb/br/pkg/utils/set-remaining-attempts-to-one=return(true)"
restore_fail=0
run_br --pd $PD_ADDR restore full -s "local://$TEST_DIR/$DB" || restore_fail=1
export GO_FAILPOINTS=""
if [ $restore_fail -ne 1 ]; then
    echo 'expect restore to fail on file corruption but succeed'
    exit 1
fi
run_sql "DROP DATABASE IF EXISTS $DB;"

# verify validating checksum is still performed even backup didn't enable it
for filename in $(find $TEST_DIR/$DB -name "*.sst_bak"); do
    mv "$filename" "${filename%_bak}"
done

export GO_FAILPOINTS="github.com/pingcap/tidb/br/pkg/restore/snap_client/full-restore-validate-checksum=return(true)"
restore_fail=0
run_br --pd $PD_ADDR restore full -s "local://$TEST_DIR/$DB" || restore_fail=1
export GO_FAILPOINTS=""
if [ $restore_fail -ne 1 ]; then
    echo 'expect restore to fail on checksum mismatch but succeed'
    exit 1
fi
run_sql "DROP DATABASE IF EXISTS $DB;"

# sanity check restore can succeed
run_br --pd $PD_ADDR restore full -s "local://$TEST_DIR/$DB"
echo 'file corruption tests passed'
