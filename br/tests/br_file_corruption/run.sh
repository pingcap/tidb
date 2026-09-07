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
run_br --pd $PD_ADDR backup full -s "local://$TEST_DIR/$DB"

filename=$(find $TEST_DIR/$DB -regex ".*.sst" | head -n 1)
filename_temp=$filename"_temp"
filename_bak=$filename"_bak"
echo "corruption" > $filename_temp
cat $filename >> $filename_temp

# file lost
mv $filename $filename_bak
export GO_FAILPOINTS="github.com/pingcap/tidb/br/pkg/utils/set-import-attempt-to-one=return(true)"
restore_fail=0
run_br --pd $PD_ADDR restore full -s "local://$TEST_DIR/$DB" || restore_fail=1
export GO_FAILPOINTS=""
if [ $restore_fail -ne 1 ]; then
    echo 'restore success' 
    exit 1
fi

# file corruption
mv $filename_temp $filename
truncate --size=-11 $filename
export GO_FAILPOINTS="github.com/pingcap/tidb/br/pkg/utils/set-import-attempt-to-one=return(true)"
restore_fail=0
run_br --pd $PD_ADDR restore full -s "local://$TEST_DIR/$DB" || restore_fail=1
export GO_FAILPOINTS=""
if [ $restore_fail -ne 1 ]; then
    echo 'restore success' 
    exit 1
fi
