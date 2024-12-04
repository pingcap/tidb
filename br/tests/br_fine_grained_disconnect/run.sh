#!/bin/sh
#
# Copyright 2022 PingCAP, Inc.
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
DB="$TEST_NAME"
TABLE="usertable"
DB_COUNT=3
CUR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)

function create_db_with_table(){
    for i in $(seq $DB_COUNT); do
        run_sql "CREATE DATABASE $DB${i};"
        go-ycsb load mysql -P $CUR/workload -p mysql.host=$TIDB_IP -p mysql.port=$TIDB_PORT -p mysql.user=root -p mysql.db=$DB${i}
    done
}

function drop_db(){
    for i in $(seq $DB_COUNT); do
        run_sql "DROP DATABASE $DB${i};"
    done
}

# Create dbs with table
create_db_with_table

export GO_FAILPOINTS="github.com/pingcap/tidb/br/pkg/backup/noop-backup=100*return(1)"
export GO_FAILPOINTS="github.com/pingcap/tidb/br/pkg/backup/disconnect=100"
run_br --pd $PD_ADDR backup full -s "local://$TEST_DIR/$DB/${CRYPTER_METHOD}_file" 



# Drop dbs finally
drop_db
