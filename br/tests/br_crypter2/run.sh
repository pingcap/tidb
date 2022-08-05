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

function create_db_with_table(){
    for i in $(seq $DB_COUNT); do
        run_sql "CREATE DATABASE $DB${i};"
        go-ycsb load mysql -P tests/$TEST_NAME/workload -p mysql.host=$TIDB_IP -p mysql.port=$TIDB_PORT -p mysql.user=root -p mysql.db=$DB${i}
    done
}

function drop_db(){
    for i in $(seq $DB_COUNT); do
        run_sql "DROP DATABASE $DB${i};"
    done
}

function check_db_row(){
    for i in $(seq $DB_COUNT); do
        row_count_new[${i}]=$(run_sql "SELECT COUNT(*) FROM $DB${i}.$TABLE;" | awk '/COUNT/{print $2}')
    done

    fail=false
    for i in $(seq $DB_COUNT); do
        if [ "${row_count_ori[i]}" != "${row_count_new[i]}" ];then
            fail=true
            echo "TEST: [$TEST_NAME] fail on database $DB${i}"
        fi
        echo "database $DB${i} [original] row count: ${row_count_ori[i]}, [after br] row count: ${row_count_new[i]}"
    done

    if $fail; then
        echo "TEST: [$TEST_NAME] failed!"
        exit 1
    fi
}

# Create dbs with table
create_db_with_table

# Get the original row count from dbs
for i in $(seq $DB_COUNT); do
    row_count_ori[${i}]=$(run_sql "SELECT COUNT(*) FROM $DB${i}.$TABLE;" | awk '/COUNT/{print $2}')
done

# Test backup/restore with crypt for br
CRYPTER_METHOD=aes128-ctr
CRYPTER_KEY="0123456789abcdef0123456789abcdef"

export GO_FAILPOINTS="github.com/pingcap/tidb/br/pkg/backup/noop-backup=100*return(1)"
run_br --pd $PD_ADDR backup full -s "local://$TEST_DIR/$DB/${CRYPTER_METHOD}_file" \
        --use-backupmeta-v2=true --check-requirements=false --crypter.method $CRYPTER_METHOD  --crypter.key $CRYPTER_KEY

drop_db

run_br --pd $PD_ADDR restore full -s "local://$TEST_DIR/$DB/${CRYPTER_METHOD}_file" \
        --check-requirements=false --crypter.method $CRYPTER_METHOD  --crypter.key $CRYPTER_KEY

check_db_row

# Drop dbs finally
drop_db
