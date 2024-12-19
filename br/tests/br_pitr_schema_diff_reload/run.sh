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
CUR=$(cd `dirname $0`; pwd)
TASK_NAME="pitr_table_filter"
. run_services

# helper methods
create_tables_with_values() {
    local prefix=$1    # table name prefix
    local count=$2     # number of tables to create

    for i in $(seq 1 $count); do
        run_sql "create table $DB.${prefix}_${i}(c int); insert into $DB.${prefix}_${i} values ($i);"
    done
}

verify_tables() {
    local prefix=$1        # table name prefix
    local count=$2         # number of tables to verify
    local should_exist=$3  # true/false - whether tables should exist

    for i in $(seq 1 $count); do
        if [ "$should_exist" = "true" ]; then
            run_sql "select count(*) = 1 from $DB.${prefix}_${i} where c = $i" || {
                echo "Table $DB.${prefix}_${i} doesn't have expected value $i"
                exit 1
            }
        else
            if run_sql "select * from $DB.${prefix}_${i}" 2>/dev/null; then
                echo "Table $DB.${prefix}_${i} exists but should not"
                exit 1
            fi
        fi
    done
}

rename_tables() {
    local old_prefix=$1    # original table name prefix
    local new_prefix=$2    # new table name prefix
    local count=$3         # number of tables to rename

    for i in $(seq 1 $count); do
        run_sql "rename table $DB.${old_prefix}_${i} to $DB.${new_prefix}_${i};"
    done
}

drop_tables() {
    local prefix=$1    # table name prefix
    local count=$2     # number of tables to drop

    for i in $(seq 1 $count); do
        run_sql "drop table $DB.${prefix}_${i};"
    done
}

test_basic_filter() {
    restart_services || { echo "Failed to restart services"; exit 1; }

    echo "start basic filter testing"
    run_br --pd $PD_ADDR log start --task-name $TASK_NAME -s "local://$TEST_DIR/$TASK_NAME/log"

    run_sql "create schema $DB;"

    echo "write initial data and do snapshot backup"
    create_tables_with_values "full_backup" 3
    create_tables_with_values "table_to_drop" 3

    run_br backup full -f "$DB.*" -s "local://$TEST_DIR/$TASK_NAME/full" --pd $PD_ADDR

    echo "write more data and wait for log backup to catch up"
    create_tables_with_values "log_backup_lower" 3
    create_tables_with_values "LOG_BACKUP_UPPER" 3
    create_tables_with_values "other" 3
    drop_tables "table_to_drop" 3

    . "$CUR/../br_test_utils.sh" && wait_log_checkpoint_advance "$TASK_NAME"

    # restart services to clean up the cluster
    restart_services || { echo "Failed to restart services"; exit 1; }

    echo "case 1 sanity check, zero filter"
    run_br --pd "$PD_ADDR" restore point -s "local://$TEST_DIR/$TASK_NAME/log" --full-backup-storage "local://$TEST_DIR/$TASK_NAME/full" -f "$DB.*"

    verify_tables "log_backup_lower" 3 true
    verify_tables "LOG_BACKUP_UPPER" 3 true
    verify_tables "full_backup" 3 true
    verify_tables "other" 3 true
    verify_tables "table_to_drop" 3 false

#    echo "case 2 with log backup table filter"
#    run_sql "drop schema $DB;"
#    run_br --pd "$PD_ADDR" restore point -s "local://$TEST_DIR/$TASK_NAME/log" --full-backup-storage "local://$TEST_DIR/$TASK_NAME/full" -f "$DB.log*"
#
#    verify_tables "log_backup_lower" 3 true
#    verify_tables "LOG_BACKUP_UPPER" 3 true
#    verify_tables "full_backup" 3 false
#    verify_tables "other" 3 false
#    verify_tables "table_to_drop" 3 false
#
#    echo "case 3 with multiple filters"
#    run_sql "drop schema $DB;"
#    run_br --pd "$PD_ADDR" restore point -s "local://$TEST_DIR/$TASK_NAME/log" --full-backup-storage "local://$TEST_DIR/$TASK_NAME/full" -f "$DB.log*" -f "$DB.full*"
#
#    verify_tables "log_backup_lower" 3 true
#    verify_tables "LOG_BACKUP_UPPER" 3 true
#    verify_tables "full_backup" 3 true
#    verify_tables "other" 3 false
#    verify_tables "table_to_drop" 3 false
#
#    echo "case 4 with negative filters"
#    run_sql "drop schema $DB;"
#    # have to use a match all filter before using negative filters
#    run_br --pd "$PD_ADDR" restore point -s "local://$TEST_DIR/$TASK_NAME/log" --full-backup-storage "local://$TEST_DIR/$TASK_NAME/full" -f "*.*" -f "!$DB.log*"
#
#    verify_tables "log_backup_lower" 3 false
#    verify_tables "LOG_BACKUP_UPPER" 3 false
#    verify_tables "full_backup" 3 true
#    verify_tables "other" 3 true
#    verify_tables "table_to_drop" 3 false
#
#    echo "case 5 restore dropped table"
#    run_sql "drop schema $DB;"
#    run_br --pd "$PD_ADDR" restore point -s "local://$TEST_DIR/$TASK_NAME/log" --full-backup-storage "local://$TEST_DIR/$TASK_NAME/full" -f "$DB.table*"
#
#    verify_tables "log_backup_lower" 3 false
#    verify_tables "LOG_BACKUP_UPPER" 3 false
#    verify_tables "full_backup" 3 false
#    verify_tables "other" 3 false
#    verify_tables "table_to_drop" 3 false
#
#    # cleanup
#    rm -rf "$TEST_DIR/$TASK_NAME"
#
#    echo "basic filter test cases passed"
}

test_basic_filter

echo "br pitr schema diff reload passed"
