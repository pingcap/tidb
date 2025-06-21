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

# disable global ENCRYPTION_ARGS and ENABLE_ENCRYPTION_CHECK for this script
ENCRYPTION_ARGS=""
ENABLE_ENCRYPTION_CHECK=false
export ENCRYPTION_ARGS
export ENABLE_ENCRYPTION_CHECK

set -eux
DB="$TEST_NAME"
CUR=$(cd `dirname $0`; pwd)
TASK_NAME="pitr_table_filter"
. run_services

# Helper function to verify no unexpected tables exist - add at the top level for all tests to use
verify_no_unexpected_tables() {
    local expected_count=$1
    
    # Get count of all tables and views across all non-system schemas
    # Exclude mysql, information_schema, performance_schema, sys, metrics_schema
    actual_count=$(run_sql "SELECT COUNT(*) FROM information_schema.tables
                           WHERE table_schema NOT IN ('mysql', 'information_schema', 'performance_schema', 'sys', 'metrics_schema', 'test')
                           AND table_type IN ('BASE TABLE', 'VIEW')" | awk 'NR==2 {print $2}')
    
    if [ "$actual_count" -ne "$expected_count" ]; then
        echo "Found wrong number of tables in the cluster. Expected: $expected_count, got: $actual_count"
        # Print the actual tables to help debugging
        run_sql "SELECT table_schema, table_name FROM information_schema.tables
                WHERE table_schema NOT IN ('mysql', 'information_schema', 'performance_schema', 'sys', 'metrics_schema', 'test')
                AND table_type IN ('BASE TABLE', 'VIEW')
                ORDER BY table_schema, table_name"
        return 1
    fi

    echo "Verified total of $actual_count tables in the cluster (excluding system tables)"
    return 0
}

drop_schemas() {
    local base_name=$1  # base schema name
    local count=$2      # number of schemas to drop

    for i in $(seq 1 $count); do
        run_sql "drop schema if exists ${base_name}_${i};"
    done

    echo "Dropped $count schemas with base name $base_name"
}

create_tables_with_values() {
    local db_name=$1    # database name
    local prefix=$2     # table name prefix
    local count=$3      # number of tables to create
    
    for i in $(seq 1 $count); do
        run_sql "create table $db_name.${prefix}_${i}(c int); insert into $db_name.${prefix}_${i} values ($i);"
    done
}

verify_tables() {
    local db_name=$1       # database name
    local prefix=$2        # table name prefix
    local count=$3         # number of tables to verify
    local should_exist=$4  # true/false - whether tables should exist
    
    for i in $(seq 1 $count); do
        if [ "$should_exist" = "true" ]; then
            run_sql "select count(*) = 1 from $db_name.${prefix}_${i} where c = $i" || {
                echo "Table $db_name.${prefix}_${i} doesn't have expected value $i"
                exit 1
            }
        else
            if run_sql "select * from $db_name.${prefix}_${i}" 2>/dev/null; then
                echo "Table $db_name.${prefix}_${i} exists but should not"
                exit 1
            fi
        fi
    done
}

rename_tables() {
    local db_name=$1       # database name
    local db_name_new=$2
    local old_prefix=$3    # original table name prefix
    local new_prefix=$4    # new table name prefix
    local count=$5         # number of tables to rename
    
    for i in $(seq 1 $count); do
        run_sql "rename table $db_name.${old_prefix}_${i} to $db_name_new.${new_prefix}_${i};"
    done
}

drop_tables() {
    local db_name=$1   # database name
    local prefix=$2    # table name prefix
    local count=$3     # number of tables to drop
    
    for i in $(seq 1 $count); do
        run_sql "drop table $db_name.${prefix}_${i};"
    done
}

test_basic_filter() {
    restart_services || { echo "Failed to restart services"; exit 1; }

    echo "start basic filter testing"

    run_sql "create schema ${DB}_1;"
    run_sql "create schema ${DB}_2;"

    create_tables_with_values "${DB}_1" "initial_tables" 3
    create_tables_with_values "${DB}_1" "prefix_initial_tables" 3
    create_tables_with_values "${DB}_2" "initial_tables_to_drop" 3

    run_br --pd $PD_ADDR log start --task-name $TASK_NAME -s "local://$TEST_DIR/$TASK_NAME/log"

    run_sql "create schema ${DB}_3;"
    run_sql "create schema ${DB}_4;"

    create_tables_with_values "${DB}_3" "full_tables" 3
    create_tables_with_values "${DB}_3" "prefix_full_tables" 3
    create_tables_with_values "${DB}_4" "full_tables_to_drop" 3

    run_br backup full -s "local://$TEST_DIR/$TASK_NAME/full" --pd $PD_ADDR

    echo "write more data and wait for log backup to catch up"

    run_sql "create schema ${DB}_5;"
    run_sql "create schema ${DB}_6;"

    create_tables_with_values "${DB}_5" "prefix_log_tables" 3
    create_tables_with_values "${DB}_5" "PREFIX_LOG_TABLES_UPPER" 3
    create_tables_with_values "${DB}_5" "log_tables" 3
    create_tables_with_values "${DB}_6" "log_tables_to_drop" 3

    drop_tables "${DB}_2" "initial_tables_to_drop" 3
    drop_tables "${DB}_4" "full_tables_to_drop" 3
    drop_tables "${DB}_6" "log_tables_to_drop" 3

    . "$CUR/../br_test_utils.sh" && wait_log_checkpoint_advance "$TASK_NAME"

    # restart services to clean up the cluster
    restart_services || { echo "Failed to restart services"; exit 1; }

    echo "case 1 sanity check, zero filter"
    run_br --pd "$PD_ADDR" restore point -s "local://$TEST_DIR/$TASK_NAME/log" --full-backup-storage "local://$TEST_DIR/$TASK_NAME/full"

    verify_tables "${DB}_1" "initial_tables" 3 true
    verify_tables "${DB}_1" "prefix_initial_tables" 3 true
    verify_tables "${DB}_3" "full_tables" 3 true
    verify_tables "${DB}_3" "prefix_full_tables" 3 true
    verify_tables "${DB}_5" "log_tables" 3 true
    verify_tables "${DB}_5" "prefix_log_tables" 3 true
    verify_tables "${DB}_5" "PREFIX_LOG_TABLES_UPPER" 3 true
    verify_no_unexpected_tables 21 || {
        echo "Found unexpected number of tables in case 1"
        exit 1
    }
    drop_schemas $DB 6

    echo "case 2 with log restore table filter"
    run_br --pd "$PD_ADDR" restore point -s "local://$TEST_DIR/$TASK_NAME/log" --full-backup-storage "local://$TEST_DIR/$TASK_NAME/full" -f "$DB*.prefix*"

    verify_tables "${DB}_1" "prefix_initial_tables" 3 true
    verify_tables "${DB}_3" "prefix_full_tables" 3 true
    verify_tables "${DB}_5" "prefix_log_tables" 3 true
    verify_tables "${DB}_5" "PREFIX_LOG_TABLES_UPPER" 3 true
    verify_no_unexpected_tables 12 || {
        echo "Found unexpected number of tables in case 2"
        exit 1
    }
    drop_schemas $DB 6

    echo "case 3 with multiple filters"
    run_br --pd "$PD_ADDR" restore point -s "local://$TEST_DIR/$TASK_NAME/log" --full-backup-storage "local://$TEST_DIR/$TASK_NAME/full" -f "$DB*.log*" -f "$DB*.prefix*"

    verify_tables "${DB}_1" "prefix_initial_tables" 3 true
    verify_tables "${DB}_3" "prefix_full_tables" 3 true
    verify_tables "${DB}_5" "prefix_log_tables" 3 true
    verify_tables "${DB}_5" "PREFIX_LOG_TABLES_UPPER" 3 true
    verify_tables "${DB}_5" "log_tables" 3 true
    verify_no_unexpected_tables 15 || {
        echo "Found unexpected number of tables in case 3"
        exit 1
    }
    drop_schemas $DB 6

    echo "case 4 with negative filters"
    run_br --pd "$PD_ADDR" restore point -s "local://$TEST_DIR/$TASK_NAME/log" --full-backup-storage "local://$TEST_DIR/$TASK_NAME/full" -f "*.*" -f "!mysql.*" -f "!sys.*" -f "!$DB*.prefix*"

    verify_tables "${DB}_1" "initial_tables" 3 true
    verify_tables "${DB}_3" "full_tables" 3 true
    verify_tables "${DB}_5" "log_tables" 3 true
    verify_no_unexpected_tables 9 || {
        echo "Found unexpected number of tables in case 4"
        exit 1
    }
    drop_schemas $DB 6

    echo "case 5 restore dropped table"
    run_br --pd "$PD_ADDR" restore point -s "local://$TEST_DIR/$TASK_NAME/log" --full-backup-storage "local://$TEST_DIR/$TASK_NAME/full" -f "$DB*.*drop"

    verify_no_unexpected_tables 0 "$DB" || {
        echo "Found unexpected number of tables in case 5"
        exit 1
    }
    drop_schemas $DB 6

    echo "case 6 restore entire database"
    run_br --pd "$PD_ADDR" restore point -s "local://$TEST_DIR/$TASK_NAME/log" --full-backup-storage "local://$TEST_DIR/$TASK_NAME/full" -f "${DB}_1.*" -f "${DB}_3.*" -f "${DB}_5.*"

    verify_tables "${DB}_1" "initial_tables" 3 true
    verify_tables "${DB}_1" "prefix_initial_tables" 3 true
    verify_tables "${DB}_3" "full_tables" 3 true
    verify_tables "${DB}_3" "prefix_full_tables" 3 true
    verify_tables "${DB}_5" "log_tables" 3 true
    verify_tables "${DB}_5" "prefix_log_tables" 3 true
    verify_tables "${DB}_5" "PREFIX_LOG_TABLES_UPPER" 3 true
    verify_no_unexpected_tables 21 || {
        echo "Found unexpected number of tables in case 1"
        exit 1
    }
    drop_schemas $DB 6

    echo "case 7 exact match restore"
    run_br --pd "$PD_ADDR" restore point -s "local://$TEST_DIR/$TASK_NAME/log" --full-backup-storage "local://$TEST_DIR/$TASK_NAME/full" -f "${DB}_1.initial_tables_1" -f "${DB}_1.initial_tables_2" -f "${DB}_1.initial_tables_3"
    verify_tables "${DB}_1" "initial_tables" 3 true
    verify_no_unexpected_tables 3 || {
        echo "Found unexpected number of tables in case 7"
        exit 1
    }

    # cleanup
    rm -rf "$TEST_DIR/$TASK_NAME"

    echo "basic filter test cases passed"
}

test_with_full_backup_filter() {
    restart_services || { echo "Failed to restart services"; exit 1; }

    echo "start with full backup filter testing"

    run_sql "create schema ${DB}_1;"
    run_sql "create schema ${DB}_2;"

    create_tables_with_values "${DB}_1" "initial_tables" 3
    create_tables_with_values "${DB}_2" "initial_tables" 3

    run_br --pd $PD_ADDR log start --task-name $TASK_NAME -s "local://$TEST_DIR/$TASK_NAME/log"

    run_sql "create schema ${DB}_3;"
    run_sql "create schema ${DB}_4;"

    create_tables_with_values "${DB}_3" "full_tables" 3
    create_tables_with_values "${DB}_4" "full_tables" 3

    run_br backup full -f "${DB}_1.*" -f "${DB}_3.*" -s "local://$TEST_DIR/$TASK_NAME/full" --pd $PD_ADDR

    run_sql "create schema ${DB}_5;"
    run_sql "create schema ${DB}_6;"
    create_tables_with_values "${DB}_5" "log_tables" 3
    create_tables_with_values "${DB}_6" "log_tables" 3

    . "$CUR/../br_test_utils.sh" && wait_log_checkpoint_advance "$TASK_NAME"

    # restart services to clean up the cluster
    restart_services || { echo "Failed to restart services"; exit 1; }

    echo "case 8 sanity check, backup filter with pitr zero filter "
    run_br --pd "$PD_ADDR" restore point -s "local://$TEST_DIR/$TASK_NAME/log" --full-backup-storage "local://$TEST_DIR/$TASK_NAME/full"

    verify_tables "${DB}_1" "initial_tables" 3 true
    verify_tables "${DB}_3" "full_tables" 3 true
    verify_tables "${DB}_5" "log_tables" 3 true
    verify_tables "${DB}_6" "log_tables" 3 true
    verify_no_unexpected_tables 12 || {
        echo "Found unexpected number of tables in case 7"
        exit 1
    }
    drop_schemas $DB 6

    echo "case 9 full backup same filter with pitr table filter"
    run_br --pd "$PD_ADDR" restore point -s "local://$TEST_DIR/$TASK_NAME/log" --full-backup-storage "local://$TEST_DIR/$TASK_NAME/full" -f "${DB}_1.*" -f "${DB}_3.*"

    verify_tables "${DB}_1" "initial_tables" 3 true
    verify_tables "${DB}_3" "full_tables" 3 true
    verify_no_unexpected_tables 6 || {
        echo "Found unexpected number of tables in case 8"
        exit 1
    }
    drop_schemas $DB 6

    echo "case 10 with log backup filter include nothing"
    run_br --pd "$PD_ADDR" restore point -s "local://$TEST_DIR/$TASK_NAME/log" --full-backup-storage "local://$TEST_DIR/$TASK_NAME/full" -f "${DB}_2.*" -f "${DB}_4.*"

    verify_no_unexpected_tables 0 || {
        echo "Found unexpected number of tables in case 9"
        exit 1
    }

    # cleanup
    rm -rf "$TEST_DIR/$TASK_NAME"

    echo "with full backup filter test cases passed"
}

test_cover_all_ddl() {
    restart_services || { echo "Failed to restart services"; exit 1; }
    run_sql "set @@global.foreign_key_checks=1;"
    run_sql "set @@global.tidb_enable_check_constraint=1;"
    echo "start all the ddl cover testing"
    
    run_sql_file $CUR/sqls/snapshot.sql

    run_br --pd $PD_ADDR log start --task-name $TASK_NAME -s "local://$TEST_DIR/$TASK_NAME/log"
    run_br backup full -s "local://$TEST_DIR/$TASK_NAME/full" --pd $PD_ADDR

    run_sql_file $CUR/sqls/log.sql

    . "$CUR/../br_test_utils.sh" && wait_log_checkpoint_advance "$TASK_NAME"

    # restart services to clean up the cluster
    restart_services || { echo "Failed to restart services"; exit 1; }
    run_sql "set @@global.foreign_key_checks=1;"
    run_sql "set @@global.tidb_enable_check_constraint=1;"

    run_br --pd "$PD_ADDR" restore point -s "local://$TEST_DIR/$TASK_NAME/log" --full-backup-storage "local://$TEST_DIR/$TASK_NAME/full" -f "test_*.*"

    bash $CUR/sqls/check.sh

    # cleanup
    rm -rf "$TEST_DIR/$TASK_NAME"

    echo "all ddl tests passed"
}

test_table_rename() {
    restart_services || { echo "Failed to restart services"; exit 1; }

    echo "start table rename with filter testing"

    run_sql "create schema ${DB}_1;"
    run_sql "create schema ${DB}_2;"
    run_sql "create schema ${DB}_3;"

    run_sql "create schema ${DB}_drop_and_rename;"

    create_tables_with_values "${DB}_1" "initial_tables_to_rename_in_same_db" 1
    create_tables_with_values "${DB}_1" "initial_tables_to_rename_in_diff_db" 1
    create_tables_with_values "${DB}_2" "prefix_initial_tables_to_rename_out_same_db" 1
    create_tables_with_values "${DB}_2" "prefix_initial_tables_to_rename_out_diff_db" 1
    create_tables_with_values "${DB}_3" "initial_tables_many_rename_in_same_db" 1
    create_tables_with_values "${DB}_3" "initial_tables_many_rename_in_diff_db" 1

    # Create the tables but don't populate them with the standard values
    run_sql "create table ${DB}_drop_and_rename.table_to_drop(c int);"
    run_sql "create table ${DB}_drop_and_rename.table_to_rename(c int);"
    run_sql "insert into ${DB}_drop_and_rename.table_to_drop values (100);"
    run_sql "insert into ${DB}_drop_and_rename.table_to_rename values (200);"

    run_br --pd $PD_ADDR log start --task-name $TASK_NAME -s "local://$TEST_DIR/$TASK_NAME/log"

    run_sql "create schema ${DB}_4;"
    run_sql "create schema ${DB}_5;"
    run_sql "create schema ${DB}_6;"

    create_tables_with_values "${DB}_4" "full_tables_to_rename_in_same_db" 1
    create_tables_with_values "${DB}_4" "full_tables_to_rename_in_diff_db" 1
    create_tables_with_values "${DB}_5" "prefix_full_tables_to_rename_out_same_db" 1
    create_tables_with_values "${DB}_5" "prefix_full_tables_to_rename_out_diff_db" 1
    create_tables_with_values "${DB}_6" "full_tables_many_rename_in_same_db" 1
    create_tables_with_values "${DB}_6" "full_tables_many_rename_in_diff_db" 1

    run_br backup full -s "local://$TEST_DIR/$TASK_NAME/full" --pd $PD_ADDR

    run_sql "create schema ${DB}_7;"
    run_sql "create schema ${DB}_8;"
    run_sql "create schema ${DB}_9;"

    create_tables_with_values "${DB}_7" "log_tables_to_rename_in_same_db" 1
    create_tables_with_values "${DB}_7" "log_tables_to_rename_in_diff_db" 1
    create_tables_with_values "${DB}_8" "prefix_log_tables_to_rename_out_same_db" 1
    create_tables_with_values "${DB}_8" "prefix_log_tables_to_rename_out_diff_db" 1
    create_tables_with_values "${DB}_9" "log_tables_many_rename_in_same_db" 1
    create_tables_with_values "${DB}_9" "log_tables_many_rename_in_diff_db" 1

    # same db rename in
    rename_tables "${DB}_1" "${DB}_1" "initial_tables_to_rename_in_same_db" "prefix_initial_tables_to_rename_in_same_db" 1
    rename_tables "${DB}_4" "${DB}_4" "full_tables_to_rename_in_same_db" "prefix_full_tables_to_rename_in_same_db" 1
    rename_tables "${DB}_7" "${DB}_7" "log_tables_to_rename_in_same_db" "prefix_log_tables_to_rename_in_same_db" 1

    # different db rename in
    rename_tables "${DB}_1" "${DB}_2" "initial_tables_to_rename_in_diff_db" "prefix_initial_tables_to_rename_in_diff_db" 1
    rename_tables "${DB}_4" "${DB}_5" "full_tables_to_rename_in_diff_db" "prefix_full_tables_to_rename_in_diff_db" 1
    rename_tables "${DB}_7" "${DB}_8" "log_tables_to_rename_in_diff_db" "prefix_log_tables_to_rename_in_diff_db" 1

    # same db rename out
    rename_tables "${DB}_2" "${DB}_2" "prefix_initial_tables_to_rename_out_same_db" "initial_tables_to_rename_out_same_db" 1
    rename_tables "${DB}_5" "${DB}_5" "prefix_full_tables_to_rename_out_same_db" "full_tables_to_rename_out_same_db" 1
    rename_tables "${DB}_8" "${DB}_8" "prefix_log_tables_to_rename_out_same_db" "log_tables_to_rename_out_same_db" 1

    # different db rename out
    rename_tables "${DB}_2" "${DB}_1" "prefix_initial_tables_to_rename_out_diff_db" "initial_tables_to_rename_out_diff_db" 1
    rename_tables "${DB}_5" "${DB}_4" "prefix_full_tables_to_rename_out_diff_db" "full_tables_to_rename_out_diff_db" 1
    rename_tables "${DB}_8" "${DB}_7" "prefix_log_tables_to_rename_out_diff_db" "log_tables_to_rename_out_diff_db" 1

    # same db multiple rename in - initial stage
    rename_tables "${DB}_3" "${DB}_3" "initial_tables_many_rename_in_same_db" "initial_tables_many_rename_in_same_db_once" 1
    rename_tables "${DB}_3" "${DB}_3" "initial_tables_many_rename_in_same_db_once" "initial_tables_many_rename_in_same_db_twice" 1
    rename_tables "${DB}_3" "${DB}_3" "initial_tables_many_rename_in_same_db_twice" "prefix_initial_tables_many_rename_in_same_db" 1

    # same db multiple rename in - full stage
    rename_tables "${DB}_6" "${DB}_6" "full_tables_many_rename_in_same_db" "full_tables_many_rename_in_same_db_once" 1
    rename_tables "${DB}_6" "${DB}_6" "full_tables_many_rename_in_same_db_once" "full_tables_many_rename_in_same_db_twice" 1
    rename_tables "${DB}_6" "${DB}_6" "full_tables_many_rename_in_same_db_twice" "prefix_full_tables_many_rename_in_same_db" 1

    # same db multiple rename in - log stage
    rename_tables "${DB}_9" "${DB}_9" "log_tables_many_rename_in_same_db" "log_tables_many_rename_in_same_db_once" 1
    rename_tables "${DB}_9" "${DB}_9" "log_tables_many_rename_in_same_db_once" "log_tables_many_rename_in_same_db_twice" 1
    rename_tables "${DB}_9" "${DB}_9" "log_tables_many_rename_in_same_db_twice" "prefix_log_tables_many_rename_in_same_db" 1

    # cross-stage renames (initial to full, initial to log, full to log)
    rename_tables "${DB}_3" "${DB}_6" "initial_tables_many_rename_in_diff_db" "initial_tables_many_rename_in_diff_db" 1
    rename_tables "${DB}_6" "${DB}_9" "initial_tables_many_rename_in_diff_db" "prefix_initial_tables_many_rename_in_diff_db" 1

    rename_tables "${DB}_6" "${DB}_9" "full_tables_many_rename_in_diff_db" "full_tables_many_rename_in_diff_db" 1
    rename_tables "${DB}_9" "${DB}_3" "full_tables_many_rename_in_diff_db" "prefix_full_tables_many_rename_in_diff_db" 1

    rename_tables "${DB}_9" "${DB}_3" "log_tables_many_rename_in_diff_db" "log_tables_many_rename_in_diff_db" 1
    rename_tables "${DB}_3" "${DB}_6" "log_tables_many_rename_in_diff_db" "prefix_log_tables_many_rename_in_diff_db" 1

    # drop and rename scenario - drop table_to_drop and rename table_to_rename to table_to_drop
    run_sql "DROP TABLE ${DB}_drop_and_rename.table_to_drop;"
    run_sql "RENAME TABLE ${DB}_drop_and_rename.table_to_rename TO ${DB}_drop_and_rename.table_to_drop;"
    
    # add some data to the renamed table
    run_sql "INSERT INTO ${DB}_drop_and_rename.table_to_drop (c) VALUES (300);"

    . "$CUR/../br_test_utils.sh" && wait_log_checkpoint_advance "$TASK_NAME"

    # restart services to clean up the cluster
    restart_services || { echo "Failed to restart services"; exit 1; }

    echo "case 11: rename with filter"
    run_br --pd "$PD_ADDR" restore point -s "local://$TEST_DIR/$TASK_NAME/log" --full-backup-storage "local://$TEST_DIR/$TASK_NAME/full" -f "$DB*.prefix*"

    # Same DB rename in
    verify_tables "${DB}_1" "prefix_initial_tables_to_rename_in_same_db" 1 true
    verify_tables "${DB}_4" "prefix_full_tables_to_rename_in_same_db" 1 true
    verify_tables "${DB}_7" "prefix_log_tables_to_rename_in_same_db" 1 true

    # Different DB rename in
    verify_tables "${DB}_2" "prefix_initial_tables_to_rename_in_diff_db" 1 true
    verify_tables "${DB}_5" "prefix_full_tables_to_rename_in_diff_db" 1 true
    verify_tables "${DB}_8" "prefix_log_tables_to_rename_in_diff_db" 1 true

    # Multiple renames - same db
    verify_tables "${DB}_3" "prefix_initial_tables_many_rename_in_same_db" 1 true
    verify_tables "${DB}_6" "prefix_full_tables_many_rename_in_same_db" 1 true
    verify_tables "${DB}_9" "prefix_log_tables_many_rename_in_same_db" 1 true

    # Mutiple renames cross db
    verify_tables "${DB}_9" "prefix_initial_tables_many_rename_in_diff_db" 1 true
    verify_tables "${DB}_3" "prefix_full_tables_many_rename_in_diff_db" 1 true
    verify_tables "${DB}_6" "prefix_log_tables_many_rename_in_diff_db" 1 true

    verify_no_unexpected_tables 12 || {
        echo "Found unexpected number of tables in Test 10"
        exit 1
    }

    # Drop schemas from the previous test but leave drop_and_rename for our next test
    for i in $(seq 1 9); do
        run_sql "drop schema if exists ${DB}_${i};"
    done

    echo "testing renamed in from another DB"
    run_br --pd "$PD_ADDR" restore point -s "local://$TEST_DIR/$TASK_NAME/log" --full-backup-storage "local://$TEST_DIR/$TASK_NAME/full" -f "${DB}_2.prefix_initial_tables_to_rename_in_diff_db*"

    # verify the table exists and has correct data
    run_sql "SELECT COUNT(*) = 1 FROM ${DB}_2.prefix_initial_tables_to_rename_in_diff_db_1" || {
        echo "prefix_initial_tables_to_rename_in_diff_db_1 doesn't have expected row count"
        exit 1
    }

    # verify the data value is correct
    run_sql "SELECT COUNT(*) = 1 FROM ${DB}_2.prefix_initial_tables_to_rename_in_diff_db_1 WHERE c = 1" || {
        echo "prefix_initial_tables_to_rename_in_diff_db_1 doesn't have expected data"
        exit 1
    }

    # verify no other tables exist
    verify_no_unexpected_tables 1 || {
        echo "Found unexpected number of tables after restore"
        exit 1
    }

    run_sql "drop schema if exists ${DB}_2;"
    
    # Test drop and rename scenario
    echo "testing drop and rename"
    run_br --pd "$PD_ADDR" restore point -s "local://$TEST_DIR/$TASK_NAME/log" --full-backup-storage "local://$TEST_DIR/$TASK_NAME/full" -f "${DB}_drop_and_rename.table_to_drop"

    # Verify the restored table contains data from the original table_to_rename plus the new data
    run_sql "SELECT COUNT(*) = 2 FROM ${DB}_drop_and_rename.table_to_drop" || {
        echo "table_to_drop doesn't have expected data after restore"
        exit 1
    }

    # Verify the data contains both old data from table_to_rename and the data added after rename
    run_sql "SELECT COUNT(*) = 1 FROM ${DB}_drop_and_rename.table_to_drop WHERE c = 200" || {
        echo "table_to_drop missing expected data from table_to_rename"
        exit 1
    }
    run_sql "SELECT COUNT(*) = 1 FROM ${DB}_drop_and_rename.table_to_drop WHERE c = 300" || {
        echo "table_to_drop missing expected data added after rename"
        exit 1
    }

    # Verify table_to_rename doesn't exist
    if run_sql "SELECT * FROM ${DB}_drop_and_rename.table_to_rename" 2>/dev/null; then
        echo "table_to_rename exists but should have been renamed"
        exit 1
    fi

    # Verify the original data from the dropped table is gone by checking that no row
    # with value 100 (from original table_to_drop) exists
    run_sql "SELECT COUNT(*) = 0 FROM ${DB}_drop_and_rename.table_to_drop WHERE c = 100" || {
        echo "Found data from the original dropped table which should be gone"
        exit 1
    }
    # cleanup
    rm -rf "$TEST_DIR/$TASK_NAME"

    echo "table rename with filter passed"
}

test_with_checkpoint() {
    restart_services || { echo "Failed to restart services"; exit 1; }

    echo "case 12: table filter with checkpoint"
    run_br --pd $PD_ADDR log start --task-name $TASK_NAME -s "local://$TEST_DIR/$TASK_NAME/log"

    run_sql "create schema $DB;"

    echo "write initial data and do snapshot backup"
    create_tables_with_values "$DB" "full_backup" 3
    create_tables_with_values "$DB" "renamed_in" 3
    create_tables_with_values "$DB" "log_renamed_out" 3

    run_br backup full -f "$DB.*" -s "local://$TEST_DIR/$TASK_NAME/full" --pd $PD_ADDR

    echo "write more data and wait for log backup to catch up"
    create_tables_with_values "$DB" "log_backup" 3
    rename_tables "$DB" "$DB" "renamed_in" "log_backup_renamed_in" 3
    rename_tables "$DB" "$DB" "log_renamed_out" "renamed_out" 3

    . "$CUR/../br_test_utils.sh" && wait_log_checkpoint_advance "$TASK_NAME"

    # restart services to clean up the cluster
    restart_services || { echo "Failed to restart services"; exit 1; }

    export GO_FAILPOINTS='github.com/pingcap/tidb/br/pkg/restore/snap_client/corrupt-files=return("corrupt-last-table-files")'
    restore_fail=0
    run_br --pd $PD_ADDR restore point --full-backup-storage "local://$TEST_DIR/$TASK_NAME/full" -s "local://$TEST_DIR/$TASK_NAME/log" -f "$DB.log*" || restore_fail=1
    export GO_FAILPOINTS=""
    if [ $restore_fail -ne 1 ]; then
        echo 'expecting full backup last table corruption but success'
        exit 1
    fi

    # PITR with checkpoint but failed in the log restore metakv stage
    export GO_FAILPOINTS='github.com/pingcap/tidb/br/pkg/restore/snap_client/corrupt-files=return("only-last-table-files");github.com/pingcap/tidb/br/pkg/restore/log_client/failed-after-id-maps-saved=return(true)'
    restore_fail=0
    run_br --pd $PD_ADDR restore point --full-backup-storage "local://$TEST_DIR/$TASK_NAME/full" -s "local://$TEST_DIR/$TASK_NAME/log" -f "$DB.log*" || restore_fail=1
    export GO_FAILPOINTS=""
    if [ $restore_fail -ne 1 ]; then
        echo 'expecting failed after id map saved but success'
        exit 1
    fi

    # PITR with checkpoint but failed in the log restore datakv stage
    # skip the snapshot restore stage
    export GO_FAILPOINTS='github.com/pingcap/tidb/br/pkg/task/corrupt-files=return("corrupt-last-table-files")'
    restore_fail=0
    run_br --pd $PD_ADDR restore point --full-backup-storage "local://$TEST_DIR/$TASK_NAME/full" -s "local://$TEST_DIR/$TASK_NAME/log" -f "$DB.log*" || restore_fail=1
    export GO_FAILPOINTS=""
    if [ $restore_fail -ne 1 ]; then
        echo 'expecting log restore last table corruption but success'
        exit 1
    fi

    # PITR with checkpoint
    export GO_FAILPOINTS='github.com/pingcap/tidb/br/pkg/task/corrupt-files=return("only-last-table-files")'
    run_br --pd $PD_ADDR restore point --full-backup-storage "local://$TEST_DIR/$TASK_NAME/full" -s "local://$TEST_DIR/$TASK_NAME/log" -f "$DB.log*"
    export GO_FAILPOINTS=""

    verify_tables "$DB" "log_backup" 3 true
    verify_tables "$DB" "log_backup_renamed_in" 3 true

    verify_no_unexpected_tables 6 || {
        echo "Found unexpected number of tables after checkpoint test"
        exit 1
    }

    # cleanup
    rm -rf "$TEST_DIR/$TASK_NAME"

    echo "table filter checkpoint passed"
}

test_system_tables() {
    restart_services || { echo "Failed to restart services"; exit 1; }

    echo "case 13: pitr table filter with system tables"
    run_br --pd $PD_ADDR log start --task-name $TASK_NAME -s "local://$TEST_DIR/$TASK_NAME/log"

    run_sql "create schema $DB;"
    echo "write initial data and do snapshot backup"
    # make some changes to system tables
    run_sql "create user 'test_user'@'%' identified by 'password';"
    run_sql "grant select on $DB.* to 'test_user'@'%';"

    run_br backup full -s "local://$TEST_DIR/$TASK_NAME/full" --pd $PD_ADDR

    echo "make more changes to system tables and wait for log backup"
    run_sql "create user 'post_backup_user'@'%' identified by 'otherpassword';"
    run_sql "alter user 'test_user'@'%' identified by 'newpassword';"

    . "$CUR/../br_test_utils.sh" && wait_log_checkpoint_advance "$TASK_NAME"

    restart_services || { echo "Failed to restart services"; exit 1; }

    echo "Test 1: Verify that default restore behavior (no filter) properly handles system tables"
    # restore without any filter, should only restore snapshot system tables, not log backup.
    # this is the current behavior as restore log backup to system table will have issue
    run_br --pd "$PD_ADDR" restore point -s "local://$TEST_DIR/$TASK_NAME/log" --full-backup-storage "local://$TEST_DIR/$TASK_NAME/full"


    # verify system tables are restored from snapshot only
    # only test_user should exist, post_backup_user should not exist
    users_result=$(run_sql "SELECT _tidb_rowid, user, host, authentication_string FROM mysql.user WHERE user IN ('test_user', 'post_backup_user')")

    test_user_count=$(echo "$users_result" | grep -c "test_user" || true)

    # Verify there is exactly one test_user
    if [ "$test_user_count" -eq 0 ]; then
        echo "Error: test_user not found in mysql.user table"
        exit 1
    elif [ "$test_user_count" -gt 1 ]; then
        echo "Error: Found $test_user_count instances of test_user in mysql.user table, expected exactly 1"
        echo "Full query result:"
        echo "$users_result"
        exit 1
    fi

    # Check that post_backup_user does not exist (was created after snapshot)
    if echo "$users_result" | grep -q "post_backup_user"; then
        echo "Error: post_backup_user found in mysql.user table but should not be restored"
        echo "Full query result:"
        echo "$users_result"
        exit 1
    fi

    echo "Default restore correctly restored system tables from snapshot only: verified one test_user exists"

    echo "PiTR should error out when system tables are included with explicit filter"
    restore_fail=0
    run_br --pd "$PD_ADDR" restore point -f "*.*" -s "local://$TEST_DIR/$TASK_NAME/log" --full-backup-storage "local://$TEST_DIR/$TASK_NAME/full" || restore_fail=1
    if [ $restore_fail -ne 1 ]; then
        echo "Expected restore to fail when including system tables with filter"
        exit 1
    fi

    # Also verify that specific system table filters fail
    restore_fail=0
    run_br --pd "$PD_ADDR" restore point -f "mysql.*" -s "local://$TEST_DIR/$TASK_NAME/log" --full-backup-storage "local://$TEST_DIR/$TASK_NAME/full" || restore_fail=1
    if [ $restore_fail -ne 1 ]; then
        echo "Expected restore to fail when explicitly filtering system tables"
        exit 1
    fi

    rm -rf "$TEST_DIR/$TASK_NAME"
    echo "system tables test passed"
}

test_foreign_keys() {
    restart_services || { echo "Failed to restart services"; exit 1; }

    run_br --pd $PD_ADDR log start --task-name $TASK_NAME -s "local://$TEST_DIR/$TASK_NAME/log"

    run_sql "create schema $DB;"

    run_sql "CREATE TABLE $DB.departments (
        dept_id INT PRIMARY KEY,
        dept_name VARCHAR(50)
    );"
    run_sql "CREATE TABLE $DB.employees (
        emp_id INT PRIMARY KEY,
        emp_name VARCHAR(50),
        dept_id INT,
        salary INT,
        FOREIGN KEY (dept_id) REFERENCES departments(dept_id)
    );"
    
    run_sql "INSERT INTO $DB.departments VALUES (1, 'Engineering'), (2, 'Sales');"
    run_sql "INSERT INTO $DB.employees VALUES (1, 'John', 1, 5000), (2, 'Jane', 2, 6000);"

    run_br backup full -s "local://$TEST_DIR/$TASK_NAME/full" --pd $PD_ADDR

    echo "make changes and wait for log backup"
    
    run_sql "INSERT INTO $DB.departments VALUES (3, 'Marketing');"
    run_sql "INSERT INTO $DB.employees VALUES (3, 'Bob', 3, 5500);"

    . "$CUR/../br_test_utils.sh" && wait_log_checkpoint_advance "$TASK_NAME"

    restart_services || { echo "Failed to restart services"; exit 1; }

    echo "case 14: forgien key restore only one table - should succeed but queries should fail"
    run_br --pd "$PD_ADDR" restore point -s "local://$TEST_DIR/$TASK_NAME/log" --full-backup-storage "local://$TEST_DIR/$TASK_NAME/full" -f "$DB.employees"

    # verify the employees table exists
    run_sql "SELECT COUNT(*) FROM $DB.employees" || {
        echo "Failed to select from employees table"
        exit 1
    }

    # verify queries involving foreign key fail
    query_fail=0
    run_sql "SELECT e.emp_name, d.dept_name FROM $DB.employees e JOIN $DB.departments d ON e.dept_id = d.dept_id" || query_fail=1
    if [ $query_fail -ne 1 ]; then
        echo "Expected JOIN query to fail due to missing departments table"
        exit 1
    fi

    # verify insert succeeds even without the referenced table
    run_sql "INSERT INTO $DB.employees VALUES (4, 'Alice', 1, 6000)" || {
        echo "Failed to insert into employees table"
        exit 1
    }
    # Verify the insert worked
    run_sql "SELECT COUNT(*) = 1 FROM $DB.employees WHERE emp_id = 4" || {
        echo "Newly inserted row not found in employees table"
        exit 1
    }

    run_sql "drop schema $DB;"

    echo "case 15: forgien key restore both tables - should succeed"
    # restore both tables - should succeed
    run_br --pd "$PD_ADDR" restore point -s "local://$TEST_DIR/$TASK_NAME/log" --full-backup-storage "local://$TEST_DIR/$TASK_NAME/full" -f "$DB.employees" -f "$DB.departments"

    # verify foreign key relationship works
    run_sql "SELECT COUNT(*) = 3 FROM $DB.employees e JOIN $DB.departments d ON e.dept_id = d.dept_id" || {
        echo "Foreign key relationship not working after restore"
        exit 1
    }

    # cleanup
    rm -rf "$TEST_DIR/$TASK_NAME"

    echo "schema objects test passed"
}

test_index_filter() {
    restart_services || { echo "Failed to restart services"; exit 1; }

    echo "case 16: indexes with pitr filter"

    run_sql "create schema $DB;"

    # create tables with different index types
    run_sql "CREATE TABLE $DB.btree_index_table (
        id INT PRIMARY KEY,
        name VARCHAR(50),
        age INT,
        INDEX idx_name_age(name, age) USING BTREE
    );"

    run_sql "CREATE TABLE $DB.hash_index_table (
        id INT,
        value INT,
        PRIMARY KEY(id) USING HASH,
        INDEX idx_value(value) USING HASH
    );"

    run_sql "CREATE TABLE $DB.multi_index_table (
        id INT PRIMARY KEY,
        data JSON,
        INDEX idx_multi((CAST(data->'$.tags' AS CHAR(64) ARRAY)))
    );"

    run_br --pd $PD_ADDR log start --task-name $TASK_NAME -s "local://$TEST_DIR/$TASK_NAME/log"

    run_sql "INSERT INTO $DB.btree_index_table VALUES (1, 'Alice', 25), (2, 'Bob', 30);"
    run_sql "INSERT INTO $DB.hash_index_table VALUES (1, 100), (2, 200);"
    run_sql "INSERT INTO $DB.multi_index_table VALUES 
        (1, '{\"tags\": [\"tag1\", \"tag2\"]}'),
        (2, '{\"tags\": [\"tag3\", \"tag4\"]}');"

    run_br backup full -s "local://$TEST_DIR/$TASK_NAME/full" --pd $PD_ADDR

    run_sql "INSERT INTO $DB.btree_index_table VALUES (3, 'Charlie', 35);"
    run_sql "INSERT INTO $DB.hash_index_table VALUES (3, 300);"
    run_sql "INSERT INTO $DB.multi_index_table VALUES (3, '{\"tags\": [\"tag5\", \"tag6\"]}');"
    
    # add new indexes after backup
    run_sql "ALTER TABLE $DB.btree_index_table ADD INDEX idx_age(age);"
    run_sql "ALTER TABLE $DB.hash_index_table ADD UNIQUE INDEX uniq_value(value);"

    . "$CUR/../br_test_utils.sh" && wait_log_checkpoint_advance "$TASK_NAME"

    restart_services || { echo "Failed to restart services"; exit 1; }

    # Test case 1: Restore btree index table
    echo "Testing restoration of btree index table"
    run_br --pd "$PD_ADDR" restore point -s "local://$TEST_DIR/$TASK_NAME/log" --full-backup-storage "local://$TEST_DIR/$TASK_NAME/full" -f "$DB.btree*"

    # verify data and indexes
    run_sql "SELECT COUNT(*) = 3 FROM $DB.btree_index_table" || {
        echo "btree_index_table doesn't have expected row count"
        exit 1
    }

    # verify index structure exists
    run_sql "SHOW INDEX FROM $DB.btree_index_table WHERE Key_name = 'idx_name_age'" || {
        echo "idx_name_age index not found in btree_index_table"
        exit 1
    }
    run_sql "SHOW INDEX FROM $DB.btree_index_table WHERE Key_name = 'idx_age'" || {
        echo "idx_age index not found in btree_index_table"
        exit 1
    }

    # run admin check table to validate indexes
    echo "Running admin check table to validate indexes..."
    run_sql "ADMIN CHECK TABLE $DB.btree_index_table" || {
        echo "Admin check table failed for btree_index_table"
        exit 1
    }

    # verify explicit index usage with USE INDEX hint
    echo "Verifying explicit index usage with USE INDEX hint..."
    run_sql "SELECT * FROM $DB.btree_index_table USE INDEX(idx_name_age) WHERE name = 'Alice' AND age = 25" || {
        echo "Failed to use idx_name_age index explicitly on btree_index_table"
        exit 1
    }
    run_sql "SELECT * FROM $DB.btree_index_table USE INDEX(idx_age) WHERE age = 25" || {
        echo "Failed to use idx_age index explicitly on btree_index_table"
        exit 1
    }

    # other tables should not exist
    if run_sql "SELECT * FROM $DB.hash_index_table" 2>/dev/null; then
        echo "hash_index_table exists but should not with btree* filter"
        exit 1
    fi

    run_sql "drop schema $DB;"

    # Test case 2: Restore all tables with indexes
    echo "Testing restoration of all tables with indexes"
    run_br --pd "$PD_ADDR" restore point -s "local://$TEST_DIR/$TASK_NAME/log" --full-backup-storage "local://$TEST_DIR/$TASK_NAME/full" -f "$DB.*"

    # verify all tables exist with correct data
    run_sql "SELECT COUNT(*) = 3 FROM $DB.btree_index_table" || {
        echo "btree_index_table doesn't have expected row count"
        exit 1
    }
    run_sql "SELECT COUNT(*) = 3 FROM $DB.hash_index_table" || {
        echo "hash_index_table doesn't have expected row count"
        exit 1
    }
    run_sql "SELECT COUNT(*) = 3 FROM $DB.multi_index_table" || {
        echo "multi_index_table doesn't have expected row count"
        exit 1
    }

    # run admin check table to validate indexes for all tables
    echo "Running admin check table to validate indexes for all tables..."
    run_sql "ADMIN CHECK TABLE $DB.btree_index_table" || {
        echo "Admin check table failed for btree_index_table"
        exit 1
    }
    run_sql "ADMIN CHECK TABLE $DB.hash_index_table" || {
        echo "Admin check table failed for hash_index_table"
        exit 1
    }
    run_sql "ADMIN CHECK TABLE $DB.multi_index_table" || {
        echo "Admin check table failed for multi_index_table"
        exit 1
    }


    # verify explicit index usage with USE INDEX hint for all tables
    echo "Verifying explicit index usage with USE INDEX hint for all tables..."
    run_sql "SELECT * FROM $DB.btree_index_table USE INDEX(idx_name_age) WHERE name = 'Alice' AND age = 25" || {
        echo "Failed to use idx_name_age index explicitly on btree_index_table"
        exit 1
    }
    run_sql "SELECT * FROM $DB.hash_index_table USE INDEX(idx_value) WHERE value = 100" || {
        echo "Failed to use idx_value index explicitly on hash_index_table"
        exit 1
    }
    run_sql "EXPLAIN SELECT * FROM $DB.multi_index_table WHERE JSON_CONTAINS(data->'$.tags', '\"tag1\"')" || {
        echo "Failed to use idx_multi index on multi_index_table"
        exit 1
    }

    # cleanup
    rm -rf "$TEST_DIR/$TASK_NAME"

    echo "Index filter test passed"
}

test_partition_exchange() {
    restart_services || { echo "Failed to restart services"; exit 1; }

    echo "case 17: start testing partition exchange with filter"

    run_sql "create schema $DB;"

    # Create tables that will be in backup
    echo "creating tables for backup..."
    run_sql "CREATE TABLE $DB.backup_source (
        id INT,
        value INT,
        PRIMARY KEY(id, value)
    ) PARTITION BY RANGE (value) (
        PARTITION p0 VALUES LESS THAN (100),
        PARTITION p1 VALUES LESS THAN (200)
    );"

    run_sql "CREATE TABLE $DB.backup_target1 (
        id INT,
        value INT,
        PRIMARY KEY(id, value)
    );"

    run_sql "CREATE TABLE $DB.backup_target2 (
        id INT,
        value INT,
        PRIMARY KEY(id, value)
    );"

    run_br --pd $PD_ADDR log start --task-name $TASK_NAME -s "local://$TEST_DIR/$TASK_NAME/log"

    # Insert data into backup tables
    run_sql "INSERT INTO $DB.backup_source VALUES (1, 50), (2, 150);"
    run_sql "INSERT INTO $DB.backup_target1 VALUES (3, 50);"
    run_sql "INSERT INTO $DB.backup_target2 VALUES (4, 150);"

    # Take full backup
    run_br backup full -s "local://$TEST_DIR/$TASK_NAME/full" --pd $PD_ADDR

    # Create tables that will only exist in log
    echo "creating tables that will only exist in log..."
    run_sql "CREATE TABLE $DB.log_source (
        id INT,
        value INT,
        PRIMARY KEY(id, value)
    ) PARTITION BY RANGE (value) (
        PARTITION p0 VALUES LESS THAN (100),
        PARTITION p1 VALUES LESS THAN (200)
    );"

    run_sql "CREATE TABLE $DB.log_target1 (
        id INT,
        value INT,
        PRIMARY KEY(id, value)
    );"

    run_sql "CREATE TABLE $DB.log_target2 (
        id INT,
        value INT,
        PRIMARY KEY(id, value)
    );"

    # Insert data into log-only tables
    run_sql "INSERT INTO $DB.log_source VALUES (5, 50), (6, 150);"
    run_sql "INSERT INTO $DB.log_target1 VALUES (7, 50);"
    run_sql "INSERT INTO $DB.log_target2 VALUES (8, 150);"

    echo "performing all partition exchange operations..."

    # Case 1: Exchange between backup tables
    run_sql "ALTER TABLE $DB.backup_source EXCHANGE PARTITION p0 WITH TABLE $DB.backup_target1;"

    # Case 2: Exchange between log-only tables
    run_sql "ALTER TABLE $DB.log_source EXCHANGE PARTITION p0 WITH TABLE $DB.log_target1;"

    # Case 3: Exchange between backup source and log target
    run_sql "ALTER TABLE $DB.backup_source EXCHANGE PARTITION p1 WITH TABLE $DB.log_target2;"

    # Case 4: Exchange between log source and backup target
    run_sql "ALTER TABLE $DB.log_source EXCHANGE PARTITION p1 WITH TABLE $DB.backup_target2;"

    # Wait for log backup to catch up with all operations
    . "$CUR/../br_test_utils.sh" && wait_log_checkpoint_advance "$TASK_NAME"

    # Stop log backup before starting restore tests
    run_br log stop --task-name $TASK_NAME

    echo "starting restore tests..."

    # Test 1: Backup source and all in filter - should succeed
    echo "test 1: backup source and all in filter"
    run_sql "drop schema if exists $DB;"
    run_br --pd "$PD_ADDR" restore point -s "local://$TEST_DIR/$TASK_NAME/log" \
        --full-backup-storage "local://$TEST_DIR/$TASK_NAME/full" \
        -f "$DB.backup_source" -f "$DB.backup_target1" -f "$DB.log_target2"|| {
        echo "Failed: backup source and all in filter should succeed"
        exit 1
    }
    # Verify data after restore
    run_sql "SELECT COUNT(*) = 1 FROM $DB.backup_source PARTITION (p0) WHERE id = 3 AND value = 50" || {
        echo "backup_source p0 doesn't have expected data after restore"
        exit 1
    }
    run_sql "SELECT COUNT(*) = 1 FROM $DB.backup_target1 WHERE id = 1 AND value = 50" || {
        echo "backup_target1 doesn't have expected data after restore"
        exit 1
    }
    run_sql "SELECT COUNT(*) = 1 FROM $DB.log_target2 WHERE id = 2 AND value = 150" || {
        echo "backup_target1 doesn't have expected data after restore"
        exit 1
    }

    # Test 2: Log source and all in filter - should succeed
    echo "test 2: log source and all in filter"
    run_sql "drop schema if exists $DB;"
    run_br --pd "$PD_ADDR" restore point -s "local://$TEST_DIR/$TASK_NAME/log" \
        --full-backup-storage "local://$TEST_DIR/$TASK_NAME/full" \
        -f "$DB.log_source" -f "$DB.log_target1" -f "$DB.backup_target2" || {
        echo "Failed: log source and all in filter should succeed"
        exit 1
    }
    # Verify data after restore
    run_sql "SELECT COUNT(*) = 1 FROM $DB.log_source PARTITION (p0) WHERE id = 7 AND value = 50" || {
        echo "log_source p0 doesn't have expected data after restore"
        exit 1
    }
    run_sql "SELECT COUNT(*) = 1 FROM $DB.log_target1 WHERE id = 5 AND value = 50" || {
        echo "log_target1 doesn't have expected data after restore"
        exit 1
    }
    run_sql "SELECT COUNT(*) = 1 FROM $DB.backup_target2 WHERE id = 6 AND value = 150" || {
        echo "backup_target1 doesn't have expected data after restore"
        exit 1
    }

    # Test 3: Only backup source in filter - should fail
    echo "test 3: only backup source in filter"
    run_sql "drop schema if exists $DB;"
    restore_fail=0
    run_br --pd "$PD_ADDR" restore point -s "local://$TEST_DIR/$TASK_NAME/log" \
        --full-backup-storage "local://$TEST_DIR/$TASK_NAME/full" \
        -f "$DB.backup_source" || restore_fail=1
    if [ $restore_fail -ne 1 ]; then
        echo "Failed: backup source only in filter should fail"
        exit 1
    fi
    run_br --pd "$PD_ADDR" restore point -s "local://$TEST_DIR/$TASK_NAME/log" \
        --full-backup-storage "local://$TEST_DIR/$TASK_NAME/full" \
        -f "$DB.backup_source" 2>&1 | grep "partition exchange detected" || {
        echo "Error message does not contain partition exchange information"
        exit 1
    }

    # Test 4: Only backup target in filter - should fail
    echo "test 4: only backup target in filter"
    run_sql "drop schema if exists $DB;"
    restore_fail=0
    run_br --pd "$PD_ADDR" restore point -s "local://$TEST_DIR/$TASK_NAME/log" \
        --full-backup-storage "local://$TEST_DIR/$TASK_NAME/full" \
        -f "$DB.backup_target1" || restore_fail=1
    if [ $restore_fail -ne 1 ]; then
        echo "Failed: backup target only in filter should fail"
        exit 1
    fi

    restore_fail=0
    run_br --pd "$PD_ADDR" restore point -s "local://$TEST_DIR/$TASK_NAME/log" \
        --full-backup-storage "local://$TEST_DIR/$TASK_NAME/full" \
        -f "$DB.backup_target2" || restore_fail=1
    if [ $restore_fail -ne 1 ]; then
        echo "Failed: backup target only in filter should fail"
        exit 1
    fi

    # Test 5: Only log source in filter - should fail
    echo "test 5: only log source in filter"
    run_sql "drop schema if exists $DB;"
    restore_fail=0
    run_br --pd "$PD_ADDR" restore point -s "local://$TEST_DIR/$TASK_NAME/log" \
        --full-backup-storage "local://$TEST_DIR/$TASK_NAME/full" \
        -f "$DB.log_source" || restore_fail=1
    if [ $restore_fail -ne 1 ]; then
        echo "Failed: log source only in filter should fail"
        exit 1
    fi

    # Test 6: Only log target in filter - should fail
    echo "test 6: only log target in filter"
    run_sql "drop schema if exists $DB;"
    restore_fail=0
    run_br --pd "$PD_ADDR" restore point -s "local://$TEST_DIR/$TASK_NAME/log" \
        --full-backup-storage "local://$TEST_DIR/$TASK_NAME/full" \
        -f "$DB.log_target1" || restore_fail=1
    if [ $restore_fail -ne 1 ]; then
        echo "Failed: log target only in filter should fail"
        exit 1
    fi

    restore_fail=0
    run_br --pd "$PD_ADDR" restore point -s "local://$TEST_DIR/$TASK_NAME/log" \
        --full-backup-storage "local://$TEST_DIR/$TASK_NAME/full" \
        -f "$DB.log_target2" || restore_fail=1
    if [ $restore_fail -ne 1 ]; then
        echo "Failed: log target only in filter should fail"
        exit 1
    fi

    # Test 7: Neither table in filter - should succeed with no tables
    echo "test 7: neither table in filter"
    run_sql "drop schema if exists $DB;"
    run_br --pd "$PD_ADDR" restore point -s "local://$TEST_DIR/$TASK_NAME/log" \
        --full-backup-storage "local://$TEST_DIR/$TASK_NAME/full" \
        -f "$DB.unrelated_table" || {
        echo "Failed: neither table in filter should succeed"
        exit 1
    }
    # Verify no tables were restored
    verify_no_unexpected_tables 0 "$DB" || {
        echo "Found unexpected tables after neither table in filter"
        exit 1
    }

    # Test 8: Wildcard filter including all tables - should succeed
    echo "test 8: wildcard filter including all tables"
    run_sql "drop schema if exists $DB;"
    run_br --pd "$PD_ADDR" restore point -s "local://$TEST_DIR/$TASK_NAME/log" \
        --full-backup-storage "local://$TEST_DIR/$TASK_NAME/full" \
        -f "$DB.*" || {
        echo "Failed: wildcard filter should succeed"
        exit 1
    }
    # Verify all tables are restored
    verify_no_unexpected_tables 6 "$DB" || {
        echo "Wrong number of tables restored with wildcard filter"
        exit 1
    }

    # cleanup
    rm -rf "$TEST_DIR/$TASK_NAME"

    echo "partition exchange test passed"
}

test_table_truncation() {
    restart_services || { echo "Failed to restart services"; exit 1; }

    echo "case 18: start testing table truncation with filter"

    run_sql "create schema $DB;"

    # Create tables for snapshot backup
    echo "creating tables for snapshot backup..."
    run_sql "CREATE TABLE $DB.snapshot_truncate (
        id INT PRIMARY KEY,
        value VARCHAR(50)
    );"

    run_br --pd $PD_ADDR log start --task-name $TASK_NAME -s "local://$TEST_DIR/$TASK_NAME/log"

    # Insert initial data
    run_sql "INSERT INTO $DB.snapshot_truncate VALUES (1, 'initial data 1'), (2, 'initial data 2');"
    
    # Take full backup
    run_br backup full -s "local://$TEST_DIR/$TASK_NAME/full" --pd $PD_ADDR

    # Create tables during log backup phase
    echo "creating tables during log backup phase..."
    run_sql "CREATE TABLE $DB.log_truncate (
        id INT PRIMARY KEY,
        value VARCHAR(50)
    );"
    
    # Insert data into log-created table
    run_sql "INSERT INTO $DB.log_truncate VALUES (1, 'log data 1'), (2, 'log data 2');"
    
    # Add more data to snapshot table before truncation
    run_sql "INSERT INTO $DB.snapshot_truncate VALUES (3, 'pre-truncate data');"
    
    # Truncate both tables
    echo "truncating tables..."
    run_sql "TRUNCATE TABLE $DB.snapshot_truncate;"
    run_sql "TRUNCATE TABLE $DB.log_truncate;"
    
    # Insert new data after truncation
    run_sql "INSERT INTO $DB.snapshot_truncate VALUES (10, 'post-truncate data 1'), (20, 'post-truncate data 2');"
    run_sql "INSERT INTO $DB.log_truncate VALUES (10, 'post-truncate data 1'), (20, 'post-truncate data 2');"
    
    . "$CUR/../br_test_utils.sh" && wait_log_checkpoint_advance "$TASK_NAME"
    
    # Stop log backup before starting restore operations
    run_br log stop --task-name $TASK_NAME --pd $PD_ADDR
    
    run_sql "drop schema if exists $DB;"

    echo "Test 1: Restore both truncated tables"
    run_br --pd "$PD_ADDR" restore point -s "local://$TEST_DIR/$TASK_NAME/log" \
        --full-backup-storage "local://$TEST_DIR/$TASK_NAME/full" \
        -f "$DB.*"
    
    # Verify data after restore - should only have post-truncate data
    run_sql "SELECT COUNT(*) = 2 FROM $DB.snapshot_truncate" || {
        echo "snapshot_truncate doesn't have expected row count after restore"
        exit 1
    }
    
    run_sql "SELECT COUNT(*) = 2 FROM $DB.log_truncate" || {
        echo "log_truncate doesn't have expected row count after restore"
        exit 1
    }
    
    # Verify specific values to ensure we have post-truncate data
    run_sql "SELECT COUNT(*) = 1 FROM $DB.snapshot_truncate WHERE id = 10" || {
        echo "snapshot_truncate doesn't have expected post-truncate data"
        exit 1
    }
    
    run_sql "SELECT COUNT(*) = 1 FROM $DB.log_truncate WHERE id = 10" || {
        echo "log_truncate doesn't have expected post-truncate data"
        exit 1
    }
    
    # Verify pre-truncate data is gone
    run_sql "SELECT COUNT(*) = 0 FROM $DB.snapshot_truncate WHERE id IN (1, 2, 3)" || {
        echo "snapshot_truncate still has pre-truncate data which should be gone"
        exit 1
    }
    
    run_sql "SELECT COUNT(*) = 0 FROM $DB.log_truncate WHERE id IN (1, 2)" || {
        echo "log_truncate still has pre-truncate data which should be gone"
        exit 1
    }
    
    # Test 2: Restore only snapshot table
    run_sql "drop schema if exists $DB;"
    run_br --pd "$PD_ADDR" restore point -s "local://$TEST_DIR/$TASK_NAME/log" \
        --full-backup-storage "local://$TEST_DIR/$TASK_NAME/full" \
        -f "$DB.snapshot_truncate"
    
    # Verify only snapshot table exists
    verify_no_unexpected_tables 1 "$DB" || {
        echo "Wrong number of tables restored with snapshot_truncate filter"
        exit 1
    }
    
    # Verify data is correct
    run_sql "SELECT COUNT(*) = 2 FROM $DB.snapshot_truncate" || {
        echo "snapshot_truncate doesn't have expected row count after filtered restore"
        exit 1
    }
    
    # Test 3: Restore only log table
    run_sql "drop schema if exists $DB;"
    run_br --pd "$PD_ADDR" restore point -s "local://$TEST_DIR/$TASK_NAME/log" \
        --full-backup-storage "local://$TEST_DIR/$TASK_NAME/full" \
        -f "$DB.log_truncate"
    
    # Verify only log table exists
    verify_no_unexpected_tables 1 "$DB" || {
        echo "Wrong number of tables restored with log_truncate filter"
        exit 1
    }
    
    # Verify data is correct
    run_sql "SELECT COUNT(*) = 2 FROM $DB.log_truncate" || {
        echo "log_truncate doesn't have expected row count after filtered restore"
        exit 1
    }
    
    # cleanup
    rm -rf "$TEST_DIR/$TASK_NAME"
    
    echo "table truncation test passed"
}

test_sequential_restore() {
    restart_services || { echo "Failed to restart services"; exit 1; }

    echo "case 19: start testing sequential table restore with filter"

    run_sql "create schema $DB;"

    # Create multiple tables with different data
    echo "creating tables for testing sequential restore..."
    run_sql "CREATE TABLE $DB.table1 (
        id INT PRIMARY KEY,
        value VARCHAR(50)
    );"
    run_sql "CREATE TABLE $DB.table2 (
        id INT PRIMARY KEY,
        value VARCHAR(50)
    );"
    run_sql "CREATE TABLE $DB.table3 (
        id INT PRIMARY KEY,
        value VARCHAR(50)
    );"

    run_br --pd $PD_ADDR log start --task-name $TASK_NAME -s "local://$TEST_DIR/$TASK_NAME/log"

    # Insert initial data
    run_sql "INSERT INTO $DB.table1 VALUES (1, 'table1 data 1'), (2, 'table1 data 2');"
    run_sql "INSERT INTO $DB.table2 VALUES (1, 'table2 data 1'), (2, 'table2 data 2');"
    run_sql "INSERT INTO $DB.table3 VALUES (1, 'table3 data 1'), (2, 'table3 data 2');"
    
    # Take full backup
    run_br backup full -s "local://$TEST_DIR/$TASK_NAME/full" --pd $PD_ADDR

    # Add more data after backup
    run_sql "INSERT INTO $DB.table1 VALUES (3, 'table1 data 3'), (4, 'table1 data 4');"
    run_sql "INSERT INTO $DB.table2 VALUES (3, 'table2 data 3'), (4, 'table2 data 4');"
    run_sql "INSERT INTO $DB.table3 VALUES (3, 'table3 data 3'), (4, 'table3 data 4');"
    
    # Wait for log backup to catch up with all operations
    . "$CUR/../br_test_utils.sh" && wait_log_checkpoint_advance "$TASK_NAME"
    
    # Stop log backup before starting restore operations
    run_br log stop --task-name $TASK_NAME --pd $PD_ADDR
    
    # Clean up the database before starting the sequential restore tests
    run_sql "drop schema if exists $DB;"
    
    echo "Test 1: Restore first table"
    run_br --pd "$PD_ADDR" restore point -s "local://$TEST_DIR/$TASK_NAME/log" \
        --full-backup-storage "local://$TEST_DIR/$TASK_NAME/full" \
        -f "$DB.table1"
    
    # Verify only table1 exists with correct data
    verify_no_unexpected_tables 1 "$DB" || {
        echo "Wrong number of tables after restoring table1"
        exit 1
    }
    
    run_sql "SELECT COUNT(*) = 4 FROM $DB.table1" || {
        echo "table1 doesn't have expected row count after restore"
        exit 1
    }
    
    echo "Test 2: Restore second table without cleaning up"
    run_br --pd "$PD_ADDR" restore point -s "local://$TEST_DIR/$TASK_NAME/log" \
        --full-backup-storage "local://$TEST_DIR/$TASK_NAME/full" \
        -f "$DB.table2"
    
    # Verify both table1 and table2 exist with correct data
    verify_no_unexpected_tables 2 "$DB" || {
        echo "Wrong number of tables after restoring table2"
        exit 1
    }
    
    run_sql "SELECT COUNT(*) = 4 FROM $DB.table1" || {
        echo "table1 doesn't have expected row count after second restore"
        exit 1
    }
    
    run_sql "SELECT COUNT(*) = 4 FROM $DB.table2" || {
        echo "table2 doesn't have expected row count after restore"
        exit 1
    }
    
    echo "Test 3: Restore third table without cleaning up"
    run_br --pd "$PD_ADDR" restore point -s "local://$TEST_DIR/$TASK_NAME/log" \
        --full-backup-storage "local://$TEST_DIR/$TASK_NAME/full" \
        -f "$DB.table3"
    
    # Verify all three tables exist with correct data
    verify_no_unexpected_tables 3 "$DB" || {
        echo "Wrong number of tables after restoring table3"
        exit 1
    }
    
    run_sql "SELECT COUNT(*) = 4 FROM $DB.table1" || {
        echo "table1 doesn't have expected row count after third restore"
        exit 1
    }
    
    run_sql "SELECT COUNT(*) = 4 FROM $DB.table2" || {
        echo "table2 doesn't have expected row count after third restore"
        exit 1
    }
    
    run_sql "SELECT COUNT(*) = 4 FROM $DB.table3" || {
        echo "table3 doesn't have expected row count after restore"
        exit 1
    }
    
    # cleanup
    rm -rf "$TEST_DIR/$TASK_NAME"
    
    echo "sequential restore test passed"
}

test_log_compaction() {
    restart_services || { echo "Failed to restart services"; exit 1; }

    echo "case 20: start testing table filter with log compaction"

    run_sql "create schema $DB;"

    # Create tables for snapshot backup
    echo "creating tables for snapshot backup..."
    run_sql "CREATE TABLE $DB.compaction_snapshot (
        id INT PRIMARY KEY,
        value VARCHAR(50)
    );"

    run_br --pd $PD_ADDR log start --task-name $TASK_NAME -s "local://$TEST_DIR/$TASK_NAME/log"

    # Insert initial data
    run_sql "INSERT INTO $DB.compaction_snapshot VALUES (1, 'initial data 1'), (2, 'initial data 2');"
    
    # Take full backup
    run_br backup full -s "local://$TEST_DIR/$TASK_NAME/full" --pd $PD_ADDR

    # Create tables during log backup phase
    echo "creating tables during log backup phase..."
    run_sql "CREATE TABLE $DB.compaction_log (
        id INT PRIMARY KEY,
        value VARCHAR(50)
    );"
    
    # Insert data into log-created table
    run_sql "INSERT INTO $DB.compaction_log VALUES (1, 'log data 1'), (2, 'log data 2');"
    
    # Add more data to snapshot table
    run_sql "INSERT INTO $DB.compaction_snapshot VALUES (3, 'more data 1'), (4, 'more data 2');"
    
    # Wait for log backup to catch up with all operations
    current_ts=$(python3 -c "import time; print(int(time.time() * 1000) << 18)")
    . "$CUR/../br_test_utils.sh" && wait_log_checkpoint_advance "$TASK_NAME"
    
    # Verify no SST files exist before compaction
    pre_compaction_files=$(find "$TEST_DIR/$TASK_NAME/log" -name "*.sst" | wc -l)
    if [ "$pre_compaction_files" -ne 0 ]; then
        echo "Found $pre_compaction_files SST files before compaction, expected 0"
        exit 1
    fi
    echo "Verified no SST files exist before compaction"
    
    # Step 1: Get the Base64 encoded storage URL
    echo "Encoding storage URL to Base64"

    # Run the base64ify command and capture its output, redirecting stderr to stdout
    base64_output=$(run_br operator base64ify --storage "local://$TEST_DIR/$TASK_NAME/log" 2>&1)

    # Extract only lines that look like Base64 (long string of base64 chars)
    storage_base64=$(echo "$base64_output" | grep -o '[A-Za-z0-9+/]\{20,\}=\{0,2\}' | grep '^E' | head -1)

    # Verify that we got a valid Base64 string
    if [ -z "$storage_base64" ]; then
        echo "Failed to extract Base64 encoded storage URL. Full output:"
        echo "$base64_output"
        exit 1
    fi

    echo "Extracted Base64 encoded storage URL: $storage_base64"
    
    # Get current timestamp and a timestamp from 1 hour ago for compaction range
    one_hour_ago_ts=$(python3 -c "import time; print(int((time.time() - 3600) * 1000) << 18)")
    
    echo "Current timestamp: $current_ts"
    echo "One hour ago timestamp: $one_hour_ago_ts"
    
    echo "Compacting logs from $one_hour_ago_ts to $current_ts"
    
    # Run tikv-ctl to perform compaction
    tikv-ctl --log-level=info compact-log-backup --from "$one_hour_ago_ts" --until "$current_ts" -s "$storage_base64" -N 4 --minimal-compaction-size 0

    # Verify SST files exist after compaction
    post_compaction_files=$(find "$TEST_DIR/$TASK_NAME/log" -name "*.sst" | wc -l)
    if [ "$post_compaction_files" -eq 0 ]; then
        echo "No SST files found after compaction, expected at least 1"
        exit 1
    fi
    echo "Verified $post_compaction_files SST files exist after compaction"

    # Add more data after compaction
    run_sql "INSERT INTO $DB.compaction_snapshot VALUES (5, 'post-compaction data 1');"
    run_sql "INSERT INTO $DB.compaction_log VALUES (3, 'post-compaction data 2');"
    
    # Wait for log backup to catch up again
    . "$CUR/../br_test_utils.sh" && wait_log_checkpoint_advance "$TASK_NAME"
    
    # Stop log backup before starting restore operations
    run_br log stop --task-name $TASK_NAME --pd $PD_ADDR
    
    run_sql "drop schema if exists $DB;"

    echo "Test 1: Restore both tables with compacted logs"
    run_br --pd "$PD_ADDR" restore point -s "local://$TEST_DIR/$TASK_NAME/log" \
        --full-backup-storage "local://$TEST_DIR/$TASK_NAME/full" \
        -f "$DB.*"
    
    # Verify data after restore - should have all data including post-compaction
    run_sql "SELECT COUNT(*) = 5 FROM $DB.compaction_snapshot" || {
        echo "compaction_snapshot doesn't have expected row count after restore"
        exit 1
    }
    
    run_sql "SELECT COUNT(*) = 3 FROM $DB.compaction_log" || {
        echo "compaction_log doesn't have expected row count after restore"
        exit 1
    }
    
    # Verify specific values to ensure we have post-compaction data
    run_sql "SELECT COUNT(*) = 1 FROM $DB.compaction_snapshot WHERE id = 5" || {
        echo "compaction_snapshot doesn't have expected post-compaction data"
        exit 1
    }
    
    run_sql "SELECT COUNT(*) = 1 FROM $DB.compaction_log WHERE id = 3" || {
        echo "compaction_log doesn't have expected post-compaction data"
        exit 1
    }
    
    # Test 2: Restore only snapshot table with filter
    run_sql "drop schema if exists $DB;"
    run_br --pd "$PD_ADDR" restore point -s "local://$TEST_DIR/$TASK_NAME/log" \
        --full-backup-storage "local://$TEST_DIR/$TASK_NAME/full" \
        -f "$DB.compaction_snapshot"
    
    # Verify only snapshot table exists
    verify_no_unexpected_tables 1 "$DB" || {
        echo "Wrong number of tables restored with compaction_snapshot filter"
        exit 1
    }
    
    # Verify data is correct
    run_sql "SELECT COUNT(*) = 5 FROM $DB.compaction_snapshot" || {
        echo "compaction_snapshot doesn't have expected row count after filtered restore"
        exit 1
    }
    
    # Test 3: Restore only log table with filter
    run_sql "drop schema if exists $DB;"
    run_br --pd "$PD_ADDR" restore point -s "local://$TEST_DIR/$TASK_NAME/log" \
        --full-backup-storage "local://$TEST_DIR/$TASK_NAME/full" \
        -f "$DB.compaction_log"
    
    # Verify only log table exists
    verify_no_unexpected_tables 1 "$DB" || {
        echo "Wrong number of tables restored with compaction_log filter"
        exit 1
    }
    
    # Verify data is correct
    run_sql "SELECT COUNT(*) = 3 FROM $DB.compaction_log" || {
        echo "compaction_log doesn't have expected row count after filtered restore"
        exit 1
    }
    
    rm -rf "$TEST_DIR/$TASK_NAME"
    
    echo "log compaction with filter test passed"
}

test_pitr_chaining() {
    restart_services || { echo "Failed to restart services"; exit 1; }

    echo "case 21: start testing PITR chaining (sequential restores without cleaning up)"

    run_sql "create schema $DB;"

    echo "creating tables for initial state..."
    run_sql "CREATE TABLE $DB.table_a (
        id INT PRIMARY KEY,
        value VARCHAR(50)
    );"
    run_sql "CREATE TABLE $DB.table_b (
        id INT PRIMARY KEY,
        value VARCHAR(50)
    );"

    run_br --pd $PD_ADDR log start --task-name $TASK_NAME -s "local://$TEST_DIR/$TASK_NAME/log"

    run_sql "INSERT INTO $DB.table_a VALUES (1, 'initial data 1'), (2, 'initial data 2');"
    run_sql "INSERT INTO $DB.table_b VALUES (1, 'initial data 1'), (2, 'initial data 2');"
    
    run_br backup full -s "local://$TEST_DIR/$TASK_NAME/full" --pd $PD_ADDR

    run_sql "INSERT INTO $DB.table_a VALUES (3, 'post-backup data 1');"
    run_sql "INSERT INTO $DB.table_b VALUES (3, 'post-backup data 1');"
    
    . "$CUR/../br_test_utils.sh" && wait_log_checkpoint_advance "$TASK_NAME"
    first_restore_ts=$(python3 -c "import time; print(int(time.time() * 1000) << 18)")
    echo "Captured first checkpoint timestamp: $first_restore_ts"
    sleep 5
    
    run_sql "INSERT INTO $DB.table_a VALUES (4, 'post-first-checkpoint data');"
    run_sql "INSERT INTO $DB.table_b VALUES (4, 'post-first-checkpoint data');"
    
    run_sql "CREATE TABLE $DB.table_c (
        id INT PRIMARY KEY,
        value VARCHAR(50)
    );"
    run_sql "INSERT INTO $DB.table_c VALUES (1, 'created after first checkpoint');"
    
    . "$CUR/../br_test_utils.sh" && wait_log_checkpoint_advance "$TASK_NAME"

    run_br --pd $PD_ADDR log stop --task-name $TASK_NAME
    
    run_sql "drop schema if exists $DB;"
    
    echo "Step 1: First restore with full backup to first checkpoint timestamp"
    run_br --pd "$PD_ADDR" restore point -s "local://$TEST_DIR/$TASK_NAME/log" \
        --full-backup-storage "local://$TEST_DIR/$TASK_NAME/full" \
        --restored-ts $first_restore_ts \
        -f "$DB.*"
    
    run_sql "SELECT COUNT(*) = 3 FROM $DB.table_a" || {
        echo "table_a doesn't have expected row count after first restore"
        exit 1
    }
    
    run_sql "SELECT COUNT(*) = 3 FROM $DB.table_b" || {
        echo "table_b doesn't have expected row count after first restore"
        exit 1
    }
    
    if run_sql "SELECT * FROM $DB.table_c" 2>/dev/null; then
        echo "table_c exists after first restore but shouldn't"
        exit 1
    fi
    
    echo "Step 2: Second restore with log only using first checkpoint timestamp as startTS"
    run_br --pd "$PD_ADDR" restore point -s "local://$TEST_DIR/$TASK_NAME/log" \
        --start-ts $first_restore_ts \
        -f "$DB.*"
    
    # Verify data after second restore
    run_sql "SELECT COUNT(*) = 4 FROM $DB.table_a" || {
        echo "table_a doesn't have expected row count after second restore"
        exit 1
    }
    
    run_sql "SELECT COUNT(*) = 4 FROM $DB.table_b" || {
        echo "table_b doesn't have expected row count after second restore"
        exit 1
    }
    
    run_sql "SELECT COUNT(*) = 1 FROM $DB.table_c" || {
        echo "table_c doesn't have expected row count after second restore"
        exit 1
    }

    # make sure able to write data after restore
    run_sql "INSERT INTO $DB.table_a VALUES (5, 'post-second-checkpoint data');"
    run_sql "CREATE TABLE $DB.table_d (
        id INT PRIMARY KEY,
        value VARCHAR(50)
    );"
    run_sql "INSERT INTO $DB.table_d VALUES (1, 'created after second checkpoint');"

    verify_no_unexpected_tables 4 "$DB" || {
        echo "Wrong number of tables after all restores"
        exit 1
    }

    run_sql "drop schema if exists $DB;"
    rm -rf "$TEST_DIR/$TASK_NAME"
    
    echo "PITR sequential restore test passed"
}

test_cover_all_ddl
test_basic_filter
test_with_full_backup_filter
test_table_rename
test_with_checkpoint
test_partition_exchange
test_system_tables
test_foreign_keys
test_index_filter
test_table_truncation
test_sequential_restore
test_log_compaction
test_pitr_chaining

echo "br pitr table filter all tests passed"
