#!/bin/bash
#
# Copyright 2026 PingCAP, Inc.
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
. run_services
CUR=$(cd "$(dirname "${BASH_SOURCE[0]}")"; pwd)

export ENCRYPTION_ARGS=""
export ENABLE_ENCRYPTION_CHECK=false

PREFIX="lease_lock"
CASE_ROOT="$TEST_DIR/lease_lock"
LOG_DIR="$CASE_ROOT/logs"
MARKER_DIR="$CASE_ROOT/markers"
LEASE_FP='github.com/pingcap/tidb/pkg/objstore/lease-lock-test-constants=return("ttl=2s,interval=200ms,write-timeout-cap=100ms,min-remaining=200ms,stale-reclaim-grace=500ms,base-backoff=100ms")'

background_pids=()

cleanup_background_pids() {
    local pid
    for pid in "${background_pids[@]:-}"; do
        if kill -0 "$pid" 2>/dev/null; then
            kill_process_tree "$pid"
        fi
    done
    export GO_FAILPOINTS=""
}

trap cleanup_background_pids EXIT

case_task_name() {
    echo "${PREFIX}_$1"
}

reset_case_dir() {
    local case_name=$1
    local case_dir="$CASE_ROOT/$case_name"
    rm -rf "$case_dir"
    mkdir -p "$case_dir/storage" "$case_dir/markers" "$case_dir/logs" "$LOG_DIR" "$MARKER_DIR"
    echo "$case_dir"
}

wait_for_file() {
    local file=$1
    local timeout_seconds=$2
    local waited=0
    while [ "$waited" -lt "$timeout_seconds" ]; do
        if [ -f "$file" ]; then
            return 0
        fi
        sleep 1
        waited=$((waited + 1))
    done
    echo "Timed out waiting for file: $file"
    return 1
}

assert_file_exists() {
    local file=$1
    if [ ! -f "$file" ]; then
        echo "Expected file to exist: $file"
        exit 1
    fi
}

assert_file_not_exists() {
    local file=$1
    if [ -e "$file" ]; then
        echo "Expected file to be absent: $file"
        exit 1
    fi
}

find_single_lock_file() {
    local storage_dir=$1
    local kind=$2
    local regex
    case "$kind" in
        migration-read)
            regex='/v1/LOCK\.READ\.[[:xdigit:]]{32}$'
            ;;
        truncate)
            regex='/truncating\.lock\.[[:xdigit:]]{32}$'
            ;;
        *)
            echo "Unknown lock kind: $kind"
            exit 1
            ;;
    esac

    local matches
    matches=$(find "$storage_dir" -type f | grep -E "$regex" || true)
    local count
    count=$(echo "$matches" | sed '/^$/d' | wc -l)
    if [ "$count" -ne 1 ]; then
        echo "Expected exactly one $kind lock under $storage_dir, got $count"
        echo "$matches"
        exit 1
    fi
    echo "$matches"
}

read_expire_at_epoch_ns() {
    local lock_file=$1
    python3 - "$lock_file" <<'PY'
import datetime
import json
import sys

with open(sys.argv[1], encoding="utf-8") as f:
    meta = json.load(f)
expire_at = meta["expire_at"]
if expire_at.endswith("Z"):
    expire_at = expire_at[:-1] + "+00:00"
dt = datetime.datetime.fromisoformat(expire_at)
print(int(dt.timestamp() * 1_000_000_000))
PY
}

read_lock_txn_id() {
    local lock_file=$1
    python3 - "$lock_file" <<'PY'
import json
import sys

with open(sys.argv[1], encoding="utf-8") as f:
    print(json.load(f)["txn_id"])
PY
}

wait_expire_at_advanced() {
    local lock_file=$1
    local old_ns=$2
    local waited=0
    while [ "$waited" -lt 20 ]; do
        local new_ns
        new_ns=$(read_expire_at_epoch_ns "$lock_file")
        if [ "$new_ns" -gt "$old_ns" ]; then
            echo "$new_ns"
            return 0
        fi
        sleep 1
        waited=$((waited + 1))
    done
    echo "Timed out waiting for ExpireAt to advance in $lock_file"
    return 1
}

count_pd_clock_markers() {
    local marker_dir=$1
    find "$marker_dir" -maxdepth 1 -type f -name 'now.*' 2>/dev/null | wc -l
}

wait_pd_clock_marker_count_gt() {
    local marker_dir=$1
    local old_count=$2
    local waited=0
    while [ "$waited" -lt 20 ]; do
        local new_count
        new_count=$(count_pd_clock_markers "$marker_dir")
        if [ "$new_count" -gt "$old_count" ]; then
            echo "$new_count"
            return 0
        fi
        sleep 1
        waited=$((waited + 1))
    done
    echo "Timed out waiting for PD clock markers in $marker_dir to exceed $old_count"
    return 1
}

assert_log_contains() {
    local log_file=$1
    local pattern=$2
    if ! grep -Fq "$pattern" "$log_file"; then
        echo "Expected log $log_file to contain: $pattern"
        cat "$log_file"
        exit 1
    fi
}

assert_log_not_contains() {
    local log_file=$1
    local pattern=$2
    if grep -Fq "$pattern" "$log_file"; then
        echo "Expected log $log_file not to contain: $pattern"
        cat "$log_file"
        exit 1
    fi
}

run_br_capture() {
    local log_file=$1
    shift
    mkdir -p "$(dirname "$log_file")"
    run_br "$@" > "$log_file" 2>&1
}

run_br_with_failpoints() {
    local failpoints=$1
    local log_file=$2
    shift 2
    mkdir -p "$(dirname "$log_file")"
    GO_FAILPOINTS="$failpoints" run_br "$@" > "$log_file" 2>&1
}

run_br_bg_with_failpoints() {
    local failpoints=$1
    local log_file=$2
    shift 2
    mkdir -p "$(dirname "$log_file")"
    (
        export GO_FAILPOINTS="$failpoints"
        run_br "$@"
    ) > "$log_file" 2>&1 &
    BR_BG_PID=$!
    background_pids+=("$BR_BG_PID")
}

wait_pid_with_timeout() {
    local pid=$1
    local timeout_seconds=$2
    local expected_exit=$3
    local log_file=${4:-}
    local waited=0

    while kill -0 "$pid" 2>/dev/null; do
        if [ "$waited" -ge "$timeout_seconds" ]; then
            echo "Timed out waiting for pid $pid"
            if [ -n "$log_file" ] && [ -f "$log_file" ]; then
                cat "$log_file"
            fi
            kill_process_tree "$pid"
            exit 1
        fi
        sleep 1
        waited=$((waited + 1))
    done

    local status=0
    wait "$pid" || status=$?
    case "$expected_exit" in
        success)
            if [ "$status" -ne 0 ]; then
                echo "Expected pid $pid to succeed, got exit $status"
                [ -z "$log_file" ] || cat "$log_file"
                exit 1
            fi
            ;;
        failure)
            if [ "$status" -eq 0 ]; then
                echo "Expected pid $pid to fail, but it succeeded"
                [ -z "$log_file" ] || cat "$log_file"
                exit 1
            fi
            ;;
        *)
            echo "Unknown expected exit mode: $expected_exit"
            exit 1
            ;;
    esac
}

kill_process_tree() {
    local pid=$1
    local child
    for child in $(pgrep -P "$pid" 2>/dev/null || true); do
        kill_process_tree "$child"
    done
    kill "$pid" 2>/dev/null || true
    sleep 1
    kill -9 "$pid" 2>/dev/null || true
}

prepare_pitr_fixture() {
    local case_name=$1
    CASE_DIR=$(reset_case_dir "$case_name")
    CASE_MARKER_DIR="$CASE_DIR/markers"
    CASE_LOG_DIR="$CASE_DIR/logs"
    FULL_STORAGE="$CASE_DIR/storage/full"
    LOG_STORAGE="$CASE_DIR/storage/log"
    DB="${PREFIX}_${case_name}"
    TASK_NAME=$(case_task_name "$case_name")
    EXPECTED_RESTORE_SUM=233

    echo "Preparing PITR fixture for $case_name"
    restart_services
    run_sql "DROP DATABASE IF EXISTS $DB;"
    run_sql "CREATE DATABASE $DB;"
    run_sql "CREATE TABLE $DB.t (id INT PRIMARY KEY);"
    run_sql "INSERT INTO $DB.t VALUES (1), (10), (100);"

    run_br_capture "$CASE_LOG_DIR/log-start.log" \
        --pd "$PD_ADDR" log start --task-name "$TASK_NAME" -s "local://$LOG_STORAGE"
    run_br_capture "$CASE_LOG_DIR/backup-full.log" \
        --pd "$PD_ADDR" backup full -s "local://$FULL_STORAGE"

    run_sql "INSERT INTO $DB.t VALUES (11), (111);"
    sleep 5
    RESTORED_TS=$(python3 -c "import time; print(int(time.time() * 1000) << 18)")

    run_sql "CREATE TABLE $DB.truncate_probe (id INT PRIMARY KEY);"
    run_sql "INSERT INTO $DB.truncate_probe VALUES (7), (70);"
    sleep 5
    TRUNCATE_TS=$(python3 -c "import time; print(int(time.time() * 1000) << 18)")

    . "$CUR/../br_test_utils.sh" && wait_log_checkpoint_advance "$TASK_NAME"
    run_br_capture "$CASE_LOG_DIR/log-stop.log" \
        --pd "$PD_ADDR" log stop --task-name "$TASK_NAME"
}

run_migration_renewal_success_case() {
    local case_name="migration_renewal_success"
    prepare_pitr_fixture "$case_name"
    restart_services

    local acquired="$CASE_MARKER_DIR/acquired"
    local release="$CASE_MARKER_DIR/release"
    local after="$CASE_MARKER_DIR/after"
    local pd_clock_dir="$CASE_MARKER_DIR/pd-clock"
    local restore_log="$CASE_LOG_DIR/restore.log"
    local failpoints="$LEASE_FP;github.com/pingcap/tidb/br/pkg/restore/lease-clock-pd-now-signal=return(\"dir=$pd_clock_dir\");github.com/pingcap/tidb/br/pkg/restore/log_client/lease-lock-after-migration-lock-acquired=return(\"signal=$acquired,release=$release,after=$after\")"

    run_br_bg_with_failpoints "$failpoints" "$restore_log" \
        --pd "$PD_ADDR" restore point \
        -s "local://$LOG_STORAGE" \
        --full-backup-storage "local://$FULL_STORAGE" \
        --restored-ts "$RESTORED_TS" \
        -f "$DB.*"
    local pid=$BR_BG_PID

    wait_for_file "$acquired" 30
    local lock_file
    lock_file=$(find_single_lock_file "$LOG_STORAGE" migration-read)
    read_lock_txn_id "$lock_file" > "$CASE_MARKER_DIR/lock-txn-id"

    local old_expire_at
    old_expire_at=$(read_expire_at_epoch_ns "$lock_file")
    local old_pd_markers
    old_pd_markers=$(count_pd_clock_markers "$pd_clock_dir")

    wait_expire_at_advanced "$lock_file" "$old_expire_at" > "$CASE_MARKER_DIR/renewed-expire-at"
    wait_pd_clock_marker_count_gt "$pd_clock_dir" "$old_pd_markers" > "$CASE_MARKER_DIR/pd-clock-count"

    touch "$release"
    wait_pid_with_timeout "$pid" 120 success "$restore_log"

    assert_file_exists "$after"
    run_sql "SELECT SUM(id) AS SUM FROM $DB.t;"
    check_contains "SUM: $EXPECTED_RESTORE_SUM"
    assert_log_not_contains "$restore_log" "lease lost"
    assert_log_not_contains "$restore_log" "context canceled"
    assert_file_not_exists "$lock_file"
}

run_migration_lost_case() {
    local mode=$1
    local case_name="migration_lost_${mode}"
    prepare_pitr_fixture "$case_name"
    restart_services

    local acquired="$CASE_MARKER_DIR/acquired"
    local release="$CASE_MARKER_DIR/release"
    local after="$CASE_MARKER_DIR/after"
    local fault="$CASE_MARKER_DIR/renewal-write-blocked"
    local fault_dir="$CASE_MARKER_DIR/renewal-write-errors"
    local restore_log="$CASE_LOG_DIR/restore-$mode.log"
    local failpoints="$LEASE_FP;github.com/pingcap/tidb/br/pkg/restore/log_client/lease-lock-after-migration-lock-acquired=return(\"signal=$acquired,release=$release,after=$after\")"

    case "$mode" in
        block)
            failpoints="$failpoints;github.com/pingcap/tidb/pkg/objstore/lease-lock-renewal-write-block=return(\"signal=$fault\")"
            ;;
        error)
            failpoints="$failpoints;github.com/pingcap/tidb/pkg/objstore/lease-lock-renewal-write-error=return(\"signal-dir=$fault_dir\")"
            ;;
        *)
            echo "Unknown migration lost mode: $mode"
            exit 1
            ;;
    esac

    run_br_bg_with_failpoints "$failpoints" "$restore_log" \
        --pd "$PD_ADDR" restore point \
        -s "local://$LOG_STORAGE" \
        --full-backup-storage "local://$FULL_STORAGE" \
        --restored-ts "$RESTORED_TS" \
        -f "$DB.*"
    local pid=$BR_BG_PID

    wait_for_file "$acquired" 30
    case "$mode" in
        block)
            wait_for_file "$fault" 30
            ;;
        error)
            wait_for_file "$fault_dir/attempt.1" 30
            wait_for_file "$fault_dir/attempt.2" 30
            ;;
    esac

    wait_pid_with_timeout "$pid" 120 failure "$restore_log"
    assert_file_not_exists "$release"
    assert_file_not_exists "$after"
    assert_log_contains "$restore_log" "context canceled"
    run_sql "SELECT COUNT(*) FROM information_schema.schemata WHERE schema_name = '$DB';"
    check_contains "COUNT(*): 0"
}

list_cases() {
    cat <<'CASES'
planned lease lock integration cases:
  migration renewal success
  migration lost by renewal-write-block
  migration lost by renewal-write-error
  truncate renewal success
  truncate lost by renewal-write-block
  truncate lost by renewal-write-error
  stale lock reclaim allows real command
CASES
}

restart_services
mkdir -p "$CASE_ROOT" "$LOG_DIR" "$MARKER_DIR"
list_cases
