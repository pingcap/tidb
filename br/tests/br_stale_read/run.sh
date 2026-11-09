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

set -euo pipefail

. run_services

CUR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
DB_NAME="$TEST_NAME"
TABLE="usertable"
INITIAL_RECORD_COUNT=1000
ADDITIONAL_RECORD_COUNT=1000
ADDITIONAL_INSERT_START=1000000
TOTAL_RECORD_COUNT=$((ADDITIONAL_INSERT_START + ADDITIONAL_RECORD_COUNT))

BACKUP_ROOT="$TEST_DIR/keep_${TEST_NAME}"
NORMAL_BACKUP_ROUND1="$BACKUP_ROOT/normal_round1"
STALE_BACKUP_ROUND1="$BACKUP_ROOT/stale_round1"
NORMAL_BACKUP_ROUND2="$BACKUP_ROOT/normal_round2"
STALE_BACKUP_ROUND2="$BACKUP_ROOT/stale_round2"

NORMAL_RESTORE_CHECKSUM_FILE="$TEST_DIR/normal_round1.checksum"
STALE_RESTORE_CHECKSUM_FILE="$TEST_DIR/stale_round1.checksum"
NORMAL_ROUND2_RESTORE_CHECKSUM_FILE="$TEST_DIR/normal_round2.checksum"
STALE_ROUND2_RESTORE_CHECKSUM_FILE="$TEST_DIR/stale_round2.checksum"

mysql_query() {
    mysql -uroot -h"$TIDB_IP" -P"$TIDB_PORT" \
        --ssl-ca="$TEST_DIR/certs/ca.pem" \
        --ssl-cert="$TEST_DIR/certs/curl.pem" \
        --ssl-key="$TEST_DIR/certs/curl.key" \
        --batch --raw --skip-column-names \
        -e "$1"
}

disable_gc() {
    echo "TEST: [$TEST_NAME] disable GC before fixing backup ts"
    run_sql "SET GLOBAL tidb_gc_enable = OFF;"
}

print_section() {
    local title=$1
    echo "=========================================="
    echo "TEST: [$TEST_NAME] $title"
    echo "=========================================="
}

print_step() {
    local message=$1
    echo ">>> $message"
}

restart_services_without_tiflash() {
    echo "TEST: [$TEST_NAME] restarting services without TiFlash"
    stop_services
    start_services --no-tiflash
}

print_state_file() {
    local label=$1
    local file=$2
    echo "TEST: [$TEST_NAME] ${label}:"
    sed 's/^/  /' "$file"
}

query_row_count() {
    mysql_query "SELECT COUNT(*) FROM \`${DB_NAME}\`.\`${TABLE}\`;" | awk 'NR == 1 { print $1; exit }'
}

list_checksum_tables() {
    mysql_query "SELECT TABLE_NAME FROM \`information_schema\`.\`tables\` WHERE TABLE_SCHEMA = '${DB_NAME}' AND TABLE_TYPE = 'BASE TABLE' ORDER BY TABLE_NAME;"
}

capture_checksum() {
    local output=$1
    local table_name
    local found_table=false

    : > "$output"
    while IFS= read -r table_name; do
        [ -n "$table_name" ] || continue
        found_table=true
        mysql_query "ADMIN CHECKSUM TABLE \`${DB_NAME}\`.\`${table_name}\`;" | \
            awk 'NR == 1 { printf "%s.%s|checksum=%s|total_kvs=%s|total_bytes=%s\n", $1, $2, $3, $4, $5 }' >> "$output"
    done < <(list_checksum_tables)

    if [ "$found_table" = false ]; then
        echo "TEST: [$TEST_NAME] no tables found in ${DB_NAME} for checksum capture"
        exit 1
    fi
}

get_fixed_backup_ts() {
    mysql_query "BEGIN; SELECT COUNT(*) FROM \`${DB_NAME}\`.\`${TABLE}\`; SELECT CONCAT('fixed_ts=', @@tidb_current_ts); ROLLBACK;" | \
        awk -F= '/^fixed_ts=[1-9][0-9]*$/ { print $2; exit }'
}

compare_restored_checksum_pair() {
    local left_label=$1
    local left_file=$2
    local right_label=$3
    local right_file=$4

    echo "TEST: [$TEST_NAME] comparing restored checksum: ${left_label} vs ${right_label}"
    if ! diff -u "$left_file" "$right_file"; then
        print_state_file "${left_label} restored checksum" "$left_file"
        print_state_file "${right_label} restored checksum" "$right_file"
        echo "TEST: [$TEST_NAME] restored checksum mismatch between ${left_label} and ${right_label}"
        exit 1
    fi

    echo "TEST: [$TEST_NAME] restored checksum matches: ${left_label} == ${right_label}"
}

print_all_restored_checksums() {
    echo "TEST: [$TEST_NAME] restored checksum summary:"
    print_state_file "normal_round1 restored checksum" "$NORMAL_RESTORE_CHECKSUM_FILE"
    print_state_file "stale_round1 restored checksum" "$STALE_RESTORE_CHECKSUM_FILE"
    print_state_file "normal_round2 restored checksum" "$NORMAL_ROUND2_RESTORE_CHECKSUM_FILE"
    print_state_file "stale_round2 restored checksum" "$STALE_ROUND2_RESTORE_CHECKSUM_FILE"
}

load_initial_data() {
    run_sql "CREATE DATABASE \`${DB_NAME}\`;"
    go-ycsb load mysql -P "$CUR/workload" \
        -p mysql.host="$TIDB_IP" \
        -p mysql.port="$TIDB_PORT" \
        -p mysql.user=root \
        -p mysql.db="$DB_NAME"

    local row_count
    row_count=$(query_row_count)
    if [ "$row_count" != "$INITIAL_RECORD_COUNT" ]; then
        echo "TEST: [$TEST_NAME] unexpected initial row count for ${DB_NAME}: ${row_count}"
        exit 1
    fi
}

append_additional_data() {
    go-ycsb load mysql -P "$CUR/workload" \
        -p mysql.host="$TIDB_IP" \
        -p mysql.port="$TIDB_PORT" \
        -p mysql.user=root \
        -p mysql.db="$DB_NAME" \
        -p insertcount="$ADDITIONAL_RECORD_COUNT" \
        -p insertstart="$ADDITIONAL_INSERT_START" \
        -p recordcount="$TOTAL_RECORD_COUNT" \
        -p workload=core

    local row_count
    row_count=$(query_row_count)
    if [ "$row_count" != "$((INITIAL_RECORD_COUNT + ADDITIONAL_RECORD_COUNT))" ]; then
        echo "TEST: [$TEST_NAME] failed to append data for ${DB_NAME}: ${row_count}"
        exit 1
    fi

    echo "TEST: [$TEST_NAME] live row count after additional writes: ${row_count}"
}

decode_backup_end_version() {
    local backup_dir=$1
    local decode_output
    decode_output=$(run_br validate decode --field="end-version" -s "local://$backup_dir" 2>/dev/null)
    printf '%s\n' "$decode_output" | awk 'match($0, /^[0-9]+/) { print substr($0, RSTART, RLENGTH); exit }'
}

run_full_backup_at_ts() {
    local label=$1
    local backup_dir=$2
    local mode=$3
    local -a args=(--pd "$PD_ADDR" backup full -s "local://$backup_dir" --backupts "$BACKUP_TS")

    if [ "$mode" = "stale" ]; then
        args+=(--stale-read)
    fi

    echo "TEST: [$TEST_NAME] running ${label} backup at fixed ts ${BACKUP_TS}"
    run_br "${args[@]}"

    local decoded_ts
    decoded_ts=$(decode_backup_end_version "$backup_dir")
    echo "TEST: [$TEST_NAME] ${label} backup metadata end-version: ${decoded_ts}"
    if [ "$decoded_ts" != "$BACKUP_TS" ]; then
        echo "TEST: [$TEST_NAME] unexpected end-version for ${label}: ${decoded_ts}, expected ${BACKUP_TS}"
        exit 1
    fi
}

restore_and_capture_checksum() {
    local label=$1
    local backup_dir=$2
    local checksum_file=$3

    restart_services_without_tiflash
    echo "TEST: [$TEST_NAME] restoring ${label} from ${backup_dir}"
    run_br restore full -s "local://$backup_dir" --pd "$PD_ADDR"
    capture_checksum "$checksum_file"
    print_state_file "${label} restored checksum" "$checksum_file"
}

rm -rf "$BACKUP_ROOT"
mkdir -p "$BACKUP_ROOT"

print_section "BR stale-read fixed-ts verification"
disable_gc
print_step "Loading baseline go-ycsb data"
load_initial_data

BACKUP_TS=$(get_fixed_backup_ts)
echo "TEST: [$TEST_NAME] chosen fixed backup ts after initial load: ${BACKUP_TS}"
if ! [[ "$BACKUP_TS" =~ ^[1-9][0-9]*$ ]]; then
    echo "TEST: [$TEST_NAME] failed to obtain a fixed backup ts after initial load"
    exit 1
fi

print_step "Running two fixed-ts backups before additional writes"
run_full_backup_at_ts "normal_round1" "$NORMAL_BACKUP_ROUND1" "normal"
run_full_backup_at_ts "stale_round1" "$STALE_BACKUP_ROUND1" "stale"

print_step "Appending more go-ycsb data after fixed backup ts"
append_additional_data

# All fixed-ts backups must be taken before the first restart. Once restart_services
# recreates the cluster, the original backup TS would no longer refer to the same MVCC history.
print_step "Running two more backups with the same fixed ts after additional writes"
run_full_backup_at_ts "normal_round2" "$NORMAL_BACKUP_ROUND2" "normal"
run_full_backup_at_ts "stale_round2" "$STALE_BACKUP_ROUND2" "stale"

print_section "Restoring each backup and collecting checksums"
restore_and_capture_checksum "normal_round1" "$NORMAL_BACKUP_ROUND1" "$NORMAL_RESTORE_CHECKSUM_FILE"
restore_and_capture_checksum "stale_round1" "$STALE_BACKUP_ROUND1" "$STALE_RESTORE_CHECKSUM_FILE"
restore_and_capture_checksum "normal_round2" "$NORMAL_BACKUP_ROUND2" "$NORMAL_ROUND2_RESTORE_CHECKSUM_FILE"
restore_and_capture_checksum "stale_round2" "$STALE_BACKUP_ROUND2" "$STALE_ROUND2_RESTORE_CHECKSUM_FILE"

print_section "Restored checksum summary"
print_all_restored_checksums

print_section "Restored checksum verification"
compare_restored_checksum_pair "normal_round1" "$NORMAL_RESTORE_CHECKSUM_FILE" "stale_round1" "$STALE_RESTORE_CHECKSUM_FILE"
compare_restored_checksum_pair "normal_round1" "$NORMAL_RESTORE_CHECKSUM_FILE" "normal_round2" "$NORMAL_ROUND2_RESTORE_CHECKSUM_FILE"
compare_restored_checksum_pair "normal_round2" "$NORMAL_ROUND2_RESTORE_CHECKSUM_FILE" "stale_round2" "$STALE_ROUND2_RESTORE_CHECKSUM_FILE"
compare_restored_checksum_pair "stale_round1" "$STALE_RESTORE_CHECKSUM_FILE" "stale_round2" "$STALE_ROUND2_RESTORE_CHECKSUM_FILE"

print_section "All expected checksum comparisons passed"
echo "TEST: [$TEST_NAME] restored checksum verification passed for all four expected comparisons"
rm -rf "$BACKUP_ROOT"
