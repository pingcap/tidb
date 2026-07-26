#!/usr/bin/env bash
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

PACKED_COMMON_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
PACKED_TIKV_CONFIG="$PACKED_COMMON_DIR/../config/tikv.toml"
PACKED_WORKLOAD="$PACKED_COMMON_DIR/../br_full/workload"
PACKED_PACKAGE_FILE_SIZE=1099511627776
PACKED_DB="$TEST_NAME"
PACKED_TABLE="usertable"

PACKED_BACKUP_LABELS=(direct packed)
PACKED_BACKUP_COUNTS=()
PACKED_SST_COUNTS=()
PACKED_PACK_COUNTS=()
PACKED_RESTORE_SECONDS=()

packed_restart_cluster() {
    export TIKV_CONFIG="$PACKED_TIKV_CONFIG"
    stop_services
    start_services --no-tiflash
}

packed_prepare_data() {
    echo "Preparing packed backup data in database $PACKED_DB"
    run_sql "DROP DATABASE IF EXISTS $PACKED_DB;"
    run_sql "CREATE DATABASE $PACKED_DB;"
    go-ycsb load mysql -P "$PACKED_WORKLOAD" \
        -p mysql.host="$TIDB_IP" \
        -p mysql.port="$TIDB_PORT" \
        -p mysql.user=root \
        -p mysql.db="$PACKED_DB"
    PACKED_ORIGINAL_COUNT=$(run_sql "SELECT COUNT(*) FROM $PACKED_DB.$PACKED_TABLE;" | awk '/COUNT/{print $2}')
    export PACKED_ORIGINAL_COUNT
    echo "Original row count: $PACKED_ORIGINAL_COUNT"
}

packed_run_backup() {
    local label=$1
    local storage=$2
    local extra_args=${3:-}

    echo "Run $label backup to $storage"
    # shellcheck disable=SC2086
    run_br --pd "$PD_ADDR" backup full -s "$storage" $extra_args
}

packed_record_backup_counts() {
    local label=$1

    PACKED_BACKUP_COUNTS+=("$(packed_count_data_objects "$label")")
    PACKED_SST_COUNTS+=("$(packed_count_sst_objects "$label")")
    PACKED_PACK_COUNTS+=("$(packed_count_pack_objects "$label")")
}

packed_restore_and_time() {
    local label=$1
    local storage=$2

    echo "Restore $label backup from $storage into a fresh cluster"
    packed_restart_cluster
    packed_backend_after_cluster_restart

    local start_time
    local end_time
    local elapsed
    start_time=$(date +%s)
    run_br restore full -s "$storage" --pd "$PD_ADDR"
    end_time=$(date +%s)
    elapsed=$((end_time - start_time))

    local restored_count
    restored_count=$(run_sql "SELECT COUNT(*) FROM $PACKED_DB.$PACKED_TABLE;" | awk '/COUNT/{print $2}')
    echo "$label restore row count: $restored_count, elapsed: ${elapsed}s"
    if [ "$restored_count" != "$PACKED_ORIGINAL_COUNT" ]; then
        echo "TEST: [$TEST_NAME] $label restore row count mismatch, expected $PACKED_ORIGINAL_COUNT, got $restored_count"
        exit 1
    fi

    PACKED_RESTORE_SECONDS+=("$elapsed")
}

packed_assert_file_counts() {
    local direct=${PACKED_BACKUP_COUNTS[0]}
    local packed=${PACKED_BACKUP_COUNTS[1]}

    local direct_sst=${PACKED_SST_COUNTS[0]}
    local direct_pack=${PACKED_PACK_COUNTS[0]}
    local packed_sst=${PACKED_SST_COUNTS[1]}
    local packed_pack=${PACKED_PACK_COUNTS[1]}

    echo "Packed backup data object counts: direct=$direct, packed=$packed"
    echo "Packed backup layout: direct=${direct_sst}.sst/${direct_pack}.pack, packed=${packed_sst}.sst/${packed_pack}.pack"

    if [ "$direct_sst" -le 0 ] || [ "$direct_pack" -ne 0 ]; then
        echo "TEST: [$TEST_NAME] expected direct backup to produce SST files only"
        exit 1
    fi

    if [ "$packed_pack" -le 0 ]; then
        echo "TEST: [$TEST_NAME] expected packed backup to produce pack files"
        exit 1
    fi

    if [ "$packed_sst" -ne 0 ]; then
        echo "TEST: [$TEST_NAME] packed backup unexpectedly produced direct SST files"
        exit 1
    fi

    if [ "${PACKED_SKIP_FILE_COUNT_ASSERT:-false}" = "true" ]; then
        echo "Skip packed backup data object count assertion"
        return
    fi

    if [ "$direct" -le "$packed" ]; then
        echo "TEST: [$TEST_NAME] expected data object counts direct > packed"
        exit 1
    fi
}

packed_print_summary() {
    echo "===== packed backup summary [$TEST_NAME] ====="
    printf "%-16s %-12s %-12s %-12s %s\n" "backup" "objects" "sst/pack" "restore(s)" "storage"
    local i=0
    while [ "$i" -lt "${#PACKED_BACKUP_LABELS[@]}" ]; do
        local label=${PACKED_BACKUP_LABELS[$i]}
        printf "%-16s %-12s %-12s %-12s %s\n" \
            "$label" \
            "${PACKED_BACKUP_COUNTS[$i]}" \
            "${PACKED_SST_COUNTS[$i]}/${PACKED_PACK_COUNTS[$i]}" \
            "${PACKED_RESTORE_SECONDS[$i]}" \
            "$(packed_storage_uri "$label")"
        i=$((i + 1))
    done
}

run_packed_test() {
    local backend=$1
    PACKED_BACKEND=$backend
    export PACKED_BACKEND

    . run_services

    packed_backend_init
    packed_restart_cluster
    packed_backend_after_cluster_restart
    packed_prepare_data

    packed_run_backup direct "$(packed_storage_uri direct)" ""
    packed_record_backup_counts direct

    packed_run_backup packed "$(packed_storage_uri packed)" "--package-file-size $PACKED_PACKAGE_FILE_SIZE"
    packed_record_backup_counts packed

    packed_assert_file_counts

    local label
    for label in "${PACKED_BACKUP_LABELS[@]}"; do
        packed_restore_and_time "$label" "$(packed_storage_uri "$label")"
    done

    packed_print_summary
    echo "TEST: [$TEST_NAME] success!"
}
