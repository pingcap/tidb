#!/usr/bin/env bash
#
# Copyright 2026 PingCAP, Inc. Licensed under Apache-2.0.

set -euo pipefail

CUR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
. "$CUR/../br_packed_common/common.sh"

packed_backend_init() {
    PACKED_LOCAL_ROOT="$TEST_DIR/packed_local"
    rm -rf "$PACKED_LOCAL_ROOT"
    mkdir -p "$PACKED_LOCAL_ROOT"
    export PACKED_LOCAL_ROOT
}

packed_backend_after_cluster_restart() {
    :
}

packed_storage_uri() {
    local label=$1
    echo "local://$PACKED_LOCAL_ROOT/$label"
}

packed_count_data_objects() {
    local label=$1
    find "$PACKED_LOCAL_ROOT/$label" -type f \( -name '*.sst' -o -name '*.pack' \) | wc -l | awk '{print $1}'
}

packed_count_sst_objects() {
    local label=$1
    find "$PACKED_LOCAL_ROOT/$label" -type f -name '*.sst' | wc -l | awk '{print $1}'
}

packed_count_pack_objects() {
    local label=$1
    find "$PACKED_LOCAL_ROOT/$label" -type f -name '*.pack' | wc -l | awk '{print $1}'
}

run_packed_test local
