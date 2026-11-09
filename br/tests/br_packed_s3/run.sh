#!/usr/bin/env bash
#
# Copyright 2026 PingCAP, Inc. Licensed under Apache-2.0.

set -euo pipefail

CUR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
. "$CUR/../br_packed_common/common.sh"

packed_backend_init() {
    export MINIO_ACCESS_KEY='brpackedaccesskey'
    export MINIO_SECRET_KEY='brpackedsecretkey'
    export MINIO_ROOT_USER=$MINIO_ACCESS_KEY
    export MINIO_ROOT_PASSWORD=$MINIO_SECRET_KEY
    export MINIO_BROWSER=off
    export AWS_ACCESS_KEY_ID=$MINIO_ACCESS_KEY
    export AWS_SECRET_ACCESS_KEY=$MINIO_SECRET_KEY
    PACKED_S3_ENDPOINT=127.0.0.1:24927
    PACKED_S3_BUCKET=packed
    PACKED_S3_PREFIX="$TEST_NAME"
    PACKED_MINIO_DIR="$TEST_DIR/packed_s3_minio"
    PACKED_MC_CONFIG="$TEST_DIR/$TEST_NAME-mc"
    export PACKED_S3_ENDPOINT PACKED_S3_BUCKET PACKED_S3_PREFIX PACKED_MINIO_DIR PACKED_MC_CONFIG

    rm -rf "$PACKED_MINIO_DIR" "$PACKED_MC_CONFIG"
    mkdir -p "$PACKED_MINIO_DIR"
}

packed_start_minio() {
    if [ -n "${PACKED_MINIO_PID:-}" ] && kill -0 "$PACKED_MINIO_PID" 2>/dev/null; then
        return
    fi

    bin/minio server --address "$PACKED_S3_ENDPOINT" "$PACKED_MINIO_DIR" &
    PACKED_MINIO_PID=$!
    export PACKED_MINIO_PID

    local i=0
    local status
    status="$(curl -o /dev/null -s "http://$PACKED_S3_ENDPOINT/" -w '%{http_code}' || true)"
    while ! { [ "$status" -gt 0 ] && [ "$status" -lt 500 ]; }; do
        i=$((i + 1))
        if [ "$i" -gt 30 ]; then
            echo "Failed to start minio"
            exit 1
        fi
        sleep 2
        status="$(curl -o /dev/null -s "http://$PACKED_S3_ENDPOINT/" -w '%{http_code}' || true)"
    done

    if bin/mc config --help >/dev/null 2>&1; then
        bin/mc config --config-dir "$PACKED_MC_CONFIG" \
            host add minio "http://$PACKED_S3_ENDPOINT" "$MINIO_ACCESS_KEY" "$MINIO_SECRET_KEY"
    else
        bin/mc alias set --config-dir "$PACKED_MC_CONFIG" \
            minio "http://$PACKED_S3_ENDPOINT" "$MINIO_ACCESS_KEY" "$MINIO_SECRET_KEY"
    fi

    bin/mc mb --config-dir "$PACKED_MC_CONFIG" --ignore-existing "minio/$PACKED_S3_BUCKET"
}

packed_backend_after_cluster_restart() {
    packed_start_minio
}

packed_storage_uri() {
    local label=$1
    echo "s3://$PACKED_S3_BUCKET/$PACKED_S3_PREFIX/$label?endpoint=http://$PACKED_S3_ENDPOINT&force-path-style=true"
}

packed_count_data_objects() {
    local label=$1
    bin/mc ls --config-dir "$PACKED_MC_CONFIG" --recursive "minio/$PACKED_S3_BUCKET/$PACKED_S3_PREFIX/$label" \
        | awk '$NF ~ /\.(sst|pack)$/ { count++ } END { print count + 0 }'
}

packed_count_sst_objects() {
    local label=$1
    bin/mc ls --config-dir "$PACKED_MC_CONFIG" --recursive "minio/$PACKED_S3_BUCKET/$PACKED_S3_PREFIX/$label" \
        | awk '$NF ~ /\.sst$/ { count++ } END { print count + 0 }'
}

packed_count_pack_objects() {
    local label=$1
    bin/mc ls --config-dir "$PACKED_MC_CONFIG" --recursive "minio/$PACKED_S3_BUCKET/$PACKED_S3_PREFIX/$label" \
        | awk '$NF ~ /\.pack$/ { count++ } END { print count + 0 }'
}

run_packed_test s3
