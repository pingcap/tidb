#!/bin/sh
#
# Copyright 2020 PingCAP, Inc. Licensed under Apache-2.0.

set -e

echo "[$(date)] Executing bin/dumpling..."
echo "$DUMPLING_OUTPUT_DIR"

bin/dumpling -u "$DUMPLING_TEST_USER" -h 127.0.0.1 \
    -P "$DUMPLING_TEST_PORT" -B "$DUMPLING_TEST_DATABASE" \
    -o "$DUMPLING_OUTPUT_DIR" "$@"
