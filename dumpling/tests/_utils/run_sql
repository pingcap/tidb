#!/bin/sh
#
# Copyright 2020 PingCAP, Inc. Licensed under Apache-2.0.

SQL="$1"
shift

set -e

echo "[$(date)] Executing SQL: $SQL"

mysql \
  -u "$DUMPLING_TEST_USER" \
  -h 127.0.0.1 \
  -P "$DUMPLING_TEST_PORT" \
  --default-character-set=utf8mb4 \
  --database="$DUMPLING_TEST_DATABASE" \
  -E -e "$SQL" "$@"
