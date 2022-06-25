#!/bin/sh

set -eux

run_lightning \
    -L info \
    --log-file "$TEST_DIR/lightning.log" \
    --tidb-host 127.0.0.1 \
    --tidb-port 4000 \
    --tidb-user root \
    --tidb-status 10080 \
    --pd-urls 127.0.0.1:2379 \
    -d "tests/$TEST_NAME/data" \
    --backend 'tidb'

run_sql 'SELECT * FROM cmdline_override.t'
check_contains 'a: 15'
