#!/bin/sh
#
# Copyright 2020 PingCAP, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# See the License for the specific language governing permissions and
# limitations under the License.

set -eux

run_sql "SELECT CONCAT('SET GLOBAL time_zone=''', @@time_zone, ''', GLOBAL default_week_format=', @@default_week_format, ', GLOBAL block_encryption_mode=''', @@block_encryption_mode, ''';') cmd;"
UNDO_CMD=$(read_result)
undo_set_globals() {
    run_sql "$UNDO_CMD"
}
trap undo_set_globals EXIT

# There is normally a 2 second delay between these SET GLOBAL statements returns
# and the changes are actually effective. So we have this check-and-retry loop
# below to ensure Lightning gets our desired global vars.
run_sql "SET GLOBAL time_zone='-08:00', GLOBAL default_week_format=4, GLOBAL block_encryption_mode='aes-256-cbc'"
for i in $(seq 3); do
    sleep 1
    run_sql "SELECT CONCAT(@@time_zone, ',', @@default_week_format, ',', @@block_encryption_mode) res"
    if [ "$(read_result)" = '-08:00,4,aes-256-cbc' ]; then
        break
    fi
done

for BACKEND in 'local' 'tidb' 'importer'; do
    if [ "$BACKEND" = 'local' ]; then
        check_cluster_version 4 0 0 'local backend' || continue
    fi

    run_sql 'DROP DATABASE IF EXISTS gencol'

    run_lightning --backend $BACKEND

    run_sql 'SELECT * FROM gencol.nested WHERE a = 100'
    check_contains 'a: 100'
    check_contains 'b: 101'
    check_contains 'c: 102'
    check_contains 'd: 103'
    check_contains 'e: 104'

    run_sql 'SELECT * FROM gencol.various_types' --binary-as-hex
    check_contains 'int64: 3'
    check_contains 'uint64: 5764801'
    check_contains 'float32: 0.5625'
    check_contains 'float64: 5e222'
    check_contains 'string: 6ad8402ba6610f04d3ec5c9875489a7bc8e259c5'
    check_contains 'bytes: 0x6AD8402BA6610F04D3EC5C9875489A7BC8E259C5'
    check_contains 'decimal: 1234.5678'
    check_contains 'duration: 01:02:03'
    check_contains 'enum: c'
    check_contains 'bit: 0x03'
    check_contains 'set: c'
    check_contains 'time: 1987-06-05 04:03:02.100'
    check_contains 'json: {"6ad8402ba6610f04d3ec5c9875489a7bc8e259c5": 0.5625}'
    check_contains 'aes: 0xA876B03CFC8AF93D22D19E2220BD2375'
    # FIXME: test below disabled due to pingcap/tidb#21510
    # check_contains 'week: 6'
    check_contains 'tz: 1969-12-31 16:00:01'
done
