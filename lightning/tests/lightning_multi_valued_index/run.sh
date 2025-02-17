#!/bin/sh
#
# Copyright 2023 PingCAP, Inc.
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

set -eux

check_result() {
    run_sql "ADMIN CHECK TABLE mv.$1"
    run_sql "SELECT count(*) FROM mv.$1"
    check_contains 'count(*): 100'
    run_sql "SELECT -22162723 MEMBER OF (j->'$.number') as r FROM mv.$1 WHERE pk = -918151511"
    check_contains 'r: 1'
    run_sql "SELECT 'kjGNP2fd3X20K9HtIbaBw3VL7Ze58eiysenzLBij4jDFzlnO0Y3qfheDrGmo4OM' MEMBER OF (j->'$.string') as r FROM mv.$1 WHERE pk = 6"
    check_contains 'r: 1'
}

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
for _ in $(seq 3); do
    sleep 1
    run_sql "SELECT CONCAT(@@time_zone, ',', @@default_week_format, ',', @@block_encryption_mode) res"
    if [ "$(read_result)" = '-08:00,4,aes-256-cbc' ]; then
        break
    fi
done

for BACKEND in 'local' 'tidb'; do
    if [ "$BACKEND" = 'local' ]; then
        check_cluster_version 4 0 0 'local backend' || continue
    fi

    run_sql 'DROP DATABASE IF EXISTS mv'

    run_lightning --backend $BACKEND

    check_result 'l'
    check_result 't'

    run_sql 'DROP DATABASE mv'
done
