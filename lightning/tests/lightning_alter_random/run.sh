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
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

set -eu

# FIXME: auto-random is only stable on master currently.
check_cluster_version 4 0 0 AUTO_RANDOM || exit 0

# test lightning with autocommit disabled
run_sql "SET @@global.autocommit = '0';"

for backend in tidb local; do
    if [ "$backend" = 'local' ]; then
        check_cluster_version 4 0 0 'local backend' || continue
    fi

    run_sql 'DROP DATABASE IF EXISTS alter_random;'
    run_lightning --backend $backend

    run_sql "SELECT count(*) from alter_random.t"
    check_contains "count(*): 3"

    run_sql "SELECT id & b'000001111111111111111111111111111111111111111111111111111111111' as inc FROM alter_random.t"
    check_contains 'inc: 1'
    check_contains 'inc: 2'
    check_contains 'inc: 3'

    # auto random base is 4
    run_sql "INSERT INTO alter_random.t VALUES ();commit;"
    run_sql "SELECT id & b'000001111111111111111111111111111111111111111111111111111111111' as inc FROM alter_random.t"
    if [ "$backend" = 'tidb' ]; then
      check_contains 'inc: 30002'
    else
      check_contains 'inc: 4'
    fi
done

run_sql "SET @@global.autocommit = '1';"
