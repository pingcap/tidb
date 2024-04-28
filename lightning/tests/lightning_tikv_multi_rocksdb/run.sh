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

set -eux

CUR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
. $UTILS_DIR/run_services
export TIKV_CONFIG="$CUR/../config/tikv-multi-rocksdb.toml"
restart_services

check_cluster_version 4 0 0 'local backend' || exit 0

run_sql 'DROP DATABASE IF EXISTS cpeng;'
run_lightning --backend local --log-file "$TEST_DIR/lightning-local.log" -L debug
grep -qF '\"engine\":\"raft-kv2\"' $TEST_DIR/tikv*.log

# Check that everything is correctly imported
run_sql 'SELECT count(*), sum(c) FROM cpeng.a'
check_contains 'count(*): 4'
check_contains 'sum(c): 10'

run_sql 'SELECT count(*), sum(c) FROM cpeng.b'
check_contains 'count(*): 4'
check_contains 'sum(c): 46'
