#!/bin/sh
#
# Copyright 2022 PingCAP, Inc.
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

run_lightning \
    -L info \
    --log-file "$TEST_DIR/lightning.log" \
    --tidb-host 127.0.0.1 \
    --tidb-port 4000 \
    --tidb-user root \
    --tidb-status 10080 \
    --pd-urls 127.0.0.1:2379 \
    -d "$CUR/data" \
    --backend 'tidb'

run_sql 'SELECT * FROM cmdline_override.t'
check_contains 'a: 15'
