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

# Basic check for whether routing rules work

set -eux

echo "testing checksum-error..."
export GO_FAILPOINTS="github.com/pingcap/tidb/lightning/pkg/importer/checksum-error=1*return()"

run_sql 'DROP DATABASE IF EXISTS routes_a0;'
run_sql 'DROP DATABASE IF EXISTS routes_a1;'
run_sql 'DROP DATABASE IF EXISTS routes_b;'

run_lightning

echo "test checksum-error success!"

run_sql 'SELECT count(1), sum(x) FROM routes_b.u;'
check_contains 'count(1): 4'
check_contains 'sum(x): 259'

run_sql 'SELECT count(1), sum(x) FROM routes_a1.s1;'
check_contains 'count(1): 1'
check_contains 'sum(x): 1296'

run_sql 'SHOW TABLES IN routes_a1;'
check_not_contains 'Tables_in_routes_a1: t2'

