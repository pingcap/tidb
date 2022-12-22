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

# Basic check for whether partitioned tables work.

set -eu

for BACKEND in tidb local; do
    if [ "$BACKEND" = 'local' ]; then
      check_cluster_version 4 0 0 'local backend' || continue
    fi

    run_sql 'DROP DATABASE IF EXISTS foreign_key;'

    run_lightning --backend $BACKEND

    run_sql 'SELECT count(1), sum(a) FROM foreign_key.parent;'
    check_contains 'count(1): 4'
    check_contains 'sum(a): 10'

    run_sql 'SELECT count(1), sum(pid) FROM foreign_key.child;'
    check_contains 'count(1): 4'
    check_contains 'sum(pid): 10'
done
