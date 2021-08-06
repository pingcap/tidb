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

for backend in tidb importer local; do
    if [ "$backend" = 'local' ]; then
        check_cluster_version 4 0 0 'local backend' || continue
    fi

    run_sql 'DROP DATABASE IF EXISTS issue410;'
    run_lightning --backend $backend

    run_sql "SELECT * FROM issue410.row_flow_d"
    check_contains 'A123456789012345678901234567890123456789: A'
    check_contains 'Y12345678901234567890123456789012345678: Y'
    check_contains 'Z: Z'
done
