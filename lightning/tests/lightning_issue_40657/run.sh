#!/bin/bash
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

CUR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)

check_cluster_version 5 2 0 'duplicate detection' || exit 0

run_lightning -d "$CUR/data1"
run_sql 'admin check table test.t'
run_sql 'select count(*) from test.t'
check_contains 'count(*): 4'
run_sql 'select count(*) from lightning_task_info.conflict_view'
check_contains 'count(*): 2'

run_sql 'truncate table test.t'
run_lightning -d "$CUR/data2"
run_sql 'admin check table test.t'
run_sql 'select count(*) from test.t'
check_contains 'count(*): 5'
