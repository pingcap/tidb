#!/bin/sh
#
# Copyright 2019 PingCAP, Inc.
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

check_cluster_version 4 0 0 'new collation' || { echo 'TiDB does not support new collation! skipping test'; exit 0; }

set -euE

cur=$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )
. $cur/../_utils/run_services

# restart cluster with new collation enabled
start_services --tidb-cfg $cur/tidb-new-collation.toml

for BACKEND in local importer tidb; do
  # Start importing the tables.
  run_sql 'DROP DATABASE IF EXISTS nc'

  run_lightning --backend $BACKEND 2> /dev/null

  run_sql 'SELECT count(*), sum(i) FROM `nc`.t'
  check_contains "count(*): 6"
  check_contains "sum(i): 21"

  # run sql with index `s_j`, if lightning don't support new collation, no result will be returned.
  run_sql "SELECT j FROM nc.t WHERE s = 'This_Is_Test4'";
  check_contains "j: 4"

  run_sql "SELeCT i, v from nc.ci where v = 'aa';"
  check_contains "i: 1"
  check_contains "v: aA"

done

# restart with original config
start_services
