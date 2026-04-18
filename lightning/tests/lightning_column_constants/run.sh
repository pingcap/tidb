#!/bin/bash
#
# Copyright 2026 PingCAP, Inc.
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

# Integration test for column-constants: verifies that columns absent from the
# source CSV are filled with the literal values from config, not the DDL DEFAULT.

set -eux

CUR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)

for BACKEND in tidb local; do
  run_sql 'DROP DATABASE IF EXISTS col_constants;'

  run_lightning --config "$CUR/config.toml" --backend $BACKEND
  echo "Import using $BACKEND finished"

  # All 3 rows should be present.
  run_sql 'SELECT count(1) FROM col_constants.t;'
  check_contains 'count(1): 3'

  # customer_name and etl_ts must match the column-constants values, not any DDL DEFAULT.
  run_sql "SELECT count(1) FROM col_constants.t WHERE customer_name = 'acme';"
  check_contains 'count(1): 3'

  run_sql "SELECT count(1) FROM col_constants.t WHERE etl_ts = '2026-04-17 21:00:00';"
  check_contains 'count(1): 3'

  # The columns present in the CSV must be imported correctly.
  run_sql 'SELECT sum(id) FROM col_constants.t;'
  check_contains 'sum(id): 6'
done
