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

# Integration test for column-constants. Covers:
#   1. All major data types as constant values (t)
#   2. table-filter glob matching (t2)
#   3. Constant does not override an explicit non-NULL source value (t2)
#   4. NULL-healing: empty source field for NOT NULL column gets constant applied (t2)

set -eux

CUR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)

for BACKEND in tidb local; do
  run_sql 'DROP DATABASE IF EXISTS col_constants;'

  run_lightning --config "$CUR/config.toml" --backend $BACKEND
  echo "Import using $BACKEND finished"

  # -----------------------------------------------------------------------
  # Table t: all data types
  # -----------------------------------------------------------------------

  run_sql 'SELECT count(1) FROM col_constants.t;'
  check_contains 'count(1): 3'

  # Columns present in CSV must be imported correctly (not overridden).
  run_sql 'SELECT sum(id) FROM col_constants.t;'
  check_contains 'sum(id): 6'

  run_sql "SELECT count(1) FROM col_constants.t WHERE val IN ('hello','world','foo');"
  check_contains 'count(1): 3'

  # String types
  run_sql "SELECT count(1) FROM col_constants.t WHERE c_varchar = 'acme';"
  check_contains 'count(1): 3'

  run_sql "SELECT count(1) FROM col_constants.t WHERE c_text = 'hello world';"
  check_contains 'count(1): 3'

  # Integer types
  run_sql "SELECT count(1) FROM col_constants.t WHERE c_tinyint = 42;"
  check_contains 'count(1): 3'

  run_sql "SELECT count(1) FROM col_constants.t WHERE c_bigint = 9999999999;"
  check_contains 'count(1): 3'

  run_sql "SELECT count(1) FROM col_constants.t WHERE c_bool = 1;"
  check_contains 'count(1): 3'

  # Fixed/floating point
  run_sql "SELECT count(1) FROM col_constants.t WHERE c_decimal = 3.14;"
  check_contains 'count(1): 3'

  run_sql "SELECT count(1) FROM col_constants.t WHERE c_float BETWEEN 1.4 AND 1.6;"
  check_contains 'count(1): 3'

  # Date/time types
  run_sql "SELECT count(1) FROM col_constants.t WHERE c_date = '2026-04-17';"
  check_contains 'count(1): 3'

  run_sql "SELECT count(1) FROM col_constants.t WHERE c_datetime = '2026-04-17 21:00:00';"
  check_contains 'count(1): 3'

  run_sql "SELECT count(1) FROM col_constants.t WHERE c_timestamp = '2026-04-17 21:00:00';"
  check_contains 'count(1): 3'

  # Complex types
  run_sql "SELECT count(1) FROM col_constants.t WHERE JSON_EXTRACT(c_json, '$.k') = 'v';"
  check_contains 'count(1): 3'

  run_sql "SELECT count(1) FROM col_constants.t WHERE c_enum = 'b';"
  check_contains 'count(1): 3'

  # -----------------------------------------------------------------------
  # Table t2: table-filter, no-override, NULL-healing
  # -----------------------------------------------------------------------

  run_sql 'SELECT count(1) FROM col_constants.t2;'
  check_contains 'count(1): 3'

  # table-filter matched: constant was applied to the row with empty tenant.
  run_sql "SELECT count(1) FROM col_constants.t2 WHERE tenant = 'default-tenant';"
  check_contains 'count(1): 1'

  # Constant must NOT override rows with explicit non-NULL source values.
  run_sql "SELECT count(1) FROM col_constants.t2 WHERE tenant = 'acme';"
  check_contains 'count(1): 1'

  run_sql "SELECT count(1) FROM col_constants.t2 WHERE tenant = 'beta';"
  check_contains 'count(1): 1'

  # region comes from CSV unchanged (it is not a constant column).
  run_sql "SELECT count(1) FROM col_constants.t2 WHERE region = 'eu-west-1';"
  check_contains 'count(1): 1'

done
