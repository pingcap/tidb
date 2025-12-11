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

set -eu

CUR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)

function run_with() {
	backend=$1
	config_file=$2
  if [ "$backend" = 'local' ]; then
    check_cluster_version 4 0 0 'local backend' || continue
  fi

  run_sql 'DROP DATABASE IF EXISTS csv'
  run_sql 'DROP DATABASE IF EXISTS auto_incr_id'
  run_sql 'DROP DATABASE IF EXISTS no_auto_incr_id'

  run_lightning --backend $backend --config $config_file

  run_sql 'SELECT count(*), sum(PROCESSLIST_TIME), sum(THREAD_OS_ID), count(PROCESSLIST_STATE) FROM csv.threads'
  check_contains 'count(*): 43'
  check_contains 'sum(PROCESSLIST_TIME): 322253'
  check_contains 'sum(THREAD_OS_ID): 303775702'
  check_contains 'count(PROCESSLIST_STATE): 3'

  run_sql 'SELECT count(*) FROM csv.threads WHERE PROCESSLIST_TIME IS NOT NULL'
  check_contains 'count(*): 12'

  run_sql 'SELECT hex(t), j, hex(b) FROM csv.escapes WHERE i = 1'
  check_contains 'hex(t): 5C'
  check_contains 'j: {"?": []}'
  check_contains 'hex(b): FFFFFFFF'

  run_sql 'SELECT hex(t), j, hex(b) FROM csv.escapes WHERE i = 2'
  check_contains 'hex(t): 22'
  check_contains 'j: "\n\n\n"'
  check_contains 'hex(b): 0D0A0D0A'

  run_sql 'SELECT hex(t), j, hex(b) FROM csv.escapes WHERE i = 3'
  check_contains 'hex(t): 0A'
  check_contains 'j: [",,,"]'
  check_contains 'hex(b): 5C2C5C2C'

  run_sql 'SELECT id FROM csv.empty_strings WHERE a = """"'
  check_contains 'id: 3'
  run_sql 'SELECT id FROM csv.empty_strings WHERE b <> ""'
  check_not_contains 'id:'

  for table in clustered nonclustered clustered_cache1 nonclustered_cache1 nonclustered_cache1_shard_autorowid nonclustered_cache1_initial_autoid; do
    echo "check for table $table"
    run_sql "select count(*) from auto_incr_id.$table"
    check_contains 'count(*): 3'
    # insert should work
    run_sql "insert into auto_incr_id.$table(v) values(1)"
    run_sql "select count(*) from auto_incr_id.$table"
    check_contains 'count(*): 4'
  done

  for table in clustered nonclustered clustered_cache1 nonclustered_cache1 no_pk no_pk_cache1; do
    echo "check for table $table"
    run_sql "select count(*) from no_auto_incr_id.$table"
    check_contains 'count(*): 3'
    # insert should work
    run_sql "insert into no_auto_incr_id.$table values(1, 1)"
    run_sql "select count(*) from no_auto_incr_id.$table"
    check_contains 'count(*): 4'
  done
}

set +e
run_lightning --backend import-into -d "$CUR/errData" --log-file "$TEST_DIR/lightning-err.log" 2>/dev/null
set -e
## err content presented
grep ",7,8" "$TEST_DIR/lightning-err.log"
# pos should not set to end
grep "[\"syntax error\"] [pos=22]" "$TEST_DIR/lightning-err.log"
