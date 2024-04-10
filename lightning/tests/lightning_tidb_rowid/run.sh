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

# Verify that _tidb_rowid is correctly adjusted.

set -eu

for BACKEND in local tidb; do
  if [ "$BACKEND" = 'local' ]; then
      check_cluster_version 4 0 0 'local backend' || continue
  fi

  run_sql 'DROP DATABASE IF EXISTS rowid;'
  run_lightning -backend $BACKEND
  echo 'Import finished'

  # we can't determine the exact `_tidb_row_id` alloc logic, so just skip this check with tidb backend.
  if [ "$BACKEND" != 'tidb' ]; then
    run_sql 'SELECT count(*), max(id), min(_tidb_rowid), max(_tidb_rowid) FROM rowid.`non_pk_auto_inc`'
    check_contains 'count(*): 22'
    check_contains 'max(id): 37'
    check_contains 'min(_tidb_rowid): 1'
    check_contains 'max(_tidb_rowid): 22'
    run_sql 'INSERT INTO rowid.`non_pk_auto_inc` (`pk`) VALUES ("?")'
    run_sql 'SELECT id > 37, _tidb_rowid > 22 FROM rowid.`non_pk_auto_inc` WHERE `pk` = "?"'
    check_contains 'id > 37: 1'
    check_contains '_tidb_rowid > 22: 1'
  fi

  for table_name in non_pk explicit_tidb_rowid; do
      run_sql "SELECT count(*), min(_tidb_rowid), max(_tidb_rowid) FROM rowid.${table_name}"
      check_contains 'count(*): 10'
      check_contains 'min(_tidb_rowid): 1'
      check_contains 'max(_tidb_rowid): 10'
      run_sql "SELECT _tidb_rowid FROM rowid.${table_name} WHERE pk = 'five'"
      check_contains '_tidb_rowid: 5'
      run_sql "INSERT INTO rowid.${table_name} VALUES ('eleven')"
      run_sql "SELECT count(*) FROM rowid.${table_name}"
      check_contains 'count(*): 11'
      run_sql "SELECT count(*) FROM rowid.${table_name} WHERE pk > '!'"
      check_contains 'count(*): 11'
      run_sql "SELECT _tidb_rowid > 10 FROM rowid.${table_name} WHERE pk = 'eleven'"
      check_contains '_tidb_rowid > 10: 1'
  done

  run_sql 'SELECT count(*), min(_tidb_rowid), max(_tidb_rowid) FROM rowid.pre_rebase'
  check_contains 'count(*): 1'
  if [ "$BACKEND" == 'tidb' ]; then
    check_contains 'min(_tidb_rowid): 70000'
    check_contains 'max(_tidb_rowid): 70000'
  else
    check_contains 'min(_tidb_rowid): 1'
    check_contains 'max(_tidb_rowid): 1'
  fi
  run_sql 'INSERT INTO rowid.pre_rebase VALUES ("?")'
  run_sql 'SELECT _tidb_rowid > 70000 FROM rowid.pre_rebase WHERE pk = "?"'
  check_contains '_tidb_rowid > 70000: 1'

  run_sql 'SELECT count(*) FROM rowid.specific_auto_inc'
  check_contains 'count(*): 5'
  run_sql 'INSERT INTO rowid.specific_auto_inc (a) VALUES ("ffffff"), ("gggggg")'
  run_sql 'SELECT _tidb_rowid > 80000, b > 80000 FROM rowid.specific_auto_inc WHERE a = "ffffff"'
  check_contains '_tidb_rowid > 80000: 1'
  check_contains 'b > 80000: 1'
  run_sql 'SELECT _tidb_rowid > 80000, b > 80000 FROM rowid.specific_auto_inc WHERE a = "gggggg"'
  check_contains '_tidb_rowid > 80000: 1'
  check_contains 'b > 80000: 1'
done
