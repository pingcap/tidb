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

set -eu
DB="$TEST_NAME"

cur=$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )
source $cur/../_utils/run_services

PROGRESS_FILE="$TEST_DIR/progress_unit_file"
rm -rf $PROGRESS_FILE

run_sql "CREATE DATABASE $DB;"

run_sql "CREATE TABLE $DB.usertable1 ( \
  YCSB_KEY varchar(64) NOT NULL, \
  FIELD0 varchar(1) DEFAULT NULL, \
  PRIMARY KEY (YCSB_KEY) \
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin;"

run_sql "INSERT INTO $DB.usertable1 VALUES (\"a\", \"b\");"
run_sql "INSERT INTO $DB.usertable1 VALUES (\"aa\", \"b\");"

run_sql "CREATE TABLE $DB.usertable2 ( \
  YCSB_KEY varchar(64) NOT NULL, \
  FIELD0 varchar(1) DEFAULT NULL, \
  PRIMARY KEY (YCSB_KEY) \
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin;"

run_sql "INSERT INTO $DB.usertable2 VALUES (\"c\", \"d\");"

# backup db
echo "backup start ... with brv4.0.8 without NewCollactionEnable"
bin/brv4.0.8 backup db --db "$DB" -s "local://$TEST_DIR/$DB" \
    --ca "$TEST_DIR/certs/ca.pem" \
    --cert "$TEST_DIR/certs/br.pem" \
    --key "$TEST_DIR/certs/br.key" \
    --pd $PD_ADDR \
    --check-requirements=false

# restore db from v4.0.8 version without `newCollationEnable`
echo "restore start ... without NewCollactionEnable in backupmeta"
restore_fail=0
error_str="the config 'new_collations_enabled_on_first_bootstrap' not found in backupmeta"
test_log="new_collotion_enable_test.log"
unset BR_LOG_TO_TERM
run_br restore db --db $DB -s "local://$TEST_DIR/$DB" --pd $PD_ADDR --log-file $test_log || restore_fail=1
if [ $restore_fail -ne 1 ]; then
    echo "TEST: [$TEST_NAME] test restore failed!"
    exit 1
fi

if ! grep -i "$error_str" $test_log; then
    echo "${error_str} not found in log"
    echo "TEST: [$TEST_NAME] test restore failed!"
    exit 1
fi

rm -rf "$test_log"

# backup with NewCollationEable = false
echo "Restart cluster with new_collation_enable=false"
start_services --tidb-cfg $cur/config/new_collation_enable_false.toml

echo "backup start ... witch NewCollactionEnable=false in TiDB"
run_br --pd $PD_ADDR backup db --db "$DB" -s "local://$cur/${DB}_2"

echo "Restart cluster with new_collation_enable=true"
start_services --tidb-cfg $cur/config/new_collation_enable_true.toml

echo "restore start ... with NewCollactionEnable=True in TiDB"
restore_fail=0
test_log2="new_collotion_enable_test2.log"
error_str="the config 'new_collations_enabled_on_first_bootstrap' not match"
unset BR_LOG_TO_TERM
run_br restore db --db $DB -s "local://$cur/${DB}_2" --pd $PD_ADDR --log-file $test_log2 || restore_fail=1
if [ $restore_fail -ne 1 ]; then
    echo "TEST: [$TEST_NAME] test restore failed!"
    exit 1
fi

if ! grep -i "$error_str" $test_log2; then
    echo "${error_str} not found in log"
    echo "TEST: [$TEST_NAME] test restore failed!"
    exit 1
fi

rm -rf "$test_log2"
rm -rf "$cur/${DB}_2"
