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

set -eux
DB="$TEST_NAME"
TABLE="usertable"

# start log task
run_br log start --task-name 1234 -s "local://$TEST_DIR/$DB/log" --pd $PD_ADDR
if ! grep -i "inc log backup task" "$TEST_DIR/tidb.log"; then
    echo "TEST: [$TEST_NAME] log start failed!"
    exit 1
fi

run_sql "CREATE DATABASE $DB;"
run_sql "CREATE TABLE $DB.$TABLE (id int);"
run_sql "INSERT INTO $DB.$TABLE VALUES (1), (2), (3);"

# backup full
run_br backup full -s "local://$TEST_DIR/$DB/full" --pd $PD_ADDR

# clean db
run_sql "DROP DATABASE $DB;"

# restore full (should be failed)
run_br restore full -s "local://$TEST_DIR/$DB/full" --pd $PD_ADDR && exit 1

# restore point (should be failed)
run_br restore point -s "local://$TEST_DIR/$DB/log" --pd $PD_ADDR && exit 1

# pause log task
run_br log pause --task-name 1234 --pd $PD_ADDR

# restore full (should be failed)
run_br restore full -s "local://$TEST_DIR/$DB/full" --pd $PD_ADDR && exit 1

# restore point (should be failed)
run_br restore point -s "local://$TEST_DIR/$DB/log" --pd $PD_ADDR && exit 1

# stop log task
unset BR_LOG_TO_TERM
run_br log stop --task-name 1234 --pd $PD_ADDR 
if ! grep -i "dec log backup task" "$TEST_DIR/tidb.log"; then
    echo "TEST: [$TEST_NAME] log stop failed!"
    exit 1
fi

# restore full (should be success)
run_br restore full -s "local://$TEST_DIR/$DB/full" --pd $PD_ADDR

# clean db
run_sql "DROP DATABASE $DB"
