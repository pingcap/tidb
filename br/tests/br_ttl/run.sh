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
DB="$TEST_NAME"

PROGRESS_FILE="$TEST_DIR/progress_file"
BACKUPMETAV1_LOG="$TEST_DIR/backup.log"
BACKUPMETAV2_LOG="$TEST_DIR/backupv2.log"
RESTORE_LOG="$TEST_DIR/restore.log"
rm -rf $PROGRESS_FILE

run_sql "create schema $DB;"
run_sql "create table $DB.ttl_test_tbl(id int primary key, t datetime) TTL=\`t\` + interval 1 day TTL_ENABLE='ON'"

# backup db
echo "full backup meta v2 start..."
unset BR_LOG_TO_TERM
rm -f $BACKUPMETAV2_LOG
run_br backup full --log-file $BACKUPMETAV2_LOG -s "local://$TEST_DIR/${DB}v2" --pd $PD_ADDR --use-backupmeta-v2

echo "full backup meta v1 start..."
rm -f $BACKUPMETAV1_LOG
run_br backup full --log-file $BACKUPMETAV1_LOG -s "local://$TEST_DIR/$DB" --pd $PD_ADDR

TTL_MARK='![ttl]'
CREATE_SQL_CONTAINS="/*T${TTL_MARK} TTL=\`t\` + INTERVAL 1 DAY */ /*T${TTL_MARK} TTL_ENABLE='OFF' */"

# restore v2
run_sql "DROP DATABASE $DB;"
echo "restore ttl table start v2..."
run_br restore db --db $DB -s "local://$TEST_DIR/${DB}v2" --pd $PD_ADDR
run_sql "show create table $DB.ttl_test_tbl;"
check_contains "$CREATE_SQL_CONTAINS"

# restore v1
run_sql "DROP DATABASE $DB;"
echo "restore ttl table start v1..."
run_br restore db --db $DB -s "local://$TEST_DIR/$DB" --pd $PD_ADDR
run_sql "show create table $DB.ttl_test_tbl;"
check_contains "$CREATE_SQL_CONTAINS"
