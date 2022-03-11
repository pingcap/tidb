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
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

set -eu
DB="$TEST_NAME"
TABLES_COUNT=30

PROGRESS_FILE="$TEST_DIR/progress_file"
BACKUPMETAV1_LOG="$TEST_DIR/backup.log"
BACKUPMETAV2_LOG="$TEST_DIR/backupv2.log"
RESTORE_LOG="$TEST_DIR/restore.log"
rm -rf $PROGRESS_FILE

run_sql "create schema $DB;"
run_sql "create placement policy fivereplicas followers=4;"

# generate 30 tables with 1 row content with policy fivereplicas;.
i=1
while [ $i -le $TABLES_COUNT ]; do
    run_sql "create table $DB.sbtest$i(id int primary key, k int not null, c char(120) not null, pad char(60) not null) placement policy=fivereplicas;"
    run_sql "insert into $DB.sbtest$i values ($i, $i, '$i', '$i');"
    i=$(($i+1))
done

# backup db
echo "full backup meta v2 start..."
unset BR_LOG_TO_TERM
rm -f $BACKUPMETAV2_LOG
run_br backup full --log-file $BACKUPMETAV2_LOG -s "local://$TEST_DIR/${DB}v2" --pd $PD_ADDR --use-backupmeta-v2

echo "full backup meta v1 start..."
rm -f $BACKUPMETAV1_LOG
run_br backup full --log-file $BACKUPMETAV1_LOG -s "local://$TEST_DIR/$DB" --pd $PD_ADDR

# clear data
run_sql "DROP DATABASE $DB;"

# restore with tidb-placement-policy
echo "restore with tidb-placement start..."
run_br restore --db $DB -s "local://$TEST_DIR/${DB}v2" --pd $PD_ADDR

policy_count=$(run_sql "use $DB; show placement;" | grep "POLICY" | wc -l)
if [ "$policy_count" -ne "1" ];then
    echo "TEST: [$TEST_NAME] failed! due to policy restore failed"
    exit 1
fi

# clear data
run_sql "DROP DATABASE $DB;"

# restore without tidb-placement-policy
echo "restore without tidb-placement start..."
run_br restore --db $DB -s "local://$TEST_DIR/$DB" --pd $PD_ADDR --with-tidb-placement-policy=false

policy_count=$(run_sql "use $DB; show placement;" | grep "POLICY" | wc -l)
if [ "$policy_count" -ne "0" ];then
    echo "TEST: [$TEST_NAME] failed! due to policy restore failed"
    exit 1
fi

run_sql "DROP DATABASE $DB;"
