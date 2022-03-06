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
TABLES_COUNT=300

PROGRESS_FILE="$TEST_DIR/progress_file"
BACKUPMETAV1_LOG="$TEST_DIR/backup.log"
BACKUPMETAV2_LOG="$TEST_DIR/backupv2.log"
RESTORE_LOG="$TEST_DIR/restore.log"
rm -rf $PROGRESS_FILE

# functions to do float point arithmetric
calc() { awk "BEGIN{print $*}"; }

run_sql "create schema $DB;"

# generate 300 tables with 1 row content.
i=1
while [ $i -le $TABLES_COUNT ]; do
    run_sql "create table $DB.sbtest$i(id int primary key, k int not null, c char(120) not null, pad char(60) not null);"
    run_sql "insert into $DB.sbtest$i values ($i, $i, '$i', '$i');"
    i=$(($i+1))
done

# backup db
echo "backup meta v2 start..."
unset BR_LOG_TO_TERM
rm -f $BACKUPMETAV2_LOG
export GO_FAILPOINTS="github.com/pingcap/tidb/br/pkg/task/progress-call-back=return(\"$PROGRESS_FILE\")"
run_br backup db --db "$DB" --log-file $BACKUPMETAV2_LOG -s "local://$TEST_DIR/${DB}v2" --pd $PD_ADDR --use-backupmeta-v2 
backupv2_size=`grep "backup-data-size" "${BACKUPMETAV2_LOG}" | grep -oP '\[\K[^\]]+' | grep "backup-data-size" | awk -F '=' '{print $2}' | grep -oP '\d*\.\d+'`
echo "backup meta v2 backup size is ${backupv2_size}"
export GO_FAILPOINTS=""

if [[ "$(wc -l <$PROGRESS_FILE)" == "1" ]] && [[ $(grep -c "range" $PROGRESS_FILE) == "1" ]];
then
  echo "use the correct progress unit"
else
  echo "use the wrong progress unit, expect range"
  cat $PROGRESS_FILE
  exit 1
fi

rm -rf $PROGRESS_FILE

echo "backup meta v1 start..."
rm -f $BACKUPMETAV1_LOG
run_br backup db --db "$DB" --log-file $BACKUPMETAV1_LOG -s "local://$TEST_DIR/$DB" --pd $PD_ADDR 
backupv1_size=`grep "backup-data-size" "${BACKUPMETAV1_LOG}" | grep -oP '\[\K[^\]]+' | grep "backup-data-size" | awk -F '=' '{print $2}' | grep -oP '\d*\.\d+'`
echo "backup meta v1 backup size is ${backupv1_size}"


if [ $(calc "${backupv1_size}-${backupv2_size}==0") -eq 1 ]; then 
    echo "backup meta v1 data size match backup meta v2 data size"
else 
    echo "statistics unmatch"
    exit 1
fi

# truncate every table
# (FIXME: drop instead of truncate. if we drop then create-table will still be executed and wastes time executing DDLs)
i=1
while [ $i -le $TABLES_COUNT ]; do
    run_sql "truncate $DB.sbtest$i;"
    i=$(($i+1))
done

rm -rf $RESTORE_LOG
echo "restore 1/300 of the table start..."
run_br restore table --db $DB  --table "sbtest100" --log-file $RESTORE_LOG -s "local://$TEST_DIR/$DB" --pd $PD_ADDR --no-schema
restore_size=`grep "restore-data-size" "${RESTORE_LOG}" | grep -oP '\[\K[^\]]+' | grep "restore-data-size" | awk -F '=' '{print $2}' | grep -oP '\d*\.\d+'`
echo "restore data size is ${restore_size}"

diff=$(calc "$backupv2_size-$restore_size*$TABLES_COUNT")
echo "the difference is ${diff}"

threshold="1"

if [ $(calc "$diff<$threshold") -eq 1 ]; then 
    echo "statistics match" 
else 
    echo "statistics unmatch"
    exit 1 
fi

# restore db
# (FIXME: shouldn't need --no-schema to be fast, currently the alter-auto-id DDL slows things down)
echo "restore start..."
run_br restore db --db $DB -s "local://$TEST_DIR/$DB" --pd $PD_ADDR --no-schema

run_sql "DROP DATABASE $DB;"
