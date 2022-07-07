#!/bin/bash
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

set -eux
DB="$TEST_NAME"
TABLE="usertable"
DB_COUNT=3

# start the s3 server
export MINIO_ACCESS_KEY='KEXI7MANNASOPDLAOIEF'
export MINIO_SECRET_KEY='MaKYxEGDInMPtEYECXRJLU+FPNKb/wAX/MElir7E'
export MINIO_BROWSER=off
export AWS_ACCESS_KEY_ID=$MINIO_ACCESS_KEY
export AWS_SECRET_ACCESS_KEY=$MINIO_SECRET_KEY
export S3_ENDPOINT=127.0.0.1:24927

rm -rf "$TEST_DIR/$DB"
mkdir -p "$TEST_DIR/$DB"
sig_file="$TEST_DIR/sig_file_$RANDOM"
rm -f "$sig_file"

s3_pid=""
start_s3() {
    bin/minio server --address $S3_ENDPOINT "$TEST_DIR/$DB" &
    s3_pid=$!
    i=0
    status="$(curl -o /dev/null -v -s "http://$S3_ENDPOINT/" -w '%{http_code}' || true)"
    while ! { [ "$status" -gt 0 ] && [ "$status" -lt 500 ]; } ; do
        i=$(($i+1))
        if [ $i -gt 30 ]; then
            echo 'Failed to start minio'
            exit 1
        fi
        sleep 2
        status="$(curl -o /dev/null -v -s "http://$S3_ENDPOINT/" -w '%{http_code}' || true)"
    done
}

wait_sig() {
    until [ -e "$sig_file" ]; do
        sleep 1
    done
}

start_s3
echo "started s3 with pid = $s3_pid"
bin/mc config --config-dir "$TEST_DIR/$TEST_NAME" \
    host add minio http://$S3_ENDPOINT $MINIO_ACCESS_KEY $MINIO_SECRET_KEY

# Fill in the database
for i in $(seq $DB_COUNT); do
    run_sql "CREATE DATABASE $DB${i};"
    go-ycsb load mysql -P tests/$TEST_NAME/workload -p mysql.host=$TIDB_IP -p mysql.port=$TIDB_PORT -p mysql.user=root -p mysql.db=$DB${i}
done

bin/mc mb --config-dir "$TEST_DIR/$TEST_NAME" minio/mybucket
S3_KEY=""
for p in $(seq 2); do

  for i in $(seq $DB_COUNT); do
      row_count_ori[${i}]=$(run_sql "SELECT COUNT(*) FROM $DB${i}.$TABLE;" | awk '/COUNT/{print $2}')
  done

  # backup full
  echo "backup start..."
  BACKUP_LOG="backup.log"
  rm -f $BACKUP_LOG
  unset BR_LOG_TO_TERM
  ( GO_FAILPOINTS="github.com/pingcap/tidb/br/pkg/task/s3-outage-during-writing-file=1*return(\"$sig_file\")" \
      run_br --pd $PD_ADDR backup full -s "s3://mybucket/$DB?endpoint=http://$S3_ENDPOINT$S3_KEY" \
      --log-file $BACKUP_LOG || \
      ( cat $BACKUP_LOG && BR_LOG_TO_TERM=1 && exit 1 ) ) &
  br_pid=$!

  sleep 3
  kill -9 $s3_pid
  sleep 15
  start_s3
  wait_sig
  kill -9 $s3_pid
  sleep 15
  start_s3
  wait $br_pid

  cat $BACKUP_LOG
  BR_LOG_TO_TERM=1

  if grep -i $MINIO_SECRET_KEY $BACKUP_LOG; then
      echo "Secret key logged in log. Please remove them."
      exit 1
  fi

  target_log="get new_collations_enabled_on_first_bootstrap config from system table"
  if ! grep -i "$target_log" $BACKUP_LOG; then
      echo "${target_log} not found in log"
      exit 1
  fi

  for i in $(seq $DB_COUNT); do
      run_sql "DROP DATABASE $DB${i};"
  done

  # restore full
  echo "restore start..."
  RESTORE_LOG="restore.log"
  rm -f $RESTORE_LOG
  unset BR_LOG_TO_TERM
  ( run_br restore full -s "s3://mybucket/$DB?$S3_KEY" --pd $PD_ADDR --s3.endpoint="http://$S3_ENDPOINT" \
      --ratelimit 1 \
      --log-file $RESTORE_LOG || \
      ( cat $RESTORE_LOG && BR_LOG_TO_TERM=1 && exit 1 ) ) &
  br_pid=$!
  # Make a S3 outage.
  sleep 3
  kill -9 $s3_pid
  sleep 15
  start_s3
  wait $br_pid
  cat $RESTORE_LOG
  BR_LOG_TO_TERM=1


  if grep -i $MINIO_SECRET_KEY $RESTORE_LOG; then
      echo "Secret key logged in log. Please remove them."
      exit 1
  fi

  for i in $(seq $DB_COUNT); do
      row_count_new[${i}]=$(run_sql "SELECT COUNT(*) FROM $DB${i}.$TABLE;" | awk '/COUNT/{print $2}')
  done

  fail=false
  for i in $(seq $DB_COUNT); do
      if [ "${row_count_ori[i]}" != "${row_count_new[i]}" ];then
          fail=true
          echo "TEST: [$TEST_NAME] fail on database $DB${i}"
      fi
      echo "database $DB${i} [original] row count: ${row_count_ori[i]}, [after br] row count: ${row_count_new[i]}"
  done

  if $fail; then
      echo "TEST: [$TEST_NAME] failed!"
      exit 1
  fi

  # prepare for next test
  bin/mc rm --config-dir "$TEST_DIR/$TEST_NAME" --recursive --force minio/mybucket
  S3_KEY="&access-key=$MINIO_ACCESS_KEY&secret-access-key=$MINIO_SECRET_KEY"
  export AWS_ACCESS_KEY_ID=""
  export AWS_SECRET_ACCESS_KEY=""
done

for i in $(seq $DB_COUNT); do
    run_sql "DROP DATABASE $DB${i};"
done
