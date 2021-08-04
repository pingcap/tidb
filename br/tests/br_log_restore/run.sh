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
# See the License for the specific language governing permissions and
# limitations under the License.

set -eux
DB="$TEST_NAME"
TABLE="usertable"
DB_COUNT=3
BUCKET="cdcs3"
CDC_COUNT=3

# start the s3 server
export MINIO_ACCESS_KEY=brs3accesskey
export MINIO_SECRET_KEY=brs3secretkey
export MINIO_BROWSER=off
export AWS_ACCESS_KEY_ID=$MINIO_ACCESS_KEY
export AWS_SECRET_ACCESS_KEY=$MINIO_SECRET_KEY
export S3_ENDPOINT=127.0.0.1:24928
rm -rf "$TEST_DIR/$DB"
mkdir -p "$TEST_DIR/$DB"
bin/minio server --address $S3_ENDPOINT "$TEST_DIR/$DB" &
i=0
while ! curl -o /dev/null -s "http://$S3_ENDPOINT/"; do
    i=$(($i+1))
    if [ $i -gt 30 ]; then
        echo 'Failed to start minio'
        exit 1
    fi
    sleep 2
done

bin/mc config --config-dir "$TEST_DIR/$TEST_NAME" \
    host add minio http://$S3_ENDPOINT $MINIO_ACCESS_KEY $MINIO_SECRET_KEY
bin/mc mb --config-dir "$TEST_DIR/$TEST_NAME" minio/$BUCKET

# Start cdc servers
run_cdc server --pd=https://$PD_ADDR --log-file=ticdc.log --addr=0.0.0.0:18301 --advertise-addr=127.0.0.1:18301 &
trap 'cat ticdc.log' ERR

# TODO: remove this after TiCDC supports TiDB clustered index
run_sql "set @@global.tidb_enable_clustered_index=0"
# TiDB global variables cache 2 seconds
sleep 2

# create change feed for s3 log
run_cdc cli changefeed create --pd=https://$PD_ADDR --sink-uri="s3://$BUCKET/$DB?endpoint=http://$S3_ENDPOINT" --changefeed-id="simple-replication-task"

start_ts=$(run_sql "show master status;" | grep Position | awk -F ':' '{print $2}' | xargs)

# Fill in the database
for i in $(seq $DB_COUNT); do
    run_sql "CREATE DATABASE $DB${i};"
    go-ycsb load mysql -P tests/$TEST_NAME/workload -p mysql.host=$TIDB_IP -p mysql.port=$TIDB_PORT -p mysql.user=root -p mysql.db=$DB${i}
done

for i in $(seq $DB_COUNT); do
    row_count_ori[${i}]=$(run_sql "SELECT COUNT(*) FROM $DB${i}.$TABLE;" | awk '/COUNT/{print $2}')
done

# test drop & create schema/table, finally only db2 has one row
run_sql "create schema ${DB}_DDL1;"
run_sql "create table ${DB}_DDL1.t1 (a int primary key, b varchar(10));"
run_sql "insert into ${DB}_DDL1.t1 values (1, 'x');"

run_sql "drop schema ${DB}_DDL1;"
run_sql "create schema ${DB}_DDL1;"
run_sql "create schema ${DB}_DDL2;"

run_sql "create table ${DB}_DDL2.t2 (a int primary key, b varchar(10));"
run_sql "insert into ${DB}_DDl2.t2 values (2, 'x');"

run_sql "drop table ${DB}_DDL2.t2;"
run_sql "create table ${DB}_DDL2.t2 (a int primary key, b varchar(10));"
run_sql "insert into ${DB}_DDL2.t2 values (3, 'x');"
run_sql "delete from ${DB}_DDL2.t2 where a = 3;"
run_sql "insert into ${DB}_DDL2.t2 values (4, 'x');"

end_ts=$(run_sql "show master status;" | grep Position | awk -F ':' '{print $2}' | xargs)


# if we restore with ts range [start_ts, end_ts], then the below record won't be restored.
run_sql "insert into ${DB}_DDL2.t2 values (5, 'x');"

wait_time=0
checkpoint_ts=$(run_cdc cli changefeed query -c simple-replication-task --pd=https://$PD_ADDR | jq '.status."checkpoint-ts"')
while [ "$checkpoint_ts" -lt "$end_ts" ]; do
    echo "waiting for cdclog syncing... (checkpoint_ts = $checkpoint_ts; end_ts = $end_ts)"
    if [ "$wait_time" -gt 300 ]; then
        echo "cdc failed to sync after 300s, please check the CDC log."
        exit 1
    fi
    sleep 5
    wait_time=$(( wait_time + 5 ))
    checkpoint_ts=$(run_cdc cli changefeed query -c simple-replication-task --pd=https://$PD_ADDR | jq '.status."checkpoint-ts"')
done

# remove the change feed, because we don't want to record the drop ddl.
echo "Y" | run_cdc cli unsafe reset --pd=https://$PD_ADDR

for i in $(seq $DB_COUNT); do
    run_sql "DROP DATABASE $DB${i};"
done
run_sql "DROP DATABASE ${DB}_DDL1"
run_sql "DROP DATABASE ${DB}_DDL2"

# restore full
export GO_FAILPOINTS='github.com/pingcap/tidb/br/pkg/lightning/backend/local/FailIngestMeta=return("notleader")'
echo "restore start..."
run_br restore cdclog -s "s3://$BUCKET/$DB" --pd $PD_ADDR --s3.endpoint="http://$S3_ENDPOINT" \
    --log-file "restore.log" --log-level "info" --start-ts $start_ts --end-ts $end_ts

for i in $(seq $DB_COUNT); do
    row_count_new[${i}]=$(run_sql "SELECT COUNT(*) FROM $DB${i}.$TABLE;" | awk '/COUNT/{print $2}')
done

fail=false
row_count=$(run_sql "SELECT COUNT(*) FROM ${DB}_DDL2.t2 WHERE a=4;" | awk '/COUNT/{print $2}')
if [ "$row_count" -ne "1" ]; then
    fail=true
    echo "TEST: [$TEST_NAME] fail on dml&ddl drop test."
fi


# record a=5 shouldn't be restore, because we set -end-ts without this record.
row_count=$(run_sql "SELECT COUNT(*) FROM ${DB}_DDL2.t2 WHERE a=5;" | awk '/COUNT/{print $2}')
if [ "$row_count" -ne "0" ]; then
    fail=true
    echo "TEST: [$TEST_NAME] fail on ts range test."
fi

export GO_FAILPOINTS='github.com/pingcap/tidb/br/pkg/lightning/backend/local/FailIngestMeta=return("epochnotmatch")'
echo "restore again to restore a=5 record..."
run_br restore cdclog -s "s3://$BUCKET/$DB" --pd $PD_ADDR --s3.endpoint="http://$S3_ENDPOINT" \
    --log-file "restore.log" --log-level "info" --start-ts $end_ts

# record a=5 should be restore, because we set -end-ts without this record.
row_count=$(run_sql "SELECT COUNT(*) FROM ${DB}_DDL2.t2 WHERE a=5;" | awk '/COUNT/{print $2}')
if [ "$row_count" -ne "1" ]; then
    fail=true
    echo "TEST: [$TEST_NAME] fail on recover ts range test."
fi

# record a=3 should be deleted
row_count=$(run_sql "SELECT COUNT(*) FROM ${DB}_DDL2.t2 WHERE a=3;" | awk '/COUNT/{print $2}')
if [ "$row_count" -ne "0" ]; then
    fail=true
    echo "TEST: [$TEST_NAME] fail on key not deleted."
fi


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

for i in $(seq $DB_COUNT); do
    run_sql "DROP DATABASE $DB${i};"
done

run_sql "DROP DATABASE ${DB}_DDL1"
run_sql "DROP DATABASE ${DB}_DDL2"
