#!/bin/sh
#
# Copyright 2020 PingCAP, Inc. Licensed under Apache-2.0.

set -eux

echo "starting localstack writing to ${DUMPLING_OUTPUT_DIR}"
mkdir -p "${DUMPLING_OUTPUT_DIR}"
ls "${DUMPLING_OUTPUT_DIR}"

DBPATH="${DUMPLING_OUTPUT_DIR}/s3.minio"

export MINIO_ACCESS_KEY=testid
export MINIO_SECRET_KEY=testkey8
export MINIO_BROWSER=off
export S3_ENDPOINT=127.0.0.1:5000
bin/minio server --address $S3_ENDPOINT "$DBPATH" &
MINIO_PID=$!

i=0
while ! curl -o /dev/null -v -s "http://$S3_ENDPOINT/"; do
  i=$(($i+1))
  if [ $i -gt 7 ]; then
    echo 'Failed to start minio'
    exit 1
  fi
  sleep 2
done

cleanup() {
  echo "Stopping motoserver"
  kill -2 $MINIO_PID
}
trap cleanup EXIT

mkdir -p "$DBPATH/mybucket"

DB_NAME="s3"
TABLE_NAME="t"

# drop database on mysql
run_sql "drop database if exists \`$DB_NAME\`;"

# build data on mysql
run_sql "create database $DB_NAME;"
run_sql "create table $DB_NAME.$TABLE_NAME (a int(255));"

# insert 100 records
run_sql "insert into $DB_NAME.$TABLE_NAME values $(seq -s, 100 | sed 's/,*$//g' | sed "s/[0-9]*/('1')/g");"

# run dumpling!
HOST_DIR=${DUMPLING_OUTPUT_DIR}
export DUMPLING_OUTPUT_DIR=s3://mybucket/dump
export DUMPLING_TEST_DATABASE=$DB_NAME
export AWS_REGION=us-east-1
export AWS_ACCESS_KEY_ID="$MINIO_ACCESS_KEY"
export AWS_SECRET_ACCESS_KEY="$MINIO_SECRET_KEY"
run_dumpling --s3.endpoint="http://$S3_ENDPOINT/"
ls "${HOST_DIR}"

curl -o "${HOST_DIR}/s3-schema-create.sql" http://$S3_ENDPOINT/mybucket/dump/s3-schema-create.sql
curl -o "${HOST_DIR}/s3.t-schema.sql" http://$S3_ENDPOINT/mybucket/dump/s3.t-schema.sql
curl -o "${HOST_DIR}/s3.t.000000000.sql" http://$S3_ENDPOINT/mybucket/dump/s3.t.000000000.sql

file_should_exist "$HOST_DIR/s3-schema-create.sql"
file_should_exist "$HOST_DIR/s3.t-schema.sql"
file_should_exist "$HOST_DIR/s3.t.000000000.sql"
