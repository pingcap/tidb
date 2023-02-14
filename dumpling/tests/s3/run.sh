#!/bin/sh
#
# Copyright 2020 PingCAP, Inc. Licensed under Apache-2.0.

set -eu

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
run_sql "create database $DB_NAME DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin;"

# load 50MB data into MySQL
(cd "$(dirname "$0")" && GO111MODULE=on go build -o out)
$DUMPLING_BASE_NAME/out -B $DB_NAME -T $TABLE_NAME -P 3306 -w 16

HOST_DIR=${DUMPLING_OUTPUT_DIR}

# run dumpling local
export DUMPLING_OUTPUT_DIR=${HOST_DIR}/local
run_dumpling

# run dumpling s3
export DUMPLING_OUTPUT_DIR=s3://mybucket/dump
export DUMPLING_TEST_DATABASE=$DB_NAME
export AWS_REGION=us-east-1
export AWS_ACCESS_KEY_ID="$MINIO_ACCESS_KEY"
export AWS_SECRET_ACCESS_KEY="$MINIO_SECRET_KEY"
run_dumpling --s3.endpoint="http://$S3_ENDPOINT/"
ls "${HOST_DIR}" -R

mkdir -p "${HOST_DIR}/remote"

bin/mc config host add minio http://127.0.0.1:5000 testid testkey8
bin/mc cp minio/mybucket/dump/s3-schema-create.sql "${HOST_DIR}/remote/s3-schema-create.sql"
bin/mc cp minio/mybucket/dump/s3.t-schema.sql "${HOST_DIR}/remote/s3.t-schema.sql"
bin/mc cp minio/mybucket/dump/s3.t.000000000.sql "${HOST_DIR}/remote/s3.t.000000000.sql"

diff "${HOST_DIR}/local/s3-schema-create.sql" "${HOST_DIR}/remote/s3-schema-create.sql"
diff "${HOST_DIR}/local/s3.t-schema.sql" "${HOST_DIR}/remote/s3.t-schema.sql"
diff "${HOST_DIR}/local/s3.t.000000000.sql" "${HOST_DIR}/remote/s3.t.000000000.sql"

cnt=`grep -o "('aaaaaaaaaa')" ${HOST_DIR}/remote/s3.t.000000000.sql|wc -l`
echo "1st records count is ${cnt}"
[ $cnt = 5000000 ]

# run dumpling with compress option
export DUMPLING_OUTPUT_DIR=s3://mybucket/dump-compress

for compressType in "gzip" "snappy" "zst"
do
  echo -e "\nstart ${compressType} test"
  run_dumpling --s3.endpoint="http://$S3_ENDPOINT/" --compress ${compressType}

  mkdir -p "${HOST_DIR}/compress"

  case $compressType in
  "gzip")
    suffix="gz"
    binary=gzip
  ;;

  "snappy")
    suffix="snappy"
    binary=snappy
  ;;

  "zst")
    suffix="zst"
    binary=zstd
  ;;
  esac

  bin/mc cp minio/mybucket/dump-compress/s3-schema-create.sql.${suffix} "${HOST_DIR}/compress/s3-schema-create.sql.${suffix}"
  bin/mc cp minio/mybucket/dump-compress/s3.t-schema.sql.${suffix} "${HOST_DIR}/compress/s3.t-schema.sql.${suffix}"
  bin/mc cp minio/mybucket/dump-compress/s3.t.000000000.sql.${suffix} "${HOST_DIR}/compress/s3.t.000000000.sql.${suffix}"

  ${binary} -d "${HOST_DIR}/compress/s3-schema-create.sql.${suffix}"
  diff "${HOST_DIR}/local/s3-schema-create.sql" "${HOST_DIR}/compress/s3-schema-create.sql"

  ${binary} -d "${HOST_DIR}/compress/s3.t-schema.sql.${suffix}"
  diff "${HOST_DIR}/local/s3.t-schema.sql" "${HOST_DIR}/compress/s3.t-schema.sql"

  ${binary} -d "${HOST_DIR}/compress/s3.t.000000000.sql.${suffix}"
  diff "${HOST_DIR}/local/s3.t.000000000.sql" "${HOST_DIR}/compress/s3.t.000000000.sql"

  rm "${HOST_DIR}/compress/s3-schema-create.sql"
  rm "${HOST_DIR}/compress/s3.t-schema.sql"
  rm "${HOST_DIR}/compress/s3.t.000000000.sql"
done

run_sql "drop database if exists \`$DB_NAME\`;"
