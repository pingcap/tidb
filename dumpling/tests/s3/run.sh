#!/bin/sh

set -eux

echo "starting localstack writing to ${DUMPLING_OUTPUT_DIR}"
mkdir -p "${DUMPLING_OUTPUT_DIR}"
ls "${DUMPLING_OUTPUT_DIR}"
docker run --name dumpling_test_s3 -d \
  -p 5000:5000 \
  motoserver/moto
sleep 1 # wait for motoserver to start up
cleanup() {
  echo "Stopping motoserver"
  docker rm -f dumpling_test_s3
}
trap cleanup EXIT

awslocal() {
  docker run --rm --net=host -it -e PAGER=cat -e AWS_ACCESS_KEY_ID=foo -e AWS_SECRET_ACCESS_KEY=foo amazon/aws-cli --endpoint http://localhost:5000 "$@"
}
awslocal s3api create-bucket --bucket mybucket

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
export AWS_ACCESS_KEY_ID=testid
export AWS_SECRET_ACCESS_KEY=testkey
run_dumpling --s3.endpoint=http://localhost:5000
ls "${HOST_DIR}"

curl -o "${HOST_DIR}/s3-schema-create.sql" http://localhost:5000/mybucket/dump/s3-schema-create.sql
curl -o "${HOST_DIR}/s3.t-schema.sql" http://localhost:5000/mybucket/dump/s3.t-schema.sql
curl -o "${HOST_DIR}/s3.t.0.sql" http://localhost:5000/mybucket/dump/s3.t.0.sql

file_should_exist "$HOST_DIR/s3-schema-create.sql"
file_should_exist "$HOST_DIR/s3.t-schema.sql"
file_should_exist "$HOST_DIR/s3.t.0.sql"
