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
DB="s3_test"
TABLE="tbl"

check_cluster_version 4 0 0 'local backend' || exit 0

set -euE

# Populate the mydumper source
DBPATH="$TEST_DIR/s3.mydump"

# start the s3 server
export MINIO_ACCESS_KEY=s3accesskey
export MINIO_SECRET_KEY=s3secretkey
export MINIO_BROWSER=off
export S3_ENDPOINT=127.0.0.1:9900
rm -rf "$TEST_DIR/$DB"
mkdir -p "$TEST_DIR/$DB"
bin/minio server --address $S3_ENDPOINT "$DBPATH" &
i=0
while ! curl -o /dev/null -v -s "http://$S3_ENDPOINT/"; do
    i=$(($i+1))
    if [ $i -gt 30 ]; then
        echo 'Failed to start minio'
        exit 1
    fi
    sleep 2
done

BUCKET=test-bucket
DATA_PATH=$DBPATH/$BUCKET
mkdir -p $DATA_PATH
echo 'CREATE DATABASE s3_test;' > "$DATA_PATH/$DB-schema-create.sql"
echo "CREATE TABLE t(i INT, s varchar(32));" > "$DATA_PATH/$DB.$TABLE-schema.sql"
echo 'INSERT INTO tbl (i, s) VALUES (1, "1"),(2, "test2"), (3, "qqqtest");' > "$DATA_PATH/$DB.$TABLE.sql"
cat > "$DATA_PATH/$DB.$TABLE.0.csv" << _EOF_
i,s
100,"test100"
101,"\""
102,"😄😄😄😄😄"
104,""
_EOF_

# Fill in the database
# Start importing the tables.
run_sql "DROP DATABASE IF EXISTS $DB;"
run_sql "DROP TABLE IF EXISTS $DB.$TABLE;"

# test not exist path
rm -f $TEST_DIR/lightning.log
SOURCE_DIR="s3://$BUCKET/not-exist-path?endpoint=http%3A//127.0.0.1%3A9900&access_key=$MINIO_ACCESS_KEY&secret_access_key=$MINIO_SECRET_KEY&force_path_style=true"
! run_lightning -d $SOURCE_DIR --backend local 2> /dev/null
grep -Eq "data-source-dir .* doesn't exist or contains no files" $TEST_DIR/lightning.log

# test empty dir
rm -f $TEST_DIR/lightning.log
emptyPath=empty-bucket/empty-path
mkdir -p $DBPATH/$emptyPath
SOURCE_DIR="s3://$emptyPath/not-exist-path?endpoint=http%3A//127.0.0.1%3A9900&access_key=$MINIO_ACCESS_KEY&secret_access_key=$MINIO_SECRET_KEY&force_path_style=true"
! run_lightning -d $SOURCE_DIR --backend local 2> /dev/null
grep -Eq "data-source-dir .* doesn't exist or contains no files" $TEST_DIR/lightning.log

rm -f $TEST_DIR/lightning.log
SOURCE_DIR="s3://$BUCKET/?endpoint=http%3A//127.0.0.1%3A9900&access_key=$MINIO_ACCESS_KEY&secret_access_key=$MINIO_SECRET_KEY&force_path_style=true"
run_lightning -d $SOURCE_DIR --backend local 2> /dev/null
run_sql "SELECT count(*), sum(i) FROM \`$DB\`.$TABLE"
check_contains "count(*): 7"
check_contains "sum(i): 413"

rm -f $TEST_DIR/lightning.log
run_sql "DROP DATABASE IF EXISTS $DB;"
run_sql "DROP TABLE IF EXISTS $DB.$TABLE;"
run_lightning -d $SOURCE_DIR --backend local --config "tests/$TEST_NAME/config_s3_checkpoint.toml" 2> /dev/null
run_sql "SELECT count(*), sum(i) FROM \`$DB\`.$TABLE"
check_contains "count(*): 7"
check_contains "sum(i): 413"
