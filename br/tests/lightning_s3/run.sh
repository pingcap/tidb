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

if [[ -z "${TEST_DIR}" ]]; then
    echo "TEST_DIR is not set" >&2
    exit 1
fi

# Populate the mydumper source
DBPATH="$TEST_DIR/s3.mydump"

# start the s3 server
export MINIO_ACCESS_KEY=s3accesskey
export MINIO_SECRET_KEY=s3secretkey
export MINIO_BROWSER=off
export S3_ENDPOINT=127.0.0.1:9900
rm -rf "${TEST_DIR:?}/${DB:?}"
mkdir -p "$TEST_DIR/$DB"
bin/minio server --address $S3_ENDPOINT "$DBPATH" &
i=0
while ! curl -o /dev/null -v -s "http://$S3_ENDPOINT/"; do
    i=$((i+1))
    if [ $i -gt 30 ]; then
        echo 'Failed to start minio'
        exit 1
    fi
    sleep 2
done

BUCKET=test-bucket
DATA_PATH=$DBPATH/$BUCKET
mkdir -p "$DATA_PATH"
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

function cleanup_db_and_table() {
    if ! run_sql "DROP DATABASE IF EXISTS $DB;"; then
        echo "run SQL for drop database error" >&2
        return 1
    fi
    if ! run_sql "DROP TABLE IF EXISTS $DB.$TABLE;"; then
        echo "run SQL for drop table error" >&2
        return 1
    fi
}

# test not exist path
function test_import_non_existing_path() {
    rm -f "$TEST_DIR/lightning.log"
    local SOURCE_DIR="s3://$BUCKET/not-exist-path?endpoint=http%3A//127.0.0.1%3A9900&access_key=$MINIO_ACCESS_KEY&secret_access_key=$MINIO_SECRET_KEY&force_path_style=true"
    if run_lightning -d "$SOURCE_DIR" --backend local 2> /dev/null ; then
	echo "this importing should fail" >&2
	return 2
    fi
    if ! grep -Eq "data-source-dir .* doesn't exist or contains no files" "$TEST_DIR/lightning.log" ; then
	echo "the message is not found in the log" >&2
	return 2
    fi
    return 0
}

# test empty dir
function test_import_empty_dir() {
    rm -f "$TEST_DIR/lightning.log"
    local emptyPath=empty-bucket/empty-path
    mkdir -p "$DBPATH/$emptyPath"
    local SOURCE_DIR="s3://$emptyPath/not-exist-path?endpoint=http%3A//127.0.0.1%3A9900&access_key=$MINIO_ACCESS_KEY&secret_access_key=$MINIO_SECRET_KEY&force_path_style=true"
    if run_lightning -d "$SOURCE_DIR" --backend local 2> /dev/null; then
	echo "this importing should fail" >&2
	return 2
    fi
    if ! grep -Eq "data-source-dir .* doesn't exist or contains no files" "$TEST_DIR/lightning.log"; then
	echo "the message is not found in the log" >&2
	return 2
    fi
    return 0
}

function test_normal_import() {
    rm -f "$TEST_DIR/lightning.log"
    if ! cleanup_db_and_table; then
        echo "cleanup DB and table before running the test failed" >&2
        return 1
    fi
    local SOURCE_DIR="s3://$BUCKET/?endpoint=http%3A//127.0.0.1%3A9900&access_key=$MINIO_ACCESS_KEY&secret_access_key=$MINIO_SECRET_KEY&force_path_style=true"
    if ! run_lightning -d "$SOURCE_DIR" --backend local 2> /dev/null; then
        echo "run lightning failed" >&2
	return 2
    fi
    if ! run_sql "SELECT count(*), sum(i) FROM \`$DB\`.$TABLE"; then
        echo "run SQL on target DB failed" >&2
	return 2
    fi
    if ! check_contains "count(*): 7"; then
        echo "the record count is not right" >&2
        return 2
    fi
    if ! check_contains "sum(i): 413"; then
        echo "the sum of record is not right" >&2
        return 2
    fi
    return 0
}

function test_import_with_checkpoint() {
    rm -f "$TEST_DIR/lightning.log"
    if ! cleanup_db_and_table; then
        echo "cleanup DB and table before running the test failed" >&2
        return 1
    fi
    local SOURCE_DIR="s3://$BUCKET/?endpoint=http%3A//127.0.0.1%3A9900&access_key=$MINIO_ACCESS_KEY&secret_access_key=$MINIO_SECRET_KEY&force_path_style=true"
    if ! run_lightning -d "$SOURCE_DIR" --backend local --config "tests/$TEST_NAME/config_s3_checkpoint.toml" 2> /dev/null; then
        echo "run lightning failed" >&2
	return 2
    fi
    if ! run_sql "SELECT count(*), sum(i) FROM \`$DB\`.$TABLE"; then
        echo "run SQL on target DB failed" >&2
	return 2
    fi
    if ! check_contains "count(*): 7"; then
        echo "the record count is not right" >&2
        return 2
    fi
    if ! check_contains "sum(i): 413"; then
        echo "the sum of record is not right" >&2
        return 2
    fi
}

# test manually organized dir with some empty sub-dirs
function test_import_using_manual_path_config() {
    rm -f "$TEST_DIR/lightning.log"
    if ! cleanup_db_and_table; then
        echo "cleanup DB and table before running the test failed" >&2
        return 1
    fi
    local bucket_02="test-bucket-02"
    local sub_path_02="to-be-imported"
    local base_path_02="${DBPATH}/${bucket_02}/${sub_path_02}"
    rm -rf "${base_path_02}"
    mkdir -p "${base_path_02}"
    touch "${base_path_02}/$DB-schema-create.sql"    # empty schema file
    cp "$DATA_PATH/$DB.$TABLE-schema.sql" "${base_path_02}/"
    local sql_data_path_02="${base_path_02}/data-sql"
    local csv_data_path_02="${base_path_02}/data-csv"
    local parquet_data_path_02="${base_path_02}/data-parquet"
    mkdir -p "${sql_data_path_02}/empty-dir" # try to add an empty path into it
    cp "$DATA_PATH/$DB.$TABLE.sql" "${sql_data_path_02}/"
    touch "${sql_data_path_02}/dummy.sql"    # empty file
    mkdir -p "${csv_data_path_02}/empty-dir" # try to add an empty path into it
    cp "$DATA_PATH/$DB.$TABLE.0.csv" "${csv_data_path_02}/"
    touch "${csv_data_path_02}/dummy.csv"    # empty file
    mkdir -p "${parquet_data_path_02}/empty-dir" # try to add an empty path into it
    # no touch empty file for parquet files

    local SOURCE_DIR="s3://${bucket_02}/${sub_path_02}?endpoint=http%3A//127.0.0.1%3A9900&access_key=$MINIO_ACCESS_KEY&secret_access_key=$MINIO_SECRET_KEY&force_path_style=true"
    if ! run_lightning -d "${SOURCE_DIR}" --backend local --config "tests/$TEST_NAME/config_manual_files.toml" 2> /dev/null; then
        echo "run lightning failed" >&2
        return 2
    fi
    if ! run_sql "SELECT count(*), sum(i) FROM \`$DB\`.$TABLE"; then
        echo "run SQL on target DB failed" >&2
        return 2
    fi
    if ! check_contains "count(*): 7"; then
        echo "the record count is not right" >&2
        return 2
    fi
    if ! check_contains "sum(i): 413"; then
        echo "the sum of record is not right" >&2
        return 2
    fi
    return 0
}

final_ret_code=0
if ! test_import_non_existing_path; then
    final_ret_code=2
fi

if ! test_import_empty_dir; then
    final_ret_code=2
fi

if ! test_normal_import; then
    final_ret_code=2
fi

if ! test_import_with_checkpoint; then
    final_ret_code=2
fi

if ! test_import_using_manual_path_config; then
    final_ret_code=2
fi

exit "${final_ret_code}"
