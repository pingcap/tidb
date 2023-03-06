#!/bin/sh
#
# Copyright 2021 PingCAP, Inc. Licensed under Apache-2.0.

set -eu

export DUMPLING_TEST_PORT=4000

run_sql "drop database if exists policy"
run_sql "drop placement policy if exists x"
run_sql "drop placement policy if exists x1"
run_sql "create database policy DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin"

export DUMPLING_TEST_DATABASE="policy"

run_sql 'CREATE PLACEMENT POLICY x PRIMARY_REGION="cn-east-1" REGIONS="cn-east-1,cn-east";'
run_sql 'CREATE PLACEMENT POLICY x1 FOLLOWERS=4;'

run_dumpling

file_should_exist "$DUMPLING_OUTPUT_DIR/policy-schema-create.sql"
file_should_exist "$DUMPLING_OUTPUT_DIR/x-placement-policy-create.sql"
file_should_exist "$DUMPLING_OUTPUT_DIR/x1-placement-policy-create.sql"

diff "$DUMPLING_BASE_NAME/result/x-placement-policy-create.sql" "$DUMPLING_OUTPUT_DIR/x-placement-policy-create.sql"
diff "$DUMPLING_BASE_NAME/result/x1-placement-policy-create.sql" "$DUMPLING_OUTPUT_DIR/x1-placement-policy-create.sql"

run_sql "drop placement policy if exists x"
run_sql "drop placement policy if exists x1"
run_sql "drop database if exists policy"
