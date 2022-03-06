#!/bin/sh
#
# Copyright 2020 PingCAP, Inc. Licensed under Apache-2.0.

set -eu
cur=$(cd `dirname $0`; pwd)

DB_NAME="basic"
TABLE_NAME="t"
SEQUENCE_NAME="s"

# Test for simple case.
run_sql "drop database if exists \`$DB_NAME\`;"
run_sql "create database \`$DB_NAME\` DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin;"
run_sql "create table \`$DB_NAME\`.\`$TABLE_NAME\` (a int);"
run_sql "insert into \`$DB_NAME\`.\`$TABLE_NAME\` values (1), (2);"

run_dumpling -f "$DB_NAME.$TABLE_NAME" -L ${DUMPLING_OUTPUT_DIR}/dumpling.log

cnt=$(grep -w "(.*)" ${DUMPLING_OUTPUT_DIR}/${DB_NAME}.${TABLE_NAME}.000000000.sql|wc -l)
echo "records count is ${cnt}"
[ "$cnt" = 2 ]

# make sure that dumpling log contains version infomation
cnt=$(grep -w "Welcome to dumpling.*Release Version.*Git Commit Hash.*Go Version" ${DUMPLING_OUTPUT_DIR}/dumpling.log|wc -l)
echo "version info count is ${cnt}"
[ "$cnt" = 1 ]

# Test for simple WHERE case.
run_sql "drop database if exists \`$DB_NAME\`;"
run_sql "create database \`$DB_NAME\` DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin;"
run_sql "create table \`$DB_NAME\`.\`$TABLE_NAME\` (a int);"

seq 10 | xargs -I_ run_sql "insert into \`$DB_NAME\`.\`$TABLE_NAME\` values (_);"

run_dumpling --where "a >= 3 and a <= 9" -f "$DB_NAME.$TABLE_NAME"

actual=$(grep -w "(.*)" ${DUMPLING_OUTPUT_DIR}/${DB_NAME}.${TABLE_NAME}.000000000.sql | cut -c2-2)
expected=$(seq 3 9)
echo "expected ${expected}, actual ${actual}"
[ "$actual" = "$expected" ]

# Test for OR WHERE case. Better dump MySQL here because Dumpling has some special handle for concurrently dump TiDB tables.
export DUMPLING_TEST_PORT=3306
run_sql "drop database if exists \`$DB_NAME\`;"
run_sql "create database \`$DB_NAME\` DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin;"
run_sql "create table \`$DB_NAME\`.\`$TABLE_NAME\` (a int primary key, b int);"

seq 0 99 | xargs -I_ run_sql "insert into \`$DB_NAME\`.\`$TABLE_NAME\` (a,b) values (_, 99-_);"
run_sql "analyze table \`$DB_NAME\`.\`$TABLE_NAME\`;"
run_dumpling --where "b <= 4 or b >= 95" -f "$DB_NAME.$TABLE_NAME" --rows 10

actual=$(grep -w "(.*)" ${DUMPLING_OUTPUT_DIR}/${DB_NAME}.${TABLE_NAME}.000000000.sql | cut -c2-2)
expected=$(seq 0 4)
echo "expected ${DUMPLING_OUTPUT_DIR}/${DB_NAME}.${TABLE_NAME}.000000000.sql ${expected}, actual ${actual}"
[ "$actual" = "$expected" ]
actual=$(grep -w "(.*)" ${DUMPLING_OUTPUT_DIR}/${DB_NAME}.${TABLE_NAME}.000000009.sql | cut -c2-3)
expected=$(seq 95 99)
echo "expected ${DUMPLING_OUTPUT_DIR}/${DB_NAME}.${TABLE_NAME}.000000009.sql ${expected}, actual ${actual}"
[ "$actual" = "$expected" ]

seq 1 8 | xargs -I\? file_not_exist ${DUMPLING_OUTPUT_DIR}/${DB_NAME}.${TABLE_NAME}.00000000\?.sql

# Test for specifying --filetype sql with --sql, should report an error
set +e
run_dumpling --sql "select * from \`$DB_NAME\`.\`$TABLE_NAME\`" --filetype sql > ${DUMPLING_OUTPUT_DIR}/dumpling.log
set -e

actual=$(grep -w "unsupported config.FileType 'sql' when we specify --sql, please unset --filetype or set it to 'csv'" ${DUMPLING_OUTPUT_DIR}/dumpling.log|wc -l)
echo "expected 1 return error when specifying --filetype sql and --sql, actual ${actual}"
[ "$actual" = 1 ]

export DUMPLING_TEST_PORT=4000
# Test for --sql option.
run_sql "drop database if exists \`$DB_NAME\`;"
run_sql "create database \`$DB_NAME\` DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin;"
run_sql "create sequence \`$DB_NAME\`.\`$SEQUENCE_NAME\` increment by 1;"

run_dumpling --sql "select nextval(\`$DB_NAME\`.\`$SEQUENCE_NAME\`)"

actual=$(sed -n '2p' ${DUMPLING_OUTPUT_DIR}/result.000000000.csv | sed "s/\r/r/g")
echo "expected 1r, actual ${actual}"
[ "$actual" = "1r" ]

run_dumpling --sql "select nextval(\`$DB_NAME\`.\`$SEQUENCE_NAME\`)"

actual=$(sed -n '2p' ${DUMPLING_OUTPUT_DIR}/result.000000000.csv  | sed "s/\r/r/g")
echo "expected 2r, actual ${actual}"
[ "$actual" = "2r" ]

# Test for dump with sequence
run_dumpling | tee ${DUMPLING_OUTPUT_DIR}/dumpling.log
actual=$(grep -w "dump failed" ${DUMPLING_OUTPUT_DIR}/dumpling.log|wc -l)
echo "expected 0, actual ${actual}"
[ "$actual" = 0 ]

# Test for tidb_mem_quota_query configuration
export GO_FAILPOINTS="github.com/pingcap/tidb/dumpling/export/PrintTiDBMemQuotaQuery=1*return"
run_dumpling | tee ${DUMPLING_OUTPUT_DIR}/dumpling.log
actual=$(grep -w "tidb_mem_quota_query == 1073741824" ${DUMPLING_OUTPUT_DIR}/dumpling.log|wc -l)
echo "expected 1, actual ${actual}"
[ "$actual" = 1 ]

export GO_FAILPOINTS=""

# Test for wrong sql causing panic problem: https://github.com/pingcap/dumpling/pull/234#issuecomment-759996695
set +e
run_dumpling --sql "test" > ${DUMPLING_OUTPUT_DIR}/dumpling.log 2> ${DUMPLING_OUTPUT_DIR}/dumpling.err
set -e

# check stderr, should not contain panic info
actual=$(grep -w "panic" ${DUMPLING_OUTPUT_DIR}/dumpling.err|wc -l)
echo "expected panic 0, actual ${actual}"
[ "$actual" = 0 ]

# check stdout, should contain mysql error log
actual=$(grep -w "Error 1064: You have an error in your SQL syntax" ${DUMPLING_OUTPUT_DIR}/dumpling.log|wc -l)
echo "expect contain Error 1064, actual ${actual}"
[ "$actual" -ge 1 ]
