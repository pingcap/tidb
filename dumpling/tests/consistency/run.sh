#!/bin/sh
#
# Copyright 2020 PingCAP, Inc. Licensed under Apache-2.0.

set -eu
cur=$(cd `dirname $0`; pwd)

DB_NAME="mysql_consistency"
TABLE_NAME="t"

# drop database on mysql
run_sql "drop database if exists \`$DB_NAME\`;"

# build data on mysql
run_sql "create database $DB_NAME;"
run_sql "create table $DB_NAME.$TABLE_NAME (a int(255));"

# insert 100 records
run_sql "insert into $DB_NAME.$TABLE_NAME values $(seq -s, 100 | sed 's/,*$//g' | sed "s/[0-9]*/('1')/g");"

# dumping with consistency flush
export DUMPLING_TEST_DATABASE=$DB_NAME
export GO_FAILPOINTS="github.com/pingcap/dumpling/v4/export/ConsistencyCheck=1*sleep(5000)"
run_dumpling &
# wait dumpling process to start to sleep
sleep 2

# record metadata info
metadata=`run_sql "show master status;"`
metaLog=`echo $metadata | awk -F 'File:' '{print $2}' | awk '{print $1}'`
metaPos=`echo $metadata | awk -F 'Position:' '{print $2}' | awk '{print $1}'`
metaGTID=`echo $metadata | awk -F 'Executed_Gtid_Set:' '{print $2}' | awk '{print $1}'`
# insert 100 more records, test whether dumpling will dump these data out
run_sql "insert into $DB_NAME.$TABLE_NAME values $(seq -s, 100 | sed 's/,*$//g' | sed "s/[0-9]*/('1')/g");"

wait

# check data record count
cnt=`grep -o "(1)" ${DUMPLING_OUTPUT_DIR}/${DB_NAME}.${TABLE_NAME}.000000000.sql|wc -l`
echo "1st records count is ${cnt}"
[ $cnt = 100 ]

# check metadata
echo "metaLog: $metaLog"
echo "metaPos: $metaPos"
echo "metaGTID: $metaGTID"
if [ $metaLog != "" ]; then
[ `grep -o "Log: $metaLog" ${DUMPLING_OUTPUT_DIR}/metadata|wc -l`  ]
fi
if [ $metaPos != "" ]; then
[ `grep -o "Pos: $metaPos" ${DUMPLING_OUTPUT_DIR}/metadata|wc -l`  ]
fi
if [ $metaGTID != "" ]; then
[ `grep -o "GTID: $metaGTID" ${DUMPLING_OUTPUT_DIR}/metadata|wc -l`  ]
fi

# test dumpling normally
export GO_FAILPOINTS=""
run_dumpling
cnt=`grep -o "(1)" ${DUMPLING_OUTPUT_DIR}/${DB_NAME}.${TABLE_NAME}.000000000.sql|wc -l`
echo "2nd records count is ${cnt}"
[ $cnt = 200 ]
