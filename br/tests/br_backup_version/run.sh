#!/bin/bash
#
# Copyright 2021 PingCAP, Inc.
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

set -eu
DB="$TEST_NAME"

# example
#    "cluster_id": 6931331682760961243
expected_cluster_id=`run_curl "https://$PD_ADDR/pd/api/v1/members" | grep "cluster_id"`
# example
#"4.0.10"
expected_cluster_version=`run_curl "https://$PD_ADDR/pd/api/v1/config/cluster-version"`
unset BR_LOG_TO_TERM

function check_version() {
    folder=$1
    expected_br_version=$2
    # FIXME we had strange log here, ignore it temporary
    # [INFO] [data_slow_query.go:144] ["Telemetry slow query stats initialized"] [currentSQBInfo={xxx}]
    br_version=`run_br -s "local://$TEST_DIR/$folder" debug decode --field "BrVersion" | grep -v INFO | grep -v log`
    [[ $br_version =~ $expected_br_version ]]
    cluster_version=`run_br -s "local://$TEST_DIR/$folder" debug decode --field "ClusterVersion" | grep -v INFO | grep -v log`
    [[ $cluster_version =~ $expected_cluster_version ]]
    cluster_id=`run_br -s "local://$TEST_DIR/$folder" debug decode --field "ClusterId" | grep -v INFO | grep -v log | sed -n -e '1p'`
    [[ $expected_cluster_id =~ $cluster_id ]]
}

# backup empty using BR
echo "backup start..."
run_br --pd $PD_ADDR backup full -s "local://$TEST_DIR/br_version_1"
if [ $? -ne 0 ]; then
    echo "TEST: [$TEST_NAME] failed on backup empty cluster version!"
    exit 1
fi

check_version "br_version_1" "BR"

# backup empty using BR via SQL
echo "backup start..."
run_sql "BACKUP DATABASE $DB TO \"local://$TEST_DIR/br_version_2\""

# FIXME: uncomment this after TiDB updates this BR dependency
# check_version "br_version_2" "TiDB"

# create a database and insert some data
run_sql "CREATE DATABASE $DB;"
run_sql "CREATE TABLE $DB.usertable1 ( \
  YCSB_KEY varchar(64) NOT NULL, \
  FIELD0 varchar(1) DEFAULT NULL, \
  PRIMARY KEY (YCSB_KEY) \
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin;"
# insert one row to make sure table is restored.
run_sql "INSERT INTO $DB.usertable1 VALUES (\"a\", \"b\");"

# backup tables using BR
echo "backup start..."
run_br --pd $PD_ADDR backup full -s "local://$TEST_DIR/br_version_3"
if [ $? -ne 0 ]; then
    echo "TEST: [$TEST_NAME] failed on backup empty cluster version!"
    exit 1
fi

check_version "br_version_3" "BR"

# backup tables using BR via SQL
echo "backup start..."
run_sql "BACKUP DATABASE $DB TO \"local://$TEST_DIR/br_version_4\""

# FIXME: uncomment this after TiDB updates this BR dependency
# check_version "br_version_4" "TiDB"

run_sql "DROP DATABASE $DB"
echo "TEST: [$TEST_NAME] successed!"
