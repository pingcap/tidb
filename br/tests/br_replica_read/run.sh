#!/bin/sh
#
# Copyright 2023 PingCAP, Inc.
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

set -eu
DB="$TEST_NAME"

VOTER_COUNT=$((TIKV_COUNT-1))
if [ "$VOTER_COUNT" -lt "1" ];then
  echo "Skip test because there is no enough tikv"
  exit 0
fi

# set random store to read only
random_store_id=$(run_pd_ctl -u https://$PD_ADDR store | jq 'first(.stores[]|select(.store.labels|(.!= null and any(.key == "engine" and .value=="tiflash"))| not)|.store.id)')
echo "random store id: $random_store_id"
run_pd_ctl -u https://$PD_ADDR store label $random_store_id '$mode' 'read_only'

# set placement rule to add a learner replica for each region in the read only store
run_pd_ctl -u https://$PD_ADDR config placement-rules rule-bundle load --out=$TEST_DIR/default_rules.json
cat tests/br_replica_read/placement_rule_with_learner_template.json | jq  ".[].rules[0].count = $VOTER_COUNT" > $TEST_DIR/placement_rule_with_learner.json
run_pd_ctl -u https://$PD_ADDR config placement-rules rule-bundle save --in $TEST_DIR/placement_rule_with_learner.json
sleep 3 # wait for PD to apply the placement rule

run_sql "CREATE DATABASE $DB;"

run_sql "CREATE TABLE $DB.usertable1 ( \
  YCSB_KEY varchar(64) NOT NULL, \
  FIELD0 varchar(1) DEFAULT NULL, \
  PRIMARY KEY (YCSB_KEY) \
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin;"

run_sql "INSERT INTO $DB.usertable1 VALUES (\"a\", \"b\");"
run_sql "INSERT INTO $DB.usertable1 VALUES (\"aa\", \"b\");"

run_sql "CREATE TABLE $DB.usertable2 ( \
  YCSB_KEY varchar(64) NOT NULL, \
  FIELD0 varchar(1) DEFAULT NULL, \
  PRIMARY KEY (YCSB_KEY) \
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin;"

run_sql "INSERT INTO $DB.usertable2 VALUES (\"c\", \"d\");"

# backup db
echo "backup start..."
run_br -u https://$PD_ADDR backup db --db "$DB" -s "local://$TEST_DIR/$DB" --replica-read-label '$mode:read_only'

run_sql "DROP DATABASE $DB;"

# restore db
echo "restore start..."
run_br restore db --db $DB -s "local://$TEST_DIR/$DB" -u https://$PD_ADDR

run_sql "select count(*) from $DB.usertable1;"
table1_count=$(read_result)
echo "table1 count: $table1_count"
if [ "$table1_count" -ne "2" ];then
    echo "TEST: [$TEST_NAME] failed!"
    exit 1
fi

run_sql "select count(*) from $DB.usertable2;"
table2_count=$(read_result)
echo "table2 count: $table2_count"
if [ "$table2_count" -ne "1" ];then
    echo "TEST: [$TEST_NAME] failed!"
    exit 1
fi

# Test BR DDL query string
echo "testing DDL query..."
run_curl https://$TIDB_STATUS_ADDR/ddl/history | grep -E '/\*from\(br\)\*/CREATE TABLE'
run_curl https://$TIDB_STATUS_ADDR/ddl/history | grep -E '/\*from\(br\)\*/CREATE DATABASE'

run_sql "DROP DATABASE $DB;"
run_pd_ctl -u https://$PD_ADDR store label $random_store_id '$mode' ''
run_pd_ctl -u https://$PD_ADDR config placement-rules rule-bundle save --in $TEST_DIR/default_rules.json