#!/bin/bash
#
# Copyright 2023 PingCAP, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

set -eux

cur=$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )
. $cur/../_utils/run_services
PD_CONFIG=${PD_CONFIG:-"$cur/../config/pd.toml"}
TIDB_CONFIG=${TIDB_CONFIG:-"$cur/../config/tidb.toml"}

bin/pd-server --join "https://$PD_ADDR" \
  --client-urls "https://${PD_ADDR}2" \
  --peer-urls "https://${PD_PEER_ADDR}2" \
  --log-file "$TEST_DIR/pd2.log" \
  --data-dir "$TEST_DIR/pd2" \
  --name pd2 \
  --config $PD_CONFIG &

# strange that new PD can't join too quickly
sleep 10

bin/pd-server --join "https://$PD_ADDR" \
  --client-urls "https://${PD_ADDR}3" \
  --peer-urls "https://${PD_PEER_ADDR}3" \
  --log-file "$TEST_DIR/pd3.log" \
  --data-dir "$TEST_DIR/pd3" \
  --name pd3 \
  --config $PD_CONFIG &

# restart TiDB to let TiDB load new PD nodes
killall tidb-server
# wait for TiDB to exit to release file lock
sleep 5
start_tidb

export GO_FAILPOINTS='github.com/pingcap/tidb/br/pkg/lightning/importer/beforeRun=sleep(60000)'
run_lightning --backend local --enable-checkpoint=0 &
lightning_pid=$!
# in many libraries, etcd client's auto-sync-interval is 30s, so we need to wait at least 30s before kill PD leader
sleep 45
kill $(cat /tmp/backup_restore_test/pd_pid.txt)

# Check that everything is correctly imported
wait $lightning_pid
run_sql 'SELECT count(*), sum(c) FROM cpeng.a'
check_contains 'count(*): 4'
check_contains 'sum(c): 10'

run_sql 'SELECT count(*), sum(c) FROM cpeng.b'
check_contains 'count(*): 4'
check_contains 'sum(c): 46'

restart_services
