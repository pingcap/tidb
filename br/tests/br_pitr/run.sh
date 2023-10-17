#!/bin/bash
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
CUR=$(cd `dirname $0`; pwd)
DB="$TEST_NAME"

# const value
WAIT_DONE_CODE=50
WAIT_NOT_DONE_CODE=51

# prepare the data
echo "prepare the data"
# ...

# start the log backup task
echo "start log task"
run_br --pd $PD_ADDR log start --task integration_test -s "local://$TEST_DIR/$TEST_NAME/log"

# run snapshot backup
echo "run snapshot backup"
run_br --pd $PD_ADDR backup full -s "local://$TEST_DIR/$TEST_NAME/full"

# load the incremental data
echo "load the incremental data"
run_sql_file $CUR/data/delete_range.sql
# ...

# wait checkpoint advance
echo "wait checkpoint advance"
OLD_GO_FAILPOINTS=$GO_FAILPOINTS
sleep 10
now_time=$(date "+%Y-%m-%d %H:%M:%S %z")
echo "get the current time: $now_time"
export GO_FAILPOINTS="github.com/pingcap/tidb/br/pkg/stream/only-checkpoint-ts-with-check=return(\"$now_time\")"
while true; do
    # run br with failpoint to compare the current checkpoint ts with the current time
    run_br --pd $PD_ADDR log status --task-name integration_test
    exit_code=$?
    echo "exit code: $exit_code"
    
    # the checkpoint has advanced
    if [ $exit_code -eq $WAIT_DONE_CODE ]; then
        break
    fi

    # the checkpoint hasn't advanced
    if [ $exit_code -eq $WAIT_NOT_DONE_CODE ]; then
        sleep 10
        continue
    fi

    # unknown status, maybe somewhere is wrong
    echo "TEST: [$TEST_NAME] failed to wait checkpoint advance!"
    exit 1
done
export GO_FAILPOINTS=$OLD_GO_FAILPOINTS

# dump some info from upstream cluster
# ...

# start a new cluster
echo "stop services"
stop_services
echo "clean up data"
rm -rf $TEST_DIR && mkdir -p $TEST_DIR
echo "start services"
start_services

# PITR restore
run_br --pd $PD_ADDR restore point -s "local://$TEST_DIR/$TEST_NAME/log" --full-backup-storage "local://$TEST_DIR/$TEST_NAME/full"

# check something in downstream cluster
# ...
