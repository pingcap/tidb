#!/bin/sh
#
# Copyright 2024 PingCAP, Inc.
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

wait_log_checkpoint_advance() {
    local task_name=${1:-$TASK_NAME}
    echo "wait for log checkpoint to advance for task: $task_name"
    sleep 10
    local current_ts=$(python3 -c "import time; print(int(time.time() * 1000) << 18)")
    echo "current ts: $current_ts"
    i=0
    while true; do
        # extract the checkpoint ts of the log backup task. If there is some error, the checkpoint ts should be empty
        log_backup_status=$(unset BR_LOG_TO_TERM && run_br --skip-goleak --pd $PD_ADDR log status --task-name $task_name --json 2>br.log)
        echo "log backup status: $log_backup_status"
        local checkpoint_ts=$(echo "$log_backup_status" | head -n 1 | jq 'if .[0].last_errors | length  == 0 then .[0].checkpoint else empty end')
        echo "checkpoint ts: $checkpoint_ts"

        # check whether the checkpoint ts is a number
        if [ $checkpoint_ts -gt 0 ] 2>/dev/null; then
            if [ $checkpoint_ts -gt $current_ts ]; then
                echo "the checkpoint has advanced"
                break
            fi
            echo "the checkpoint hasn't advanced"
            i=$((i+1))
            if [ "$i" -gt 50 ]; then
                echo 'the checkpoint lag is too large'
                exit 1
            fi
            sleep 10
        else
            echo "TEST: [$TEST_NAME] failed to wait checkpoint advance!"
            exit 1
        fi
    done
}
