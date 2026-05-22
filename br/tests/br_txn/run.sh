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

# disable global ENCRYPTION_ARGS and ENABLE_ENCRYPTION_CHECK for this script
ENCRYPTION_ARGS=""
ENABLE_ENCRYPTION_CHECK=false
export ENCRYPTION_ARGS
export ENABLE_ENCRYPTION_CHECK

set -eux

res_file="$TEST_DIR/sql_res.$TEST_NAME.txt"

# restart service without tiflash
source $UTILS_DIR/run_services
start_services --no-tiflash

BACKUP_DIR=$TEST_DIR/"txn_backup"
BACKUP_FULL=$TEST_DIR/"txnkv-full"

checksum() {
    bin/txnkv --pd $PD_ADDR \
        --ca "$TEST_DIR/certs/ca.pem" \
        --cert "$TEST_DIR/certs/br.pem" \
        --key "$TEST_DIR/certs/br.key" \
        --mode checksum --start-key $1 --end-key $2 | grep result | tail -n 1 | awk '{print $3}'
}

fail_and_exit() {
    echo "TEST: [$TEST_NAME] failed!"
    exit 1
}

clean() {
    bin/txnkv --pd $PD_ADDR \
    --ca "$TEST_DIR/certs/ca.pem" \
    --cert "$TEST_DIR/certs/br.pem" \
    --key "$TEST_DIR/certs/br.key" \
    --mode delete --start-key $1 --end-key $2
}

test_full_txnkv_encryption() {
    check_range_start="xhello"
    check_range_end="xworld"

    rm -rf $BACKUP_FULL

    checksum_full=$(checksum $check_range_start $check_range_end)
    # backup current state of key-values
    run_br --pd $PD_ADDR backup txn -s "local://$BACKUP_FULL" --crypter.method "aes128-ctr" --crypter.key "0123456789abcdef0123456789abcdef"

    clean $check_range_start $check_range_end
    # Ensure the data is deleted
    checksum_new=$(checksum $check_range_start $check_range_end)
    if [ "$checksum_new" == "$checksum_full" ];then
        echo "failed to delete data in range in encryption"
        fail_and_exit
    fi

    run_br --pd $PD_ADDR restore txn -s "local://$BACKUP_FULL" --crypter.method "aes128-ctr" --crypter.key "0123456789abcdef0123456789abcdef"
    checksum_new=$(checksum $check_range_start $check_range_end)
    if [ "$checksum_new" != "$checksum_full" ];then
        echo "failed to restore"
        fail_and_exit
    fi
}

run_test() {
    if [ -z "$1" ];then
        echo "run test"
    else
        export GO_FAILPOINTS="$1"
        echo "run test with failpoints: $GO_FAILPOINTS"
    fi

    rm -rf $BACKUP_DIR
    clean "xhello" "xworld" 

    # generate txn kv randomly in range[start-key, end-key) in 10s
    bin/txnkv --pd $PD_ADDR \
        --ca "$TEST_DIR/certs/ca.pem" \
        --cert "$TEST_DIR/certs/br.pem" \
        --key "$TEST_DIR/certs/br.key" \
        --mode rand-gen --start-key "xhello" --end-key "xworld" --duration 10

    checksum_ori=$(checksum "xhello" "xworld")

    # backup txnkv
    echo "backup start..."
    run_br --pd $PD_ADDR backup txn -s "local://$BACKUP_DIR" 

    # delete data in range[start-key, end-key)
    clean "xhello" "xworld" 
    # Ensure the data is deleted
    retry_cnt=0
    while true; do
        checksum_new=$(checksum "xhello" "xworld")

        if [ "$checksum_new" != "$checksum_empty" ]; then
            echo "failed to delete data in range after backup; retry_cnt = $retry_cnt"
            retry_cnt=$((retry_cnt+1))
            if [ "$retry_cnt" -gt 50 ]; then
                fail_and_exit
            fi
            sleep 1
            continue
        fi

        break
    done

    # failed on restore full
    echo "restore full start..."
    restore_fail=0
    run_br --pd $PD_ADDR restore full -s "local://$BACKUP_DIR" > $res_file 2>&1 || restore_fail=1
    if [ $restore_fail -ne 1 ]; then
        echo 'full restore from txn backup data success'
        exit 1
    fi
    check_contains "restore mode mismatch"

    checksum_new=$(checksum "xhello" "xworld")
    if [ "$checksum_new" != "$checksum_empty" ]; then
        echo "not empty after restore failed"
        fail_and_exit
    fi

    # restore rawkv
    echo "restore start..."
    run_br --pd $PD_ADDR restore txn -s "local://$BACKUP_DIR" 

    checksum_new=$(checksum "xhello" "xworld")

    if [ "$checksum_new" != "$checksum_ori" ];then
        echo "checksum failed after restore"
        fail_and_exit
    fi

    test_full_txnkv_encryption

    # delete data in range[start-key, end-key)
    clean "xhello" "xworld"
    # Ensure the data is deleted
    checksum_new=$(checksum "xhello" "xworld")

    if [ "$checksum_new" != "$checksum_empty" ];then
        echo "failed to delete data in range"
        fail_and_exit
    fi

    export GO_FAILPOINTS=""
}

# delete data in range[start-key, end-key)
clean "xhello" "xworld" 
checksum_empty=$(checksum "xhello" "xworld")
run_test ""
