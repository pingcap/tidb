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

set -eux

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
    check_range_start="hello"
    check_range_end="world"

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
    clean "hello" "world" 

    # generate txn kv randomly in range[start-key, end-key) in 10s
    bin/txnkv --pd $PD_ADDR \
        --ca "$TEST_DIR/certs/ca.pem" \
        --cert "$TEST_DIR/certs/br.pem" \
        --key "$TEST_DIR/certs/br.key" \
        --mode rand-gen --start-key "hello" --end-key "world" --duration 10

    checksum_ori=$(checksum "hello" "world")

    # backup txnkv
    echo "backup start..."
    run_br --pd $PD_ADDR backup txn -s "local://$BACKUP_DIR" 

    # delete data in range[start-key, end-key)
    clean "hello" "world" 
    # Ensure the data is deleted
    checksum_new=$(checksum "hello" "world")

    if [ "$checksum_new" != "$checksum_empty" ];then
        echo "failed to delete data in range after backup"
        fail_and_exit
    fi

    # restore rawkv
    echo "restore start..."
    run_br --pd $PD_ADDR restore txn -s "local://$BACKUP_DIR" 

    checksum_new=$(checksum "hello" "world")

    if [ "$checksum_new" != "$checksum_ori" ];then
        echo "checksum failed after restore"
        fail_and_exit
    fi

    test_full_txnkv_encryption

    # delete data in range[start-key, end-key)
    clean "hello" "world"
    # Ensure the data is deleted
    checksum_new=$(checksum "hello" "world")

    if [ "$checksum_new" != "$checksum_empty" ];then
        echo "failed to delete data in range"
        fail_and_exit
    fi

    export GO_FAILPOINTS=""
}

# delete data in range[start-key, end-key)
clean "hello" "world" 
checksum_empty=$(checksum "hello" "world")
run_test ""
