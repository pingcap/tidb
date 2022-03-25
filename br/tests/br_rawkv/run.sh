#!/bin/sh
#
# Copyright 2019 PingCAP, Inc.
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
source $( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )/../_utils/run_services
start_services --no-tiflash

BACKUP_DIR=$TEST_DIR/"raw_backup"
BACKUP_FULL=$TEST_DIR/"rawkv-full"

checksum() {
    bin/rawkv --pd $PD_ADDR \
        --ca "$TEST_DIR/certs/ca.pem" \
        --cert "$TEST_DIR/certs/br.pem" \
        --key "$TEST_DIR/certs/br.key" \
        --mode checksum --start-key $1 --end-key $2 | grep result | awk '{print $3}'
}

fail_and_exit() {
    echo "TEST: [$TEST_NAME] failed!"
    exit 1
}

clean() {
    bin/rawkv --pd $PD_ADDR \
    --ca "$TEST_DIR/certs/ca.pem" \
    --cert "$TEST_DIR/certs/br.pem" \
    --key "$TEST_DIR/certs/br.key" \
    --mode delete --start-key $1 --end-key $2
}

test_full_rawkv() {
    check_range_start=00
    check_range_end=ff

    rm -rf $BACKUP_FULL

    checksum_full=$(checksum $check_range_start $check_range_end)
    # backup current state of key-values
    # raw backup is not working with range [nil, nil]. TODO: fix it.
    run_br --pd $PD_ADDR backup raw -s "local://$BACKUP_FULL" --crypter.method "aes128-ctr" --crypter.key "0123456789abcdef0123456789abcdef" --start $check_range_start --format hex

    clean $check_range_start $check_range_end
    # Ensure the data is deleted
    checksum_new=$(checksum $check_range_start $check_range_end)
    if [ "$checksum_new" == "$checksum_full" ];then
        echo "failed to delete data in range"
        fail_and_exit
    fi

    run_br --pd $PD_ADDR restore raw -s "local://$BACKUP_FULL" --crypter.method "aes128-ctr" --crypter.key "0123456789abcdef0123456789abcdef" --start $check_range_start --format hex
    checksum_new=$(checksum $check_range_start $check_range_end)
    if [ "$checksum_new" != "$checksum_full" ];then
        echo "failed to restore"
        fail_and_exit
    fi
}

checksum_empty=$(checksum 31 3130303030303030)

run_test() {
    if [ -z "$1" ];then
        echo "run test"
    else
        export GO_FAILPOINTS="$1"
        echo "run test with failpoints: $GO_FAILPOINTS"
    fi

    rm -rf $BACKUP_DIR
    clean 31 3130303030303030

    # generate raw kv randomly in range[start-key, end-key) in 10s
    bin/rawkv --pd $PD_ADDR \
        --ca "$TEST_DIR/certs/ca.pem" \
        --cert "$TEST_DIR/certs/br.pem" \
        --key "$TEST_DIR/certs/br.key" \
        --mode rand-gen --start-key 31 --end-key 3130303030303030 --duration 10

    # put some keys around 311122 to check the correctness of endKey of restoring
    bin/rawkv --pd $PD_ADDR \
        --ca "$TEST_DIR/certs/ca.pem" \
        --cert "$TEST_DIR/certs/br.pem" \
        --key "$TEST_DIR/certs/br.key" \
        --mode put --put-data "311121:31, 31112100:32, 311122:33, 31112200:34, 3111220000:35, 311123:36"

    checksum_ori=$(checksum 31 3130303030303030)
    checksum_partial=$(checksum 311111 311122)

    # backup rawkv
    echo "backup start..."
    run_br --pd $PD_ADDR backup raw -s "local://$BACKUP_DIR" --start 31 --end 3130303030303030 --format hex --concurrency 4 --crypter.method "aes128-ctr" --crypter.key "0123456789abcdef0123456789abcdef"

    # delete data in range[start-key, end-key)
    clean 31 3130303030303030
    # Ensure the data is deleted
    checksum_new=$(checksum 31 3130303030303030)

    if [ "$checksum_new" != "$checksum_empty" ];then
        echo "failed to delete data in range"
        fail_and_exit
    fi

    # restore rawkv
    echo "restore start..."
    run_br --pd $PD_ADDR restore raw -s "local://$BACKUP_DIR" --start 31 --end 3130303030303030 --format hex --crypter.method "aes128-ctr" --crypter.key "0123456789abcdef0123456789abcdef"

    checksum_new=$(checksum 31 3130303030303030)

    if [ "$checksum_new" != "$checksum_ori" ];then
        echo "checksum failed after restore"
        fail_and_exit
    fi

    test_full_rawkv

    # delete data in range[start-key, end-key)
    clean 31 3130303030303030
    # Ensure the data is deleted
    checksum_new=$(checksum 31 3130303030303030)

    if [ "$checksum_new" != "$checksum_empty" ];then
        echo "failed to delete data in range"
        fail_and_exit
    fi

    echo "partial restore start..."
    run_br --pd $PD_ADDR restore raw -s "local://$BACKUP_DIR" --start 311111 --end 311122 --format hex --concurrency 4 --crypter.method "aes128-ctr" --crypter.key "0123456789abcdef0123456789abcdef"
    bin/rawkv --pd $PD_ADDR \
        --ca "$TEST_DIR/certs/ca.pem" \
        --cert "$TEST_DIR/certs/br.pem" \
        --key "$TEST_DIR/certs/br.key" \
        --mode scan --start-key 311121 --end-key 33

    checksum_new=$(checksum 31 3130303030303030)

    if [ "$checksum_new" != "$checksum_partial" ];then
        echo "checksum failed after restore"
        fail_and_exit
    fi

    export GO_FAILPOINTS=""
}


run_test ""

# ingest "region error" to trigger fineGrainedBackup
run_test "github.com/pingcap/tidb/br/pkg/backup/tikv-region-error=return(\"region error\")"
