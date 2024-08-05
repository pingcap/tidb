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

check_cluster_version 4 0 0 'local backend' || exit 0

run_sql 'DROP DATABASE IF EXISTS disk_quota_checkpoint;'
run_sql 'DROP DATABASE IF EXISTS tidb_lightning_checkpoint;'
rm -rf "$TEST_DIR/$TEST_NAME.sorted"

# force crash when anything is written...
export GO_FAILPOINTS='github.com/pingcap/tidb/lightning/pkg/importer/FailAfterWriteRows=panic'

# ensure only 2 engines (index + one data) are open all the time no matter
# how many times we restart from beginning, since nothing was written.
for i in $(seq 5); do
    set +e
    run_lightning --enable-checkpoint=1 2> /dev/null
    [ $? -ne 0 ] || exit 1
    set -e
    # engine sorted kv dir name is 36 length (UUID4).
    [ $(ls -1q "$TEST_DIR/$TEST_NAME.sorted" | grep -E "^\S{36}$" |  wc -l) -eq 2 ]
    # load all engines into tmp file (will repeat)
    ls -1q "$TEST_DIR/$TEST_NAME.sorted" | grep -E "^\S{36}$" >> $TEST_DIR/$TEST_NAME.sorted/engines_name
done

# allow one file to be written at a time,
export GO_FAILPOINTS='github.com/pingcap/tidb/lightning/pkg/importer/FailAfterWriteRows=1*return->panic'

# and now we should have 3 engines since one engine will be successfully imported.
set +e
run_lightning --enable-checkpoint=1 2> /dev/null
[ $? -ne 0 ] || exit 1
set -e
# engine sorted kv dir name is 36 length (UUID4).
ls -1q "$TEST_DIR/$TEST_NAME.sorted" | grep -E "^\S{36}$" >> $TEST_DIR/$TEST_NAME.sorted/engines_name
if [ ! $(cat $TEST_DIR/$TEST_NAME.sorted/engines_name | sort -n | uniq | wc -l) -eq 3 ]; then
  ls -al "$TEST_DIR/$TEST_NAME.sorted"
  exit 1
fi

# allow everything to be written,
export GO_FAILPOINTS=''
# to import everything,
run_lightning --enable-checkpoint=1
# simple check.
run_sql 'select concat(a, b) from disk_quota_checkpoint.t;'
check_contains '1one'
check_contains '5five'
check_contains '9nine'
