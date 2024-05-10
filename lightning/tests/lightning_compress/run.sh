#!/bin/sh
#
# Copyright 2022 PingCAP, Inc.
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

CUR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)

for BACKEND in tidb local; do
  for compress in gzip snappy zstd; do
    if [ "$BACKEND" = 'local' ]; then
      check_cluster_version 4 0 0 'local backend' || continue
    fi

    # Set minDeliverBytes to a small enough number to only write only 1 row each time
    # Set the failpoint to kill the lightning instance as soon as one row is written
    PKG="github.com/pingcap/tidb/lightning/pkg/importer"
    export GO_FAILPOINTS="$PKG/SlowDownWriteRows=sleep(1000);$PKG/FailAfterWriteRows=panic;$PKG/SetMinDeliverBytes=return(1)"

    # Start importing the tables.
    run_sql 'DROP DATABASE IF EXISTS compress'
    run_sql 'DROP DATABASE IF EXISTS tidb_lightning_checkpoint_test'
    set +e
    run_lightning --backend $BACKEND --config "$CUR/$BACKEND-config.toml" -d "$CUR/data.$compress" --enable-checkpoint=1 2> /dev/null
    set -e

    # restart lightning from checkpoint, the second line should be written successfully
    export GO_FAILPOINTS=
    set +e
    run_lightning --backend $BACKEND --config "$CUR/$BACKEND-config.toml" -d "$CUR/data.$compress" --enable-checkpoint=1 2> /dev/null
    set -e

    run_sql 'SELECT count(*), sum(PROCESSLIST_TIME), sum(THREAD_OS_ID), count(PROCESSLIST_STATE) FROM compress.threads'
    check_contains 'count(*): 43'
    check_contains 'sum(PROCESSLIST_TIME): 322253'
    check_contains 'sum(THREAD_OS_ID): 303775702'
    check_contains 'count(PROCESSLIST_STATE): 3'

    run_sql 'SELECT count(*) FROM compress.threads WHERE PROCESSLIST_TIME IS NOT NULL'
    check_contains 'count(*): 12'

    run_sql 'SELECT count(*) FROM compress.multi_rows WHERE a="aaaaaaaaaa"'
    check_contains 'count(*): 100000'

    run_sql 'SELECT hex(t), j, hex(b) FROM compress.escapes WHERE i = 1'
    check_contains 'hex(t): 5C'
    check_contains 'j: {"?": []}'
    check_contains 'hex(b): FFFFFFFF'

    run_sql 'SELECT hex(t), j, hex(b) FROM compress.escapes WHERE i = 2'
    check_contains 'hex(t): 22'
    check_contains 'j: "\n\n\n"'
    check_contains 'hex(b): 0D0A0D0A'

    run_sql 'SELECT hex(t), j, hex(b) FROM compress.escapes WHERE i = 3'
    check_contains 'hex(t): 0A'
    check_contains 'j: [",,,"]'
    check_contains 'hex(b): 5C2C5C2C'

    run_sql 'SELECT id FROM compress.empty_strings WHERE a = """"'
    check_contains 'id: 3'
    run_sql 'SELECT id FROM compress.empty_strings WHERE b <> ""'
    check_not_contains 'id:'
  done
done
