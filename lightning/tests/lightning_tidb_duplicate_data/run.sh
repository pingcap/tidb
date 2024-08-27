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

set -eu

CUR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)

# reset substitution if last time failed half way
# on BSD/macOS sed -i must have a following string as backup filename extension
sed -i.bak 's/new/old/g' "$CUR/data/dup.dup.sql" && rm $CUR/data/dup.dup.sql.bak

for type in replace ignore error; do
    run_sql 'DROP DATABASE IF EXISTS dup;'

    export GO_FAILPOINTS="github.com/pingcap/tidb/pkg/lightning/backend/tidb/FailIfImportedSomeRows=return"
    set +e
    run_lightning --config "$CUR/$type.toml" 2> /dev/null
    ERRORCODE=$?
    set -e
    [ "$ERRORCODE" -ne 0 ]

    # backup original sql to dup.dup.sql.bak
    sed -i.bak 's/old/new/g' "$CUR/data/dup.dup.sql"

    unset GO_FAILPOINTS

    if [ $type = 'error' ]; then
        set +e
        run_lightning --config "$CUR/$type.toml" --log-file "$TEST_DIR/lightning-error-on-dup.log"
        ERRORCODE=$?
        set -e
        [ "$ERRORCODE" -ne 0 ]
        tail -20 "$TEST_DIR/lightning-error-on-dup.log" > "$TEST_DIR/lightning-error-on-dup.tail"
        grep -Fq 'Duplicate entry' "$TEST_DIR/lightning-error-on-dup.tail"
    elif [ $type = 'replace' ]; then
        run_lightning --config "$CUR/$type.toml"
        run_sql 'SELECT count(*) FROM dup.dup'
        check_contains 'count(*): 2'
        run_sql 'SELECT d FROM dup.dup WHERE pk = 1'
        check_contains 'd: new'
        run_sql 'SELECT d FROM dup.dup WHERE pk = 2'
        check_contains 'd: new'
    elif [ $type = 'ignore' ]; then
        run_lightning --config "$CUR/$type.toml"
        run_sql 'SELECT count(*) FROM dup.dup'
        check_contains 'count(*): 2'
        run_sql 'SELECT d FROM dup.dup WHERE pk = 1'
        check_contains 'd: old'
        run_sql 'SELECT d FROM dup.dup WHERE pk = 2'
        check_contains 'd: new'
    fi

    mv $CUR/data/dup.dup.sql.bak $CUR/data/dup.dup.sql
done
