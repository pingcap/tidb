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
DB="$TEST_NAME"
TABLE="duplicate"
DB_COUNT=3
CUR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)

for i in $(seq $DB_COUNT); do
    run_sql "CREATE DATABASE $DB${i};"
    run_sql "CREATE TABLE $DB${i}.$TABLE (id INT PRIMARY KEY, name VARCHAR(64));"
    run_sql "INSERT INTO $DB${i}.$TABLE VALUES (1, 'hello'), (2, 'world');"
done

# backup full
echo "backup start..."
run_br --pd $PD_ADDR backup full -s "local://$TEST_DIR/$DB" --concurrency 4


# restore full
set +e
output=$(run_sql "restore database * from 'local://$TEST_DIR/$DB'" 2>&1)
exit_status=$?
set -e

if [ "$exit_status" -eq 0 ]; then
    echo "TEST: [$TEST_NAME] failed!"
    exit 1
fi

if ! echo "$output" | grep -q "tables already existed"; then
    echo "TEST: [$TEST_NAME] failed!"
    exit 1
fi


for i in $(seq $DB_COUNT); do
    run_sql "DROP DATABASE $DB${i};"
done
