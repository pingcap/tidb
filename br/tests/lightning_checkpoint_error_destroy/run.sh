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

# Make sure we won't run out of table concurrency by destroying checkpoints

for _ in $(seq 8); do
    ARGS="--enable-checkpoint=1 --config tests/$TEST_NAME/mysql.toml -d tests/$TEST_NAME/bad-data"
    set +e
    run_lightning $ARGS
    set -e
    run_lightning_ctl $ARGS -checkpoint-error-destroy=all
done

run_lightning --enable-checkpoint=1 --config "tests/$TEST_NAME/mysql.toml" -d "tests/$TEST_NAME/good-data"
run_sql 'SELECT * FROM cped.t'
check_contains 'x: 1999-09-09 09:09:09'

# Try again with the file checkpoints

run_sql 'DROP DATABASE cped'

# clean up possible old files
rm -rf '/tmp/cp_error_destroy.pb'
for _ in $(seq 8); do
    ARGS="--enable-checkpoint=1 --config tests/$TEST_NAME/file.toml -d tests/$TEST_NAME/bad-data"
    set +e
    run_lightning $ARGS
    set -e
    run_lightning_ctl $ARGS -checkpoint-error-destroy=all
done

run_lightning --enable-checkpoint=1 --config "tests/$TEST_NAME/file.toml" -d "tests/$TEST_NAME/good-data"
run_sql 'SELECT * FROM cped.t'
check_contains 'x: 1999-09-09 09:09:09'
