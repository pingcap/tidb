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

res_file="$TEST_DIR/sql_res.$TEST_NAME.txt"

run_br backup full -h > $res_file 2>&1
check_not_contains "--type"
check_not_contains "--volume-file"
check_not_contains "--skip-aws"
check_not_contains "--cloud-api-concurrency"
check_not_contains "--progress-file"

run_br backup full --type xxx > $res_file 2>&1 || true
check_contains "invalid full backup type"

# should success
run_br backup full --type kv --pd $PD_ADDR -s "local://$TEST_DIR/kv"

# cannot test backup ebs since we need specified tikv version to pause schedule completely.
# todo: add it later
# run_br backup full --type aws-ebs --skip-aws --pd $PD_ADDR -s "local://$TEST_DIR/ebs" --volume-file tests/$TEST_NAME/volume.json

run_br restore full -h > $res_file 2>&1
check_not_contains "--type"
check_not_contains "--prepare"
check_not_contains "--output-file"
check_not_contains "--skip-aws"
check_not_contains "--cloud-api-concurrency"
check_not_contains "--volume-type"
check_not_contains "--volume-iops"
check_not_contains "--volume-throughput"
check_not_contains "--progress-file"

run_br restore full --type xxx > $res_file 2>&1 || true
check_contains "invalid full backup type"

# todo: cannot run this test now, we need specified pd version.
# run_br restore full --meta-phase --skip-aws --pd $PD_ADDR -s "local://$(cd .; pwd)/tests/$TEST_NAME/"

