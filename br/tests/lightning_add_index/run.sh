#!/bin/bash
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

set -eu

LOG_FILE1="$TEST_DIR/lightning-add-index1.log"
LOG_FILE2="$TEST_DIR/lightning-add-index2.log"

run_lightning --config "tests/$TEST_NAME/config1.toml" --log-file "$LOG_FILE1"
run_sql "ADMIN CHECKSUM TABLE add_index.multi_indexes;"
run_sql "SHOW CREATE TABLE add_index.multi_indexes;"

run_sql "DROP DATABASE add_index;"
run_lightning --config "tests/$TEST_NAME/config2.toml" --log-file "$LOG_FILE2"
run_sql "ADMIN CHECKSUM TABLE add_index.multi_indexes;"
run_sql "SHOW CREATE TABLE add_index.multi_indexes;"
