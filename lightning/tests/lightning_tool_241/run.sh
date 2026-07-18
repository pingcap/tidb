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

# This test verifies if TOOL-200 and TOOL-241 are all fixed.
# They all involve data source with lots of empty tables.

set -eu

run_sql 'DROP DATABASE IF EXISTS qyjc;'
run_lightning
echo 'Import finished'

# Verify all data are imported
for table_name in \
    q_alarm_group \
    q_alarm_message_log \
    q_alarm_receiver \
    q_config \
    q_report_circular_data \
    q_report_desc \
    q_report_summary \
    q_system_update \
    q_user_log
do
    run_sql "SELECT count(*) FROM qyjc.$table_name;"
    check_contains 'count(*): 0'
done

# ensure the non-empty table is not affected
run_sql 'SELECT count(id), min(id), max(id) FROM qyjc.q_fish_event;'
check_contains 'count(id): 84'
check_contains 'min(id): 8343146'
check_contains 'max(id): 8343229'
