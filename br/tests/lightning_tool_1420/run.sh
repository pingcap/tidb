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

# This test verifies if TOOL-1420 is fixed.
# It involves column names not in lower-case.

set -eu

run_sql 'DROP DATABASE IF EXISTS `EE1420`;'
run_lightning
run_sql 'SELECT `ROLE_ID` FROM `EE1420`.`pt_role`;'
check_contains 'ROLE_ID: 1'
