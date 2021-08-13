#!/bin/sh
#
# Copyright 2020 PingCAP, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# See the License for the specific language governing permissions and
# limitations under the License.

set -eux

run_sql 'DROP DATABASE IF EXISTS issue519;'
run_lightning --backend tidb

run_sql "SELECT b FROM issue519.t WHERE a = '''';"
check_contains 'b: "'
# following use hex to avoid the escaping mess. 22 = `"`, 27 = `'`.
run_sql 'SELECT hex(a) FROM issue519.t WHERE b = 0x222722272727272722;'
check_contains 'hex(a): 2227272727222722'
