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

# Verify that using various uncommon types as primary key still works properly.

set -eu

for BACKEND in tidb local; do
  if [ "$BACKEND" = 'local' ]; then
    check_cluster_version 4 0 0 'local backend' || continue
  fi

  run_sql 'DROP DATABASE IF EXISTS vt;'
  run_lightning --backend $BACKEND
  echo Import using $BACKEND finished

  run_sql 'SELECT count(pk), bin(min(pk)), bin(max(pk)) FROM vt.bit'
  check_contains 'count(pk): 16'
  check_contains 'bin(min(pk)): 0'
  check_contains 'bin(max(pk)): 11'
  run_sql 'SELECT sum(ref) FROM vt.bit WHERE pk = 0b10'
  check_contains 'sum(ref): 82'

  run_sql 'SELECT count(pk), min(pk), max(pk), sum(pk) FROM vt.decimal'
  check_contains 'count(pk): 50'
  check_contains 'min(pk): -99.9990'
  check_contains 'max(pk): 99.9912'
  check_contains 'sum(pk): -9.9123'
  run_sql 'SELECT ref FROM vt.decimal WHERE pk BETWEEN -1.0 AND 0.0'
  check_contains 'ref: 22'

  run_sql 'SELECT count(pk), min(pk), max(pk) FROM vt.double'
  check_contains 'count(pk): 41'
  check_contains 'min(pk): 9.85967654375977e-305'
  check_contains 'max(pk): 1.0142320547350045e304'
  run_sql 'SELECT ref FROM vt.double WHERE pk BETWEEN 1e100 AND 1e120'
  check_contains 'ref: 245'

  run_sql 'SELECT count(pk), min(pk), max(pk), count(uk), min(uk), max(uk) FROM vt.datetime'
  check_contains 'count(pk): 70'
  check_contains 'min(pk): 1026-09-21 15:15:54.335745'
  check_contains 'max(pk): 9889-01-08 08:51:03.389832'
  check_contains 'count(uk): 70'
  check_contains 'min(uk): 1970-11-09 19:25:45.843'
  check_contains 'max(uk): 2036-10-14 10:48:28.620'
  run_sql "SELECT ref FROM vt.datetime WHERE pk BETWEEN '2882-01-01' AND '2882-12-31'"
  check_contains 'ref: 7'
  run_sql "SELECT ref FROM vt.datetime WHERE uk BETWEEN '2001-01-01' AND '2001-12-31'"
  check_contains 'ref: 32'

  run_sql 'SELECT count(pk), min(pk), max(pk) FROM vt.char'
  check_contains 'count(pk): 50'
  check_contains 'min(pk): 090abbb2-f22e-4f97-a4fe-a52eb1a80a0b'
  check_contains 'max(pk): fde1328c-409c-43a8-b1b0-8c35c8000f92'
  run_sql "SELECT ref FROM vt.char WHERE pk = '55dc0343-db6a-4208-9872-9096305b8c07'"
  check_contains 'ref: 41'

  run_sql 'SELECT count(pk), hex(min(pk)), hex(max(pk)) FROM vt.binary'
  check_contains 'count(pk): 51'
  check_contains 'hex(min(pk)): '
  check_contains 'hex(max(pk)): FDE1328C409C43A8B1B08C35C8000F92'
  run_sql "SELECT ref FROM vt.binary WHERE pk = x'55dc0343db6a420898729096305b8c07'"
  check_contains 'ref: 41'

  run_sql 'SELECT count(pk), count(DISTINCT js) FROM vt.json'
  check_contains 'count(pk): 92'
  check_contains 'count(DISTINCT js): 92'
  run_sql 'SELECT pk FROM vt.json WHERE js = json_array(1, 2, 3)'
  check_contains 'pk: 1089'
  run_sql 'SELECT js FROM vt.json WHERE pk = 2000'
  check_contains 'js: {'
  check_contains '"52": 1'
  check_contains '"54": 1'
  check_contains '"68": 1'
  check_contains '"126": 1'

  run_sql 'SELECT count(*) FROM vt.`enum-set`'
  check_contains 'count(*): 26'
  run_sql 'SELECT count(*) FROM vt.`enum-set` WHERE find_in_set("x50", `set`) > 0'
  check_contains 'count(*): 10'
  run_sql 'SELECT `set` FROM vt.`enum-set` WHERE `enum` = "gcc"'
  check_contains 'set: x00,x06,x07,x09,x17,x20,x23,x24,x27,x37,x44,x46,x49,x54,x55,x58,x61,x62'
  run_sql 'SELECT `set` FROM vt.`enum-set` WHERE `enum` = "g99"'
  check_contains 'set: x07,x08,x09,x10,x11,x12,x14,x16,x17,x18,x19,x22,x25,x26,x28,x29,x30,x31,x32,x33,x35,x38,x39,x41,x44,x46,x49,x51,x53,x55,x56,x58,x61,x63'

  run_sql 'SELECT count(*) FROM vt.empty_strings'
  check_contains 'count(*): 6'
  run_sql 'SELECT sum(pk) FROM vt.empty_strings WHERE a = ""'
  check_contains 'sum(pk): 5'
  run_sql 'SELECT sum(pk) FROM vt.empty_strings WHERE a = 0x22'
  check_contains 'sum(pk): 18'
  run_sql 'SELECT sum(pk) FROM vt.empty_strings WHERE a = 0x27'
  check_contains 'sum(pk): 40'

  run_sql 'SELECT a, b, c, d FROM vt.precise_types'
  check_contains 'a: 18446744073709551614'
  check_contains 'b: -9223372036854775806'
  check_contains 'c: 99999999999999999999.0'
  check_contains 'd: 1.8446744073709552e19'

  run_sql 'SELECT count(*), sum(id) FROM vt.bigint'
  check_contains 'count(*): 6'
  check_contains 'sum(id): -3341565093352173667'

done
