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

# Test whether BR fails fast when backup ts exceeds GC safe point.
# It is call br_*z*_gc_safepoint, because it brings lots of write and
# slows down other tests to changing GC safe point. Adding a z prefix to run
# the test last.

set -eux

DB="$TEST_NAME"
TABLE="usertable"

MAX_UINT64=9223372036854775807

run_sql "CREATE DATABASE $DB;"

go-ycsb load mysql -P tests/$TEST_NAME/workload -p mysql.host=$TIDB_IP -p mysql.port=$TIDB_PORT -p mysql.user=root -p mysql.db=$DB

# Update GC safepoint to now + 5s after 10s seconds.
sleep 10 && bin/gc -pd $PD_ADDR \
    --ca "$TEST_DIR/certs/ca.pem" \
    --cert "$TEST_DIR/certs/br.pem" \
    --key "$TEST_DIR/certs/br.key" \
    -gc-offset "5s" -update-service true &

# total bytes is 1136000
# Set ratelimit to 40960 bytes/second, it will finish within 25s,
# so it won't trigger exceed GC safe point error. even It use updateServiceGCSafePoint to update GC safePoint.
backup_gc_fail=0
echo "backup start (won't fail)..."
run_br --pd $PD_ADDR backup table -s "local://$TEST_DIR/$DB/1" --db $DB -t $TABLE --ratelimit 40960 --ratelimit-unit 1 || backup_gc_fail=1

if [ "$backup_gc_fail" -ne "0" ];then
    echo "TEST: [$TEST_NAME] test check backup ts failed!"
    exit 1
fi

# set safePoint otherwise the default safePoint is zero
bin/gc -pd $PD_ADDR \
    --ca "$TEST_DIR/certs/ca.pem" \
    --cert "$TEST_DIR/certs/br.pem" \
    --key "$TEST_DIR/certs/br.key" \
    -gc-offset "1s"

backup_gc_fail=0
echo "incremental backup start (expect fail)..."
run_br --pd $PD_ADDR backup table -s "local://$TEST_DIR/$DB/2" --db $DB -t $TABLE --lastbackupts 1 --ratelimit 1 --ratelimit-unit 1 || backup_gc_fail=1

if [ "$backup_gc_fail" -ne "1" ];then
    echo "TEST: [$TEST_NAME] test check last backup ts failed!"
    exit 1
fi

backup_gc_fail=0
echo "incremental backup with max_uint64 start (expect fail)..."
run_br --pd $PD_ADDR backup table -s "local://$TEST_DIR/$DB/3" --db $DB -t $TABLE --lastbackupts $MAX_UINT64 --ratelimit 1 --ratelimit-unit 1 || backup_gc_fail=1

if [ "$backup_gc_fail" -ne "1" ];then
    echo "TEST: [$TEST_NAME] test check max backup ts failed!"
    exit 1
fi

run_sql "DROP DATABASE $DB;"
