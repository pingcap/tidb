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

CUR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
export GO_FAILPOINTS="github.com/pingcap/tidb/lightning/pkg/importer/AddIndexFail=return()"

LOG_FILE1="$TEST_DIR/lightning-add-index1.log"
LOG_FILE2="$TEST_DIR/lightning-add-index2.log"

run_lightning --config "$CUR/config1.toml" --log-file "$LOG_FILE1"
multi_indexes_kvs=$(run_sql "ADMIN CHECKSUM TABLE add_index.multi_indexes;" | grep "Total_kvs" | awk '{print $2}')
multi_indexes_cksum=$(run_sql "ADMIN CHECKSUM TABLE add_index.multi_indexes;" | grep "Checksum_crc64_xor" | awk '{print $2}')
non_pk_auto_inc_kvs=$(run_sql "ADMIN CHECKSUM TABLE add_index.non_pk_auto_inc;" | grep "Total_kvs" | awk '{print $2}')
non_pk_auto_inc_cksum=$(run_sql "ADMIN CHECKSUM TABLE add_index.non_pk_auto_inc;" | grep "Checksum_crc64_xor" | awk '{print $2}')

run_sql "DROP DATABASE add_index;"
run_lightning --config "$CUR/config2.toml" --log-file "$LOG_FILE2"
actual_multi_indexes_kvs=$(run_sql "ADMIN CHECKSUM TABLE add_index.multi_indexes;" | grep "Total_kvs" | awk '{print $2}')
actual_multi_indexes_cksum=$(run_sql "ADMIN CHECKSUM TABLE add_index.multi_indexes;" | grep "Checksum_crc64_xor" | awk '{print $2}')
actual_non_pk_auto_inc_kvs=$(run_sql "ADMIN CHECKSUM TABLE add_index.non_pk_auto_inc;" | grep "Total_kvs" | awk '{print $2}')
actual_non_pk_auto_inc_cksum=$(run_sql "ADMIN CHECKSUM TABLE add_index.non_pk_auto_inc;" | grep "Checksum_crc64_xor" | awk '{print $2}')


# 1. Check for table multi_indexes

if [ "$multi_indexes_kvs" != "$actual_multi_indexes_kvs" ]; then
    echo "multi_indexes kvs not equal, expect $multi_indexes_kvs, got $actual_multi_indexes_kvs"
    exit 1
fi
if [ "$multi_indexes_cksum" != "$actual_multi_indexes_cksum" ]; then
    echo "multi_indexes cksum not equal, expect $multi_indexes_cksum, got $actual_multi_indexes_cksum"
    exit 1
fi

run_sql "SHOW CREATE TABLE add_index.multi_indexes;"
check_contains "INVISIBLE"
check_contains "'single column index with invisible'"

grep -Fq "ALTER TABLE \`add_index\`.\`multi_indexes\` DROP INDEX \`idx_c2\`" "$LOG_FILE2"
grep -Fq "ALTER TABLE \`add_index\`.\`multi_indexes\` DROP INDEX \`idx_c2_c3\`" "$LOG_FILE2"
grep -Fq "ALTER TABLE \`add_index\`.\`multi_indexes\` DROP INDEX \`uniq_c4\`" "$LOG_FILE2"
grep -Fq "ALTER TABLE \`add_index\`.\`multi_indexes\` DROP INDEX \`uniq_c4_c5\`" "$LOG_FILE2"
grep -Fq "ALTER TABLE \`add_index\`.\`multi_indexes\` DROP INDEX \`idx_c6\`" "$LOG_FILE2"
grep -Fq "ALTER TABLE \`add_index\`.\`multi_indexes\` DROP INDEX \`idx_c7\`" "$LOG_FILE2"
grep -Fq "ALTER TABLE \`add_index\`.\`multi_indexes\` DROP INDEX \`idx_c6_c7\`" "$LOG_FILE2"
grep -Fq "ALTER TABLE \`add_index\`.\`multi_indexes\` DROP INDEX \`idx_c8\`" "$LOG_FILE2"
grep -Fq "ALTER TABLE \`add_index\`.\`multi_indexes\` DROP INDEX \`idx_c9\`" "$LOG_FILE2"
grep -Fq "ALTER TABLE \`add_index\`.\`multi_indexes\` DROP INDEX \`idx_lower_c10\`" "$LOG_FILE2"
grep -Fq "ALTER TABLE \`add_index\`.\`multi_indexes\` DROP INDEX \`idx_prefix_c11\`" "$LOG_FILE2"
grep -Fq "ALTER TABLE \`add_index\`.\`multi_indexes\` DROP INDEX \`c2\`" "$LOG_FILE2"

grep -Fq "ALTER TABLE \`add_index\`.\`multi_indexes\` ADD KEY \`idx_c2\`(\`c2\`) COMMENT 'single column index', ADD KEY \`idx_c2_c3\`(\`c2\`,\`c3\`) COMMENT 'multiple column index', ADD UNIQUE KEY \`uniq_c4\`(\`c4\`) COMMENT 'single column unique key', ADD UNIQUE KEY \`uniq_c4_c5\`(\`c4\`,\`c5\`) COMMENT 'multiple column unique key', ADD KEY \`idx_c6\`(\`c6\`) COMMENT 'single column index with asc order', ADD KEY \`idx_c7\`(\`c7\`) COMMENT 'single column index with desc order', ADD KEY \`idx_c6_c7\`(\`c6\`,\`c7\`) COMMENT 'multiple column index with asc and desc order', ADD KEY \`idx_c8\`(\`c8\`) COMMENT 'single column index with visible', ADD KEY \`idx_c9\`(\`c9\`) INVISIBLE COMMENT 'single column index with invisible', ADD KEY \`idx_lower_c10\`((lower(\`c10\`))) COMMENT 'single column index with function', ADD KEY \`idx_prefix_c11\`(\`c11\`(3)) COMMENT 'single column index with prefix', ADD UNIQUE KEY \`c2\`(\`c2\`)" "$LOG_FILE2"


# 2. Check for table non_clustered_pk

if [ "$non_pk_auto_inc_kvs" != "$actual_non_pk_auto_inc_kvs" ]; then
    echo "non_pk_auto_inc kvs not equal, expect $non_pk_auto_inc_kvs, got $actual_non_pk_auto_inc_kvs"
    exit 1
fi
if [ "$non_pk_auto_inc_cksum" != "$actual_non_pk_auto_inc_cksum" ]; then
    echo "non_pk_auto_inc cksum not equal, expect $non_pk_auto_inc_cksum, got $actual_non_pk_auto_inc_cksum"
    exit 1
fi

grep -Fq "ALTER TABLE \`add_index\`.\`non_pk_auto_inc\` DROP PRIMARY KEY" "$LOG_FILE2"
grep -Fq "ALTER TABLE \`add_index\`.\`non_pk_auto_inc\` ADD PRIMARY KEY (\`pk\`)" "$LOG_FILE2"

# 3. Check for recovering from checkpoint
export GO_FAILPOINTS="github.com/pingcap/tidb/lightning/pkg/importer/AddIndexCrash=return()"
run_sql "DROP DATABASE add_index;"
run_lightning --enable-checkpoint=1 --config "$CUR/config2.toml" --log-file "$LOG_FILE2"
grep -Fq "task canceled" "$LOG_FILE2"

unset GO_FAILPOINTS
run_lightning --enable-checkpoint=1 --config "$CUR/config2.toml" --log-file "$LOG_FILE2"
actual_multi_indexes_kvs=$(run_sql "ADMIN CHECKSUM TABLE add_index.multi_indexes;" | grep "Total_kvs" | awk '{print $2}')
actual_multi_indexes_cksum=$(run_sql "ADMIN CHECKSUM TABLE add_index.multi_indexes;" | grep "Checksum_crc64_xor" | awk '{print $2}')
actual_non_pk_auto_inc_kvs=$(run_sql "ADMIN CHECKSUM TABLE add_index.non_pk_auto_inc;" | grep "Total_kvs" | awk '{print $2}')
actual_non_pk_auto_inc_cksum=$(run_sql "ADMIN CHECKSUM TABLE add_index.non_pk_auto_inc;" | grep "Checksum_crc64_xor" | awk '{print $2}')

set -x
[ "$multi_indexes_kvs" == "$actual_multi_indexes_kvs" ] && [ "$multi_indexes_cksum" == "$actual_multi_indexes_cksum" ]
[ "$non_pk_auto_inc_kvs" == "$actual_non_pk_auto_inc_kvs" ] && [ "$non_pk_auto_inc_cksum" == "$actual_non_pk_auto_inc_cksum" ]
