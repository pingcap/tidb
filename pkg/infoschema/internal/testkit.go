// Copyright 2023 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package internal

import (
	"os"
	"testing"

	"github.com/stretchr/testify/require"
)

// PrepareSlowLogfile prepares a slow log file for test.
func PrepareSlowLogfile(t *testing.T, slowLogFileName string) {
	f, err := os.OpenFile(slowLogFileName, os.O_CREATE|os.O_WRONLY, 0600)
	require.NoError(t, err)
	_, err = f.WriteString(`# Time: 2019-02-12T19:33:56.571953+08:00
# Txn_start_ts: 406315658548871171
# User@Host: root[root] @ localhost [127.0.0.1]
# Conn_ID: 6
# Exec_retry_time: 0.12 Exec_retry_count: 57
# Query_time: 4.895492
# Parse_time: 0.4
# Compile_time: 0.2
# Rewrite_time: 0.000000003 Preproc_subqueries: 2 Preproc_subqueries_time: 0.000000002
# Optimize_time: 0.00000001
# Wait_TS: 0.000000003
# LockKeys_time: 1.71 Request_count: 1 Prewrite_time: 0.19 Wait_prewrite_binlog_time: 0.21 Commit_time: 0.01 Commit_backoff_time: 0.18 Backoff_types: [txnLock] Resolve_lock_time: 0.03 Write_keys: 15 Write_size: 480 Prewrite_region: 1 Txn_retry: 8
# Cop_time: 0.3824278 Process_time: 0.161 Request_count: 1 Total_keys: 100001 Process_keys: 100000
# Rocksdb_delete_skipped_count: 100 Rocksdb_key_skipped_count: 10 Rocksdb_block_cache_hit_count: 10 Rocksdb_block_read_count: 10 Rocksdb_block_read_byte: 100
# Wait_time: 0.101
# Backoff_time: 0.092
# DB: test
# Is_internal: false
# Digest: 42a1c8aae6f133e934d4bf0147491709a8812ea05ff8819ec522780fe657b772
# Stats: t1:1,t2:2
# Cop_proc_avg: 0.1 Cop_proc_p90: 0.2 Cop_proc_max: 0.03 Cop_proc_addr: 127.0.0.1:20160
# Cop_wait_avg: 0.05 Cop_wait_p90: 0.6 Cop_wait_max: 0.8 Cop_wait_addr: 0.0.0.0:20160
# Mem_max: 70724
# Disk_max: 65536
# Plan_from_cache: true
# Result_rows: 10
# Succ: true
# Plan: abcd
# Plan_digest: 60e9378c746d9a2be1c791047e008967cf252eb6de9167ad3aa6098fa2d523f4
# Prev_stmt: update t set i = 2;
select * from t_slim;
# Time: 2021-09-08T14:39:54.506967433+08:00
# Txn_start_ts: 427578666238083075
# User@Host: root[root] @ 172.16.0.0 [172.16.0.0]
# Conn_ID: 40507
# Session_alias: alias123
# Query_time: 25.571605962
# Parse_time: 0.002923536
# Compile_time: 0.006800973
# Rewrite_time: 0.002100764
# Optimize_time: 0
# Wait_TS: 0.000015801
# Prewrite_time: 25.542014572 Commit_time: 0.002294647 Get_commit_ts_time: 0.000605473 Commit_backoff_time: 12.483 Backoff_types: [tikvRPC regionMiss tikvRPC regionMiss regionMiss] Write_keys: 624 Write_size: 172064 Prewrite_region: 60
# DB: rtdb
# Is_internal: false
# Digest: 124acb3a0bec903176baca5f9da00b4e7512a41c93b417923f26502edeb324cc
# Num_cop_tasks: 0
# Mem_max: 856544
# Prepared: false
# Plan_from_cache: false
# Plan_from_binding: false
# Has_more_results: false
# KV_total: 86.635049185
# PD_total: 0.015486658
# Backoff_total: 100.054
# Write_sql_response_total: 0
# Succ: true
INSERT INTO ...;
`)
	require.NoError(t, f.Close())
	require.NoError(t, err)
}
