// Copyright 2018 PingCAP, Inc.
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

package execdetails

import (
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/pingcap/tipb/go-tipb"
	"github.com/stretchr/testify/require"
	"github.com/tikv/client-go/v2/util"
)

func TestString(t *testing.T) {
	detail := &ExecDetails{
		CopTime:      time.Second + 3*time.Millisecond,
		BackoffTime:  time.Second,
		RequestCount: 1,
		CommitDetail: &util.CommitDetails{
			GetCommitTsTime: time.Second,
			GetLatestTsTime: time.Second,
			PrewriteTime:    time.Second,
			CommitTime:      time.Second,
			LocalLatchTime:  time.Second,

			Mu: struct {
				sync.Mutex
				CommitBackoffTime    int64
				PrewriteBackoffTypes []string
				CommitBackoffTypes   []string
				SlowestPrewrite      util.ReqDetailInfo
				CommitPrimary        util.ReqDetailInfo
			}{
				CommitBackoffTime: int64(time.Second),
				PrewriteBackoffTypes: []string{
					"backoff1",
					"backoff2",
				},
				CommitBackoffTypes: []string{
					"commit1",
					"commit2",
				},
				SlowestPrewrite: util.ReqDetailInfo{
					ReqTotalTime: time.Second,
					Region:       1000,
					StoreAddr:    "tikv-1:20160",
					ExecDetails: util.TiKVExecDetails{
						TimeDetail: &util.TimeDetail{
							TotalRPCWallTime: 500 * time.Millisecond,
						},
						ScanDetail: &util.ScanDetail{
							ProcessedKeys:             10,
							TotalKeys:                 100,
							RocksdbDeleteSkippedCount: 1,
							RocksdbKeySkippedCount:    1,
							RocksdbBlockCacheHitCount: 1,
							RocksdbBlockReadCount:     1,
							RocksdbBlockReadByte:      100,
							RocksdbBlockReadDuration:  20 * time.Millisecond,
						},
						WriteDetail: &util.WriteDetail{
							StoreBatchWaitDuration:        10 * time.Microsecond,
							ProposeSendWaitDuration:       20 * time.Microsecond,
							PersistLogDuration:            30 * time.Microsecond,
							RaftDbWriteLeaderWaitDuration: 40 * time.Microsecond,
							RaftDbSyncLogDuration:         45 * time.Microsecond,
							RaftDbWriteMemtableDuration:   50 * time.Microsecond,
							CommitLogDuration:             60 * time.Microsecond,
							ApplyBatchWaitDuration:        70 * time.Microsecond,
							ApplyLogDuration:              80 * time.Microsecond,
							ApplyMutexLockDuration:        90 * time.Microsecond,
							ApplyWriteLeaderWaitDuration:  100 * time.Microsecond,
							ApplyWriteWalDuration:         101 * time.Microsecond,
							ApplyWriteMemtableDuration:    102 * time.Microsecond,
						},
					},
				},
				CommitPrimary: util.ReqDetailInfo{},
			},
			WriteKeys:         1,
			WriteSize:         1,
			PrewriteRegionNum: 1,
			TxnRetry:          1,
			ResolveLock: util.ResolveLockDetail{
				ResolveLockTime: 1000000000, // 10^9 ns = 1s
			},
		},
		ScanDetail: &util.ScanDetail{
			ProcessedKeys:             10,
			TotalKeys:                 100,
			RocksdbDeleteSkippedCount: 1,
			RocksdbKeySkippedCount:    1,
			RocksdbBlockCacheHitCount: 1,
			RocksdbBlockReadCount:     1,
			RocksdbBlockReadByte:      100,
			RocksdbBlockReadDuration:  time.Millisecond,
		},
		DetailsNeedP90: DetailsNeedP90{TimeDetail: util.TimeDetail{
			ProcessTime: 2*time.Second + 5*time.Millisecond,
			WaitTime:    time.Second,
		}},
	}
	expected := "Cop_time: 1.003 Process_time: 2.005 Wait_time: 1 Backoff_time: 1 Request_count: 1 Prewrite_time: 1 Commit_time: " +
		"1 Get_commit_ts_time: 1 Get_latest_ts_time: 1 Commit_backoff_time: 1 " +
		"Prewrite_Backoff_types: [backoff1 backoff2] Commit_Backoff_types: [commit1 commit2] Slowest_prewrite_rpc_detail: {total:1.000s, region_id: 1000, " +
		"store: tikv-1:20160, tikv_wall_time: 500ms, scan_detail: {total_process_keys: 10, total_keys: 100, " +
		"rocksdb: {delete_skipped_count: 1, key_skipped_count: 1, block: {cache_hit_count: 1, read_count: 1, " +
		"read_byte: 100 Bytes, read_time: 20ms}}}, write_detail: {store_batch_wait: 10µs, propose_send_wait: 20µs, " +
		"persist_log: {total: 30µs, write_leader_wait: 40µs, sync_log: 45µs, write_memtable: 50µs}, " +
		"commit_log: 60µs, apply_batch_wait: 70µs, apply: {total:80µs, mutex_lock: 90µs, write_leader_wait: 100µs, " +
		"write_wal: 101µs, write_memtable: 102µs}}} Resolve_lock_time: 1 Local_latch_wait_time: 1 Write_keys: 1 Write_size: " +
		"1 Prewrite_region: 1 Txn_retry: 1 Process_keys: 10 Total_keys: 100 Rocksdb_delete_skipped_count: 1 Rocksdb_key_skipped_count: " +
		"1 Rocksdb_block_cache_hit_count: 1 Rocksdb_block_read_count: 1 Rocksdb_block_read_byte: 100 Rocksdb_block_read_time: 0.001"
	require.Equal(t, expected, detail.String())
	detail = &ExecDetails{}
	require.Equal(t, "", detail.String())
}

func mockExecutorExecutionSummary(TimeProcessedNs, NumProducedRows, NumIterations uint64) *tipb.ExecutorExecutionSummary {
	return &tipb.ExecutorExecutionSummary{TimeProcessedNs: &TimeProcessedNs, NumProducedRows: &NumProducedRows,
		NumIterations: &NumIterations, XXX_unrecognized: nil}
}

func mockExecutorExecutionSummaryForTiFlash(TimeProcessedNs, NumProducedRows, NumIterations, Concurrency, totalDmfileScannedPacks, totalDmfileScannedRows, totalDmfileSkippedPacks, totalDmfileSkippedRows, totalDmfileRoughSetIndexLoadTimeMs, totalDmfileReadTimeMs, totalCreateSnapshotTimeMs uint64, totalLocalRegionNum uint64, totalRemoteRegionNum uint64, ExecutorID string) *tipb.ExecutorExecutionSummary {
	tiflashScanContext := tipb.TiFlashScanContext{
		TotalDmfileScannedPacks:            &totalDmfileScannedPacks,
		TotalDmfileSkippedPacks:            &totalDmfileSkippedPacks,
		TotalDmfileScannedRows:             &totalDmfileScannedRows,
		TotalDmfileSkippedRows:             &totalDmfileSkippedRows,
		TotalDmfileRoughSetIndexLoadTimeMs: &totalDmfileRoughSetIndexLoadTimeMs,
		TotalDmfileReadTimeMs:              &totalDmfileReadTimeMs,
		TotalCreateSnapshotTimeMs:          &totalCreateSnapshotTimeMs,
		TotalLocalRegionNum:                &totalLocalRegionNum,
		TotalRemoteRegionNum:               &totalRemoteRegionNum,
	}
	return &tipb.ExecutorExecutionSummary{TimeProcessedNs: &TimeProcessedNs, NumProducedRows: &NumProducedRows,
		NumIterations: &NumIterations, Concurrency: &Concurrency, ExecutorId: &ExecutorID, DetailInfo: &tipb.ExecutorExecutionSummary_TiflashScanContext{TiflashScanContext: &tiflashScanContext}, XXX_unrecognized: nil}
}

func TestCopRuntimeStats(t *testing.T) {
	stats := NewRuntimeStatsColl(nil)
	tableScanID := 1
	aggID := 2
	tableReaderID := 3
	stats.RecordOneCopTask(tableScanID, "tikv", "8.8.8.8", mockExecutorExecutionSummary(1, 1, 1))
	stats.RecordOneCopTask(tableScanID, "tikv", "8.8.8.9", mockExecutorExecutionSummary(2, 2, 2))
	stats.RecordOneCopTask(aggID, "tikv", "8.8.8.8", mockExecutorExecutionSummary(3, 3, 3))
	stats.RecordOneCopTask(aggID, "tikv", "8.8.8.9", mockExecutorExecutionSummary(4, 4, 4))
	scanDetail := &util.ScanDetail{
		TotalKeys:                 15,
		ProcessedKeys:             10,
		ProcessedKeysSize:         10,
		RocksdbDeleteSkippedCount: 5,
		RocksdbKeySkippedCount:    1,
		RocksdbBlockCacheHitCount: 10,
		RocksdbBlockReadCount:     20,
		RocksdbBlockReadByte:      100,
	}
	stats.RecordScanDetail(tableScanID, "tikv", scanDetail)
	require.True(t, stats.ExistsCopStats(tableScanID))

	cop := stats.GetOrCreateCopStats(tableScanID, "tikv")
	expected := "tikv_task:{proc max:2ns, min:1ns, avg: 1ns, p80:2ns, p95:2ns, iters:3, tasks:2}, " +
		"scan_detail: {total_process_keys: 10, total_process_keys_size: 10, total_keys: 15, rocksdb: {delete_skipped_count: 5, key_skipped_count: 1, block: {cache_hit_count: 10, read_count: 20, read_byte: 100 Bytes}}}"
	require.Equal(t, expected, cop.String())

	copStats := cop.stats["8.8.8.8"]
	require.NotNil(t, copStats)

	newCopStats := &basicCopRuntimeStats{}
	newCopStats.SetRowNum(10)
	newCopStats.Record(time.Second, 10)
	copStats.Merge(newCopStats)
	require.Equal(t, "time:1s, loops:2", copStats.String())
	require.Equal(t, "tikv_task:{proc max:4ns, min:3ns, avg: 3ns, p80:4ns, p95:4ns, iters:7, tasks:2}", stats.GetOrCreateCopStats(aggID, "tikv").String())

	rootStats := stats.GetRootStats(tableReaderID)
	require.NotNil(t, rootStats)
	require.True(t, stats.ExistsRootStats(tableReaderID))

	cop.scanDetail.ProcessedKeys = 0
	cop.scanDetail.ProcessedKeysSize = 0
	cop.scanDetail.RocksdbKeySkippedCount = 0
	cop.scanDetail.RocksdbBlockReadCount = 0
	// Print all fields even though the value of some fields is 0.
	str := "tikv_task:{proc max:1s, min:1ns, avg: 500ms, p80:1s, p95:1s, iters:4, tasks:2}, " +
		"scan_detail: {total_keys: 15, rocksdb: {delete_skipped_count: 5, block: {cache_hit_count: 10, read_byte: 100 Bytes}}}"
	require.Equal(t, str, cop.String())

	zeroScanDetail := util.ScanDetail{}
	require.Equal(t, "", zeroScanDetail.String())
}

func TestCopRuntimeStatsForTiFlash(t *testing.T) {
	stats := NewRuntimeStatsColl(nil)
	tableScanID := 1
	aggID := 2
	tableReaderID := 3
	stats.RecordOneCopTask(aggID, "tiflash", "8.8.8.8", mockExecutorExecutionSummaryForTiFlash(1, 1, 1, 1, 1, 8192, 0, 0, 15, 200, 40, 10, 4, "tablescan_"+strconv.Itoa(tableScanID)))
	stats.RecordOneCopTask(aggID, "tiflash", "8.8.8.9", mockExecutorExecutionSummaryForTiFlash(2, 2, 2, 1, 0, 0, 0, 0, 0, 2, 0, 0, 0, "tablescan_"+strconv.Itoa(tableScanID)))
	stats.RecordOneCopTask(tableScanID, "tiflash", "8.8.8.8", mockExecutorExecutionSummaryForTiFlash(3, 3, 3, 1, 2, 12000, 1, 6000, 60, 1000, 20, 5, 1, "aggregation_"+strconv.Itoa(aggID)))
	stats.RecordOneCopTask(tableScanID, "tiflash", "8.8.8.9", mockExecutorExecutionSummaryForTiFlash(4, 4, 4, 1, 1, 8192, 10, 80000, 40, 2000, 30, 1, 1, "aggregation_"+strconv.Itoa(aggID)))
	scanDetail := &util.ScanDetail{
		TotalKeys:                 10,
		ProcessedKeys:             10,
		RocksdbDeleteSkippedCount: 10,
		RocksdbKeySkippedCount:    1,
		RocksdbBlockCacheHitCount: 10,
		RocksdbBlockReadCount:     10,
		RocksdbBlockReadByte:      100,
	}
	stats.RecordScanDetail(tableScanID, "tiflash", scanDetail)
	require.True(t, stats.ExistsCopStats(tableScanID))

	cop := stats.GetOrCreateCopStats(tableScanID, "tiflash")
	require.Equal(t, "tiflash_task:{proc max:2ns, min:1ns, avg: 1ns, p80:2ns, p95:2ns, iters:3, tasks:2, threads:2}, tiflash_scan:{dtfile:{total_scanned_packs:1, total_skipped_packs:0, total_scanned_rows:8192, total_skipped_rows:0, total_rs_index_load_time: 15ms, total_read_time: 202ms}, total_create_snapshot_time: 40ms, total_local_region_num: 10, total_remote_region_num: 4}", cop.String())

	copStats := cop.stats["8.8.8.8"]
	require.NotNil(t, copStats)

	copStats.SetRowNum(10)
	copStats.Record(time.Second, 10)
	require.Equal(t, "time:1s, loops:2, threads:1, tiflash_scan:{dtfile:{total_scanned_packs:1, total_skipped_packs:0, total_scanned_rows:8192, total_skipped_rows:0, total_rs_index_load_time: 15ms, total_read_time: 200ms}, total_create_snapshot_time: 40ms, total_local_region_num: 10, total_remote_region_num: 4}", copStats.String())
	expected := "tiflash_task:{proc max:4ns, min:3ns, avg: 3ns, p80:4ns, p95:4ns, iters:7, tasks:2, threads:2}, tiflash_scan:{dtfile:{total_scanned_packs:3, total_skipped_packs:11, total_scanned_rows:20192, total_skipped_rows:86000, total_rs_index_load_time: 100ms, total_read_time: 3000ms}, total_create_snapshot_time: 50ms, total_local_region_num: 6, total_remote_region_num: 2}"
	require.Equal(t, expected, stats.GetOrCreateCopStats(aggID, "tiflash").String())

	rootStats := stats.GetRootStats(tableReaderID)
	require.NotNil(t, rootStats)
	require.True(t, stats.ExistsRootStats(tableReaderID))
}

func TestRuntimeStatsWithCommit(t *testing.T) {
	commitDetail := &util.CommitDetails{
		GetCommitTsTime: time.Second,
		PrewriteTime:    time.Second,
		CommitTime:      time.Second,
		Mu: struct {
			sync.Mutex
			CommitBackoffTime    int64
			PrewriteBackoffTypes []string
			CommitBackoffTypes   []string
			SlowestPrewrite      util.ReqDetailInfo
			CommitPrimary        util.ReqDetailInfo
		}{
			CommitBackoffTime:    int64(time.Second),
			PrewriteBackoffTypes: []string{"backoff1", "backoff2", "backoff1"},
			CommitBackoffTypes:   []string{},
			SlowestPrewrite: util.ReqDetailInfo{
				ReqTotalTime: time.Second,
				Region:       1000,
				StoreAddr:    "tikv-1:20160",
				ExecDetails: util.TiKVExecDetails{
					TimeDetail: &util.TimeDetail{
						TotalRPCWallTime: 500 * time.Millisecond,
					},
					ScanDetail: &util.ScanDetail{
						ProcessedKeys:             10,
						TotalKeys:                 100,
						RocksdbDeleteSkippedCount: 1,
						RocksdbKeySkippedCount:    1,
						RocksdbBlockCacheHitCount: 1,
						RocksdbBlockReadCount:     1,
						RocksdbBlockReadByte:      100,
						RocksdbBlockReadDuration:  20 * time.Millisecond,
					},
					WriteDetail: &util.WriteDetail{
						StoreBatchWaitDuration:        10 * time.Microsecond,
						ProposeSendWaitDuration:       20 * time.Microsecond,
						PersistLogDuration:            30 * time.Microsecond,
						RaftDbWriteLeaderWaitDuration: 40 * time.Microsecond,
						RaftDbSyncLogDuration:         45 * time.Microsecond,
						RaftDbWriteMemtableDuration:   50 * time.Microsecond,
						CommitLogDuration:             60 * time.Microsecond,
						ApplyBatchWaitDuration:        70 * time.Microsecond,
						ApplyLogDuration:              80 * time.Microsecond,
						ApplyMutexLockDuration:        90 * time.Microsecond,
						ApplyWriteLeaderWaitDuration:  100 * time.Microsecond,
						ApplyWriteWalDuration:         101 * time.Microsecond,
						ApplyWriteMemtableDuration:    102 * time.Microsecond,
					},
				},
			},
			CommitPrimary: util.ReqDetailInfo{},
		},
		WriteKeys:         3,
		WriteSize:         66,
		PrewriteRegionNum: 5,
		TxnRetry:          2,
		ResolveLock: util.ResolveLockDetail{
			ResolveLockTime: int64(time.Second),
		},
	}
	stats := &RuntimeStatsWithCommit{
		Commit: commitDetail,
	}
	expect := "commit_txn: {prewrite:1s, get_commit_ts:1s, commit:1s, backoff: {time: 1s, prewrite type: [backoff1 backoff2]}, " +
		"slowest_prewrite_rpc: {total: 1.000s, region_id: 1000, store: tikv-1:20160, tikv_wall_time: 500ms, " +
		"scan_detail: {total_process_keys: 10, total_keys: 100, rocksdb: {delete_skipped_count: 1, key_skipped_count: 1, " +
		"block: {cache_hit_count: 1, read_count: 1, read_byte: 100 Bytes, read_time: 20ms}}}, " +
		"write_detail: {store_batch_wait: 10µs, propose_send_wait: 20µs, persist_log: {total: 30µs, write_leader_wait: 40µs, " +
		"sync_log: 45µs, write_memtable: 50µs}, commit_log: 60µs, apply_batch_wait: 70µs, apply: {total:80µs, mutex_lock: 90µs, " +
		"write_leader_wait: 100µs, write_wal: 101µs, write_memtable: 102µs}}}, resolve_lock: 1s, region_num:5, write_keys:3" +
		", write_byte:66, txn_retry:2}"
	require.Equal(t, expect, stats.String())

	lockDetail := &util.LockKeysDetails{
		TotalTime:   time.Second,
		RegionNum:   2,
		LockKeys:    10,
		BackoffTime: int64(time.Second * 3),
		Mu: struct {
			sync.Mutex
			BackoffTypes        []string
			SlowestReqTotalTime time.Duration
			SlowestRegion       uint64
			SlowestStoreAddr    string
			SlowestExecDetails  util.TiKVExecDetails
		}{
			BackoffTypes: []string{
				"backoff4",
				"backoff5",
				"backoff5",
			},
			SlowestReqTotalTime: time.Second,
			SlowestRegion:       1000,
			SlowestStoreAddr:    "tikv-1:20160",
			SlowestExecDetails: util.TiKVExecDetails{
				TimeDetail: &util.TimeDetail{
					TotalRPCWallTime: 500 * time.Millisecond,
				},
				ScanDetail: &util.ScanDetail{
					ProcessedKeys:             10,
					TotalKeys:                 100,
					RocksdbDeleteSkippedCount: 1,
					RocksdbKeySkippedCount:    1,
					RocksdbBlockCacheHitCount: 1,
					RocksdbBlockReadCount:     1,
					RocksdbBlockReadByte:      100,
					RocksdbBlockReadDuration:  20 * time.Millisecond,
				},
				WriteDetail: &util.WriteDetail{
					StoreBatchWaitDuration:        10 * time.Microsecond,
					ProposeSendWaitDuration:       20 * time.Microsecond,
					PersistLogDuration:            30 * time.Microsecond,
					RaftDbWriteLeaderWaitDuration: 40 * time.Microsecond,
					RaftDbSyncLogDuration:         45 * time.Microsecond,
					RaftDbWriteMemtableDuration:   50 * time.Microsecond,
					CommitLogDuration:             60 * time.Microsecond,
					ApplyBatchWaitDuration:        70 * time.Microsecond,
					ApplyLogDuration:              80 * time.Microsecond,
					ApplyMutexLockDuration:        90 * time.Microsecond,
					ApplyWriteLeaderWaitDuration:  100 * time.Microsecond,
					ApplyWriteWalDuration:         101 * time.Microsecond,
					ApplyWriteMemtableDuration:    102 * time.Microsecond,
				},
			}},
		LockRPCTime:  int64(time.Second * 5),
		LockRPCCount: 50,
		RetryCount:   2,
		ResolveLock: util.ResolveLockDetail{
			ResolveLockTime: int64(time.Second * 2),
		},
	}
	stats = &RuntimeStatsWithCommit{
		LockKeys: lockDetail,
	}
	expect = "lock_keys: {time:1s, region:2, keys:10, resolve_lock:2s, backoff: {time: 3s, type: [backoff4 backoff5]}, " +
		"slowest_rpc: {total: 1.000s, region_id: 1000, store: tikv-1:20160, tikv_wall_time: 500ms, scan_detail: " +
		"{total_process_keys: 10, total_keys: 100, rocksdb: {delete_skipped_count: 1, key_skipped_count: 1, block: " +
		"{cache_hit_count: 1, read_count: 1, read_byte: 100 Bytes, read_time: 20ms}}}, write_detail: " +
		"{store_batch_wait: 10µs, propose_send_wait: 20µs, persist_log: {total: 30µs, write_leader_wait: 40µs, sync_log: 45µs, write_memtable: 50µs}, " +
		"commit_log: 60µs, apply_batch_wait: 70µs, apply: {total:80µs, mutex_lock: 90µs, write_leader_wait: 100µs, write_wal: 101µs, write_memtable: 102µs}}}, " +
		"lock_rpc:5s, rpc_count:50, retry_count:2}"
	require.Equal(t, expect, stats.String())
}

func TestRootRuntimeStats(t *testing.T) {
	pid := 1
	stmtStats := NewRuntimeStatsColl(nil)
	basic1 := stmtStats.GetBasicRuntimeStats(pid)
	basic2 := stmtStats.GetBasicRuntimeStats(pid)
	basic1.Record(time.Second, 20)
	basic2.Record(time.Second*2, 30)
	concurrency := &RuntimeStatsWithConcurrencyInfo{}
	concurrency.SetConcurrencyInfo(NewConcurrencyInfo("worker", 15))
	commitDetail := &util.CommitDetails{
		GetCommitTsTime:   time.Second,
		PrewriteTime:      time.Second,
		CommitTime:        time.Second,
		WriteKeys:         3,
		WriteSize:         66,
		PrewriteRegionNum: 5,
		TxnRetry:          2,
	}
	stmtStats.RegisterStats(pid, concurrency)
	stmtStats.RegisterStats(pid, &RuntimeStatsWithCommit{
		Commit: commitDetail,
	})
	stats := stmtStats.GetRootStats(1)
	expect := "time:3s, loops:2, worker:15, commit_txn: {prewrite:1s, get_commit_ts:1s, commit:1s, region_num:5, write_keys:3, write_byte:66, txn_retry:2}"
	require.Equal(t, expect, stats.String())
}

func TestFormatDurationForExplain(t *testing.T) {
	cases := []struct {
		t string
		s string
	}{
		{"0s", "0s"},
		{"1ns", "1ns"},
		{"9ns", "9ns"},
		{"10ns", "10ns"},
		{"999ns", "999ns"},
		{"1µs", "1µs"},
		{"1.123µs", "1.12µs"},
		{"1.023µs", "1.02µs"},
		{"1.003µs", "1µs"},
		{"10.456µs", "10.5µs"},
		{"10.956µs", "11µs"},
		{"999.056µs", "999.1µs"},
		{"999.988µs", "1ms"},
		{"1.123ms", "1.12ms"},
		{"1.023ms", "1.02ms"},
		{"1.003ms", "1ms"},
		{"10.456ms", "10.5ms"},
		{"10.956ms", "11ms"},
		{"999.056ms", "999.1ms"},
		{"999.988ms", "1s"},
		{"1.123s", "1.12s"},
		{"1.023s", "1.02s"},
		{"1.003s", "1s"},
		{"10.456s", "10.5s"},
		{"10.956s", "11s"},
		{"16m39.056s", "16m39.1s"},
		{"16m39.988s", "16m40s"},
		{"24h16m39.388662s", "24h16m39.4s"},
		{"9.412345ms", "9.41ms"},
		{"10.412345ms", "10.4ms"},
		{"5.999s", "6s"},
		{"100.45µs", "100.5µs"},
	}
	for _, ca := range cases {
		d, err := time.ParseDuration(ca.t)
		require.NoError(t, err)

		result := FormatDuration(d)
		require.Equal(t, ca.s, result)
	}
}
