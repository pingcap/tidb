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
// See the License for the specific language governing permissions and
// limitations under the License.

package execdetails

import (
	"fmt"
	"strconv"
	"sync"
	"testing"
	"time"

	. "github.com/pingcap/check"
	"github.com/pingcap/tidb/util/stringutil"
	"github.com/pingcap/tipb/go-tipb"
)

func TestT(t *testing.T) {
	TestingT(t)
}

func TestString(t *testing.T) {
	detail := &ExecDetails{
		CopTime:      time.Second + 3*time.Millisecond,
		ProcessTime:  2*time.Second + 5*time.Millisecond,
		WaitTime:     time.Second,
		BackoffTime:  time.Second,
		RequestCount: 1,
		CommitDetail: &CommitDetails{
			GetCommitTsTime:   time.Second,
			PrewriteTime:      time.Second,
			CommitTime:        time.Second,
			LocalLatchTime:    time.Second,
			CommitBackoffTime: int64(time.Second),
			Mu: struct {
				sync.Mutex
				BackoffTypes []fmt.Stringer
			}{BackoffTypes: []fmt.Stringer{
				stringutil.MemoizeStr(func() string {
					return "backoff1"
				}),
				stringutil.MemoizeStr(func() string {
					return "backoff2"
				}),
			}},
			ResolveLockTime:   1000000000, // 10^9 ns = 1s
			WriteKeys:         1,
			WriteSize:         1,
			PrewriteRegionNum: 1,
			TxnRetry:          1,
		},
		CopDetail: &CopDetails{
			ProcessedKeys:             10,
			TotalKeys:                 100,
			RocksdbDeleteSkippedCount: 1,
			RocksdbKeySkippedCount:    1,
			RocksdbBlockCacheHitCount: 1,
			RocksdbBlockReadCount:     1,
			RocksdbBlockReadByte:      100,
		},
	}
	expected := "Cop_time: 1.003 Process_time: 2.005 Wait_time: 1 Backoff_time: 1 Request_count: 1 Prewrite_time: 1 Commit_time: 1 " +
		"Get_commit_ts_time: 1 Commit_backoff_time: 1 Backoff_types: [backoff1 backoff2] Resolve_lock_time: 1 Local_latch_wait_time: 1 Write_keys: 1 Write_size: 1 Prewrite_region: 1 Txn_retry: 1 " +
		"Process_keys: 10 Total_keys: 100 Rocksdb_delete_skipped_count: 1 Rocksdb_key_skipped_count: 1 Rocksdb_block_cache_hit_count: 1 Rocksdb_block_read_count: 1 Rocksdb_block_read_byte: 100"
	if str := detail.String(); str != expected {
		t.Errorf("got:\n%s\nexpected:\n%s", str, expected)
	}
	detail = &ExecDetails{}
	if str := detail.String(); str != "" {
		t.Errorf("got:\n%s\nexpected:\n", str)
	}
}

func mockExecutorExecutionSummary(TimeProcessedNs, NumProducedRows, NumIterations uint64) *tipb.ExecutorExecutionSummary {
	return &tipb.ExecutorExecutionSummary{TimeProcessedNs: &TimeProcessedNs, NumProducedRows: &NumProducedRows,
		NumIterations: &NumIterations, XXX_unrecognized: nil}
}

func mockExecutorExecutionSummaryForTiFlash(TimeProcessedNs, NumProducedRows, NumIterations uint64, ExecutorID string) *tipb.ExecutorExecutionSummary {
	return &tipb.ExecutorExecutionSummary{TimeProcessedNs: &TimeProcessedNs, NumProducedRows: &NumProducedRows,
		NumIterations: &NumIterations, ExecutorId: &ExecutorID, XXX_unrecognized: nil}
}

func TestCopRuntimeStats(t *testing.T) {
	stats := NewRuntimeStatsColl()
	tableScanID := 1
	aggID := 2
	tableReaderID := 3
	stats.RecordOneCopTask(tableScanID, "8.8.8.8", mockExecutorExecutionSummary(1, 1, 1))
	stats.RecordOneCopTask(tableScanID, "8.8.8.9", mockExecutorExecutionSummary(2, 2, 2))
	stats.RecordOneCopTask(aggID, "8.8.8.8", mockExecutorExecutionSummary(3, 3, 3))
	stats.RecordOneCopTask(aggID, "8.8.8.9", mockExecutorExecutionSummary(4, 4, 4))
	if stats.ExistsCopStats(tableScanID) != true {
		t.Fatal("exist")
	}
	cop := stats.GetCopStats(tableScanID)
	if cop.String() != "tikv_task:{proc max:2ns, min:1ns, p80:2ns, p95:2ns, iters:3, tasks:2}" {
		t.Fatal("table_scan")
	}
	copStats := cop.stats["8.8.8.8"]
	if copStats == nil {
		t.Fatal("cop stats is nil")
	}
	copStats[0].SetRowNum(10)
	copStats[0].Record(time.Second, 10)
	if copStats[0].String() != "time:1.000000001s, loops:2" {
		t.Fatalf("cop stats string is not expect, got: %v", copStats[0].String())
	}

	if stats.GetCopStats(aggID).String() != "tikv_task:{proc max:4ns, min:3ns, p80:4ns, p95:4ns, iters:7, tasks:2}" {
		t.Fatal("agg")
	}
	rootStats := stats.GetRootStats(tableReaderID)
	if rootStats == nil {
		t.Fatal("table_reader")
	}
	if stats.ExistsRootStats(tableReaderID) == false {
		t.Fatal("table_reader not exists")
	}
}

func TestCopRuntimeStatsForTiFlash(t *testing.T) {
	stats := NewRuntimeStatsColl()
	tableScanID := 1
	aggID := 2
	tableReaderID := 3
	stats.RecordOneCopTask(aggID, "8.8.8.8", mockExecutorExecutionSummaryForTiFlash(1, 1, 1, "tablescan_"+strconv.Itoa(tableScanID)))
	stats.RecordOneCopTask(aggID, "8.8.8.9", mockExecutorExecutionSummaryForTiFlash(2, 2, 2, "tablescan_"+strconv.Itoa(tableScanID)))
	stats.RecordOneCopTask(tableScanID, "8.8.8.8", mockExecutorExecutionSummaryForTiFlash(3, 3, 3, "aggregation_"+strconv.Itoa(aggID)))
	stats.RecordOneCopTask(tableScanID, "8.8.8.9", mockExecutorExecutionSummaryForTiFlash(4, 4, 4, "aggregation_"+strconv.Itoa(aggID)))
	if stats.ExistsCopStats(tableScanID) != true {
		t.Fatal("exist")
	}
	cop := stats.GetCopStats(tableScanID)
	if cop.String() != "tikv_task:{proc max:2ns, min:1ns, p80:2ns, p95:2ns, iters:3, tasks:2}" {
		t.Fatal("table_scan")
	}
	copStats := cop.stats["8.8.8.8"]
	if copStats == nil {
		t.Fatal("cop stats is nil")
	}
	copStats[0].SetRowNum(10)
	copStats[0].Record(time.Second, 10)
	if copStats[0].String() != "time:1.000000001s, loops:2" {
		t.Fatalf("cop stats string is not expect, got: %v", copStats[0].String())
	}

	if stats.GetCopStats(aggID).String() != "tikv_task:{proc max:4ns, min:3ns, p80:4ns, p95:4ns, iters:7, tasks:2}" {
		t.Fatal("agg")
	}
	rootStats := stats.GetRootStats(tableReaderID)
	if rootStats == nil {
		t.Fatal("table_reader")
	}
	if stats.ExistsRootStats(tableReaderID) == false {
		t.Fatal("table_reader not exists")
	}
}
func TestRuntimeStatsWithCommit(t *testing.T) {
	commitDetail := &CommitDetails{
		GetCommitTsTime:   time.Second,
		PrewriteTime:      time.Second,
		CommitTime:        time.Second,
		CommitBackoffTime: int64(time.Second),
		Mu: struct {
			sync.Mutex
			BackoffTypes []fmt.Stringer
		}{BackoffTypes: []fmt.Stringer{
			stringutil.MemoizeStr(func() string {
				return "backoff1"
			}),
			stringutil.MemoizeStr(func() string {
				return "backoff2"
			}),
			stringutil.MemoizeStr(func() string {
				return "backoff1"
			}),
		}},
		ResolveLockTime:   int64(time.Second),
		WriteKeys:         3,
		WriteSize:         66,
		PrewriteRegionNum: 5,
		TxnRetry:          2,
	}
	stats := &RuntimeStatsWithCommit{
		Commit: commitDetail,
	}
	expect := "commit_txn: {prewrite:1s, get_commit_ts:1s, commit:1s, backoff: {time: 1s, type: [backoff1 backoff2]}, resolve_lock: 1s, region_num:5, write_keys:3, write_byte:66, txn_retry:2}"
	if stats.String() != expect {
		t.Fatalf("%v != %v", stats.String(), expect)
	}
	lockDetail := &LockKeysDetails{
		TotalTime:       time.Second,
		RegionNum:       2,
		LockKeys:        10,
		ResolveLockTime: int64(time.Second * 2),
		BackoffTime:     int64(time.Second * 3),
		Mu: struct {
			sync.Mutex
			BackoffTypes []fmt.Stringer
		}{BackoffTypes: []fmt.Stringer{
			stringutil.MemoizeStr(func() string {
				return "backoff4"
			}),
			stringutil.MemoizeStr(func() string {
				return "backoff5"
			}),
			stringutil.MemoizeStr(func() string {
				return "backoff5"
			}),
		}},
		LockRPCTime:  int64(time.Second * 5),
		LockRPCCount: 50,
		RetryCount:   2,
	}
	stats = &RuntimeStatsWithCommit{
		LockKeys: lockDetail,
	}
	expect = "lock_keys: {time:1s, region:2, keys:10, resolve_lock:2s, backoff: {time: 3s, type: [backoff4 backoff5]}, lock_rpc:5s, rpc_count:50, retry_count:2}"
	if stats.String() != expect {
		t.Fatalf("%v != %v", stats.String(), expect)
	}
}

func TestRootRuntimeStats(t *testing.T) {
	basic1 := &BasicRuntimeStats{}
	basic2 := &BasicRuntimeStats{}
	basic1.Record(time.Second, 20)
	basic2.Record(time.Second*2, 30)
	pid := 1
	stmtStats := NewRuntimeStatsColl()
	stmtStats.RegisterStats(pid, basic1)
	stmtStats.RegisterStats(pid, basic2)
	concurrency := &RuntimeStatsWithConcurrencyInfo{}
	concurrency.SetConcurrencyInfo(NewConcurrencyInfo("worker", 15))
	stmtStats.RegisterStats(pid, concurrency)
	commitDetail := &CommitDetails{
		GetCommitTsTime:   time.Second,
		PrewriteTime:      time.Second,
		CommitTime:        time.Second,
		WriteKeys:         3,
		WriteSize:         66,
		PrewriteRegionNum: 5,
		TxnRetry:          2,
	}
	stmtStats.RegisterStats(pid, &RuntimeStatsWithCommit{
		Commit: commitDetail,
	})
	concurrency = &RuntimeStatsWithConcurrencyInfo{}
	concurrency.SetConcurrencyInfo(NewConcurrencyInfo("concurrent", 0))
	stmtStats.RegisterStats(pid, concurrency)
	stats := stmtStats.GetRootStats(1)
	expect := "time:3s, loops:2, worker:15, concurrent:OFF, commit_txn: {prewrite:1s, get_commit_ts:1s, commit:1s, region_num:5, write_keys:3, write_byte:66, txn_retry:2}"
	if stats.String() != expect {
		t.Fatalf("%v != %v", stats.String(), expect)
	}
}
