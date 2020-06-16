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
		CopTime:       time.Second + 3*time.Millisecond,
		ProcessTime:   2*time.Second + 5*time.Millisecond,
		WaitTime:      time.Second,
		BackoffTime:   time.Second,
		RequestCount:  1,
		TotalKeys:     100,
		ProcessedKeys: 10,
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
	}
	expected := "Cop_time: 1.003 Process_time: 2.005 Wait_time: 1 Backoff_time: 1 Request_count: 1 Total_keys: 100 Process_keys: 10 Prewrite_time: 1 Commit_time: 1 " +
		"Get_commit_ts_time: 1 Commit_backoff_time: 1 Backoff_types: [backoff1 backoff2] Resolve_lock_time: 1 Local_latch_wait_time: 1 Write_keys: 1 Write_size: 1 Prewrite_region: 1 Txn_retry: 1"
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

func TestCopRuntimeStats(t *testing.T) {
	stats := NewRuntimeStatsColl()
	stats.RecordOneCopTask("table_scan", "8.8.8.8", mockExecutorExecutionSummary(1, 1, 1))
	stats.RecordOneCopTask("table_scan", "8.8.8.9", mockExecutorExecutionSummary(2, 2, 2))
	stats.RecordOneCopTask("agg", "8.8.8.8", mockExecutorExecutionSummary(3, 3, 3))
	stats.RecordOneCopTask("agg", "8.8.8.9", mockExecutorExecutionSummary(4, 4, 4))
	if stats.ExistsCopStats("table_scan") != true {
		t.Fatal("exist")
	}
	cop := stats.GetCopStats("table_scan")
	if cop.String() != "proc max:2ns, min:1ns, p80:2ns, p95:2ns, iters:3, tasks:2" {
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

	if stats.GetCopStats("agg").String() != "proc max:4ns, min:3ns, p80:4ns, p95:4ns, iters:7, tasks:2" {
		t.Fatal("agg")
	}
	rootStats := stats.GetRootStats("table_reader")
	if rootStats == nil {
		t.Fatal("table_reader")
	}
	if stats.ExistsRootStats("table_reader") == false {
		t.Fatal("table_reader not exists")
	}
}

func TestReaderStats(t *testing.T) {
	r := new(ReaderRuntimeStats)
	if r.String() != "" {
		t.Fatal()
	}

	r.procKeys = append(r.procKeys, 100)
	r.copRespTime = append(r.copRespTime, time.Millisecond*100)
	if r.String() != "rpc num: 1, rpc time:100ms, proc keys:100" {
		t.Fatal()
	}

	for i := 0; i < 100; i++ {
		r.procKeys = append(r.procKeys, int64(i))
		r.copRespTime = append(r.copRespTime, time.Millisecond*time.Duration(i))
	}
	if r.String() != "rpc num: 101, rpc max:100ms, min:0s, avg:50ms, p80:80ms, p95:95ms, proc keys max:100, p95:95" {
		t.Fatal()
	}
}
