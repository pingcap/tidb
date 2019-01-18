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
	"testing"
	"time"
)

func TestString(t *testing.T) {
	detail := &ExecDetails{
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
			TotalBackoffTime:  time.Second,
			ResolveLockTime:   1000000000, // 10^9 ns = 1s
			WriteKeys:         1,
			WriteSize:         1,
			PrewriteRegionNum: 1,
			TxnRetry:          1,
		},
	}
	expected := "process_time:2.005s wait_time:1s backoff_time:1s request_count:1 total_keys:100 processed_keys:10 prewrite_time:1s commit_time:1s get_commit_ts_time:1s total_backoff_time:1s resolve_lock_time:1s local_latch_wait_time:1s write_keys:1 write_size:1 prewrite_region:1 txn_retry:1"
	if str := detail.String(); str != expected {
		t.Errorf("got:\n%s\nexpected:\n%s", str, expected)
	}
	detail = &ExecDetails{}
	if str := detail.String(); str != "" {
		t.Errorf("got:\n%s\nexpected:\n", str)
	}
}

func TestCopRuntimeStats(t *testing.T) {
	stats := NewRuntimeStatsColl()
	stats.RecordOneCopTask("table_scan", "8.8.8.8", 1, 1, 1)
	stats.RecordOneCopTask("table_scan", "8.8.8.9", 2, 2, 2)
	stats.RecordOneCopTask("agg", "8.8.8.8", 3, 3, 3)
	stats.RecordOneCopTask("agg", "8.8.8.9", 4, 4, 4)
	if stats.CopSummary("table_scan") != "procNs max:2, min:1, p80:2, p95:2, rows:3, iters:3, tasks:2" {
		t.Fatal("table_scan")
	}
	if stats.CopSummary("agg") != "procNs max:4, min:3, p80:4, p95:4, rows:7, iters:7, tasks:2" {
		t.Fatal("agg")
	}
}
