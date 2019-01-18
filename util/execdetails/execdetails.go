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
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

// CommitDetailCtxKey presents CommitDetail info key in context.
const CommitDetailCtxKey = "commitDetail"

// ExecDetails contains execution detail information.
type ExecDetails struct {
	CalleeAddress string
	ProcessTime   time.Duration
	WaitTime      time.Duration
	BackoffTime   time.Duration
	RequestCount  int
	TotalKeys     int64
	ProcessedKeys int64
	CommitDetail  *CommitDetails
}

// CommitDetails contains commit detail information.
type CommitDetails struct {
	GetCommitTsTime   time.Duration
	PrewriteTime      time.Duration
	CommitTime        time.Duration
	LocalLatchTime    time.Duration
	TotalBackoffTime  time.Duration
	ResolveLockTime   int64
	WriteKeys         int
	WriteSize         int
	PrewriteRegionNum int32
	TxnRetry          int
}

// String implements the fmt.Stringer interface.
func (d ExecDetails) String() string {
	parts := make([]string, 0, 6)
	if d.ProcessTime > 0 {
		parts = append(parts, fmt.Sprintf("process_time:%vs", d.ProcessTime.Seconds()))
	}
	if d.WaitTime > 0 {
		parts = append(parts, fmt.Sprintf("wait_time:%vs", d.WaitTime.Seconds()))
	}
	if d.BackoffTime > 0 {
		parts = append(parts, fmt.Sprintf("backoff_time:%vs", d.BackoffTime.Seconds()))
	}
	if d.RequestCount > 0 {
		parts = append(parts, fmt.Sprintf("request_count:%d", d.RequestCount))
	}
	if d.TotalKeys > 0 {
		parts = append(parts, fmt.Sprintf("total_keys:%d", d.TotalKeys))
	}
	if d.ProcessedKeys > 0 {
		parts = append(parts, fmt.Sprintf("processed_keys:%d", d.ProcessedKeys))
	}
	commitDetails := d.CommitDetail
	if commitDetails != nil {
		if commitDetails.PrewriteTime > 0 {
			parts = append(parts, fmt.Sprintf("prewrite_time:%vs", commitDetails.PrewriteTime.Seconds()))
		}
		if commitDetails.CommitTime > 0 {
			parts = append(parts, fmt.Sprintf("commit_time:%vs", commitDetails.CommitTime.Seconds()))
		}
		if commitDetails.GetCommitTsTime > 0 {
			parts = append(parts, fmt.Sprintf("get_commit_ts_time:%vs", commitDetails.GetCommitTsTime.Seconds()))
		}
		if commitDetails.TotalBackoffTime > 0 {
			parts = append(parts, fmt.Sprintf("total_backoff_time:%vs", commitDetails.TotalBackoffTime.Seconds()))
		}
		resolveLockTime := atomic.LoadInt64(&commitDetails.ResolveLockTime)
		if resolveLockTime > 0 {
			parts = append(parts, fmt.Sprintf("resolve_lock_time:%vs", time.Duration(resolveLockTime).Seconds()))
		}
		if commitDetails.LocalLatchTime > 0 {
			parts = append(parts, fmt.Sprintf("local_latch_wait_time:%vs", commitDetails.LocalLatchTime.Seconds()))
		}
		if commitDetails.WriteKeys > 0 {
			parts = append(parts, fmt.Sprintf("write_keys:%d", commitDetails.WriteKeys))
		}
		if commitDetails.WriteSize > 0 {
			parts = append(parts, fmt.Sprintf("write_size:%d", commitDetails.WriteSize))
		}
		prewriteRegionNum := atomic.LoadInt32(&commitDetails.PrewriteRegionNum)
		if prewriteRegionNum > 0 {
			parts = append(parts, fmt.Sprintf("prewrite_region:%d", prewriteRegionNum))
		}
		if commitDetails.TxnRetry > 0 {
			parts = append(parts, fmt.Sprintf("txn_retry:%d", commitDetails.TxnRetry))
		}
	}
	return strings.Join(parts, " ")
}

// RuntimeStatsColl collects executors's execution info.
type RuntimeStatsColl struct {
	mu    sync.Mutex
	stats map[string]*RuntimeStats
	// Usually, CopTasks are executed on TiKV cluster consist of multiple instances,
	// so coprocessors' addresses must be taken into account.
	// And Sometimes, several cop tasks of one operator can be executed in a same TiKV instance,
	// which depend on how we split cop tasks and the distribution of data regions.
	// So we have to use a list to maintain all tasks executed on each instance.
	// Then the type is map[operator ID]map[TiKV instance address][all tasks executed on this instance].
	copStats map[string]map[string][]*CopRuntimeStats
}

// RuntimeStats collects one executor's execution info.
type RuntimeStats struct {
	// executor's Next() called times.
	loop int32
	// executor consume time.
	consume int64
	// executor return row count.
	rows int64
}

// CopRuntimeStats collects info of one executor executed on TiKV.
type CopRuntimeStats struct {
	// Total time cost in this executor. Includes self time cost and children time cost.
	timeProcessedNs uint64
	// How many rows this executor produced totally.
	numProducedRows uint64
	// How many times executor's `next()` is called.
	numIterations uint64
}

// NewRuntimeStatsColl creates new executor collector.
func NewRuntimeStatsColl() *RuntimeStatsColl {
	return &RuntimeStatsColl{stats: make(map[string]*RuntimeStats),
		copStats: make(map[string]map[string][]*CopRuntimeStats)}
}

// Get gets execStat for a executor.
func (e *RuntimeStatsColl) Get(planID string) *RuntimeStats {
	e.mu.Lock()
	defer e.mu.Unlock()
	runtimeStats, exists := e.stats[planID]
	if !exists {
		runtimeStats = &RuntimeStats{}
		e.stats[planID] = runtimeStats
	}
	return runtimeStats
}

// RecordOneCopTask records a specific cop tasks's execution detail.
func (e *RuntimeStatsColl) RecordOneCopTask(planID, address string,
	timeProcessedNs, numProducedRows, numIterations uint64) {
	e.mu.Lock()
	defer e.mu.Unlock()

	_, ok := e.copStats[planID]
	if !ok {
		e.copStats[planID] = make(map[string][]*CopRuntimeStats, 8)
	}

	e.copStats[planID][address] = append(e.copStats[planID][address],
		&CopRuntimeStats{timeProcessedNs, numProducedRows, numIterations})
}

// CopSummary gets a summary of the cop task's execution information.
func (e *RuntimeStatsColl) CopSummary(planID string) string {
	e.mu.Lock()
	defer e.mu.Unlock()
	stats, ok := e.copStats[planID]
	if !ok {
		return ""
	}
	var totalRows, totalIters, totalTasks uint64
	procTimes := make([]uint64, 0, 32)
	for _, instanceStats := range stats {
		for _, stat := range instanceStats {
			procTimes = append(procTimes, stat.timeProcessedNs)
			totalRows += stat.numProducedRows
			totalIters += stat.numIterations
			totalTasks++
		}
	}

	n := len(procTimes)
	sort.Slice(procTimes, func(i, j int) bool { return procTimes[i] < procTimes[j] })
	return fmt.Sprintf("procNs max:%v, min:%v, p80:%v, p95:%v, rows:%v, iters:%v, tasks:%v",
		procTimes[n-1], procTimes[0], procTimes[n*4/5], procTimes[n*19/20], totalRows, totalIters, totalTasks)
}

// Exists checks if the planID exists in the stats collection.
func (e *RuntimeStatsColl) Exists(planID string) bool {
	e.mu.Lock()
	defer e.mu.Unlock()
	_, exists := e.stats[planID]
	return exists
}

// Record records executor's execution.
func (e *RuntimeStats) Record(d time.Duration, rowNum int) {
	atomic.AddInt32(&e.loop, 1)
	atomic.AddInt64(&e.consume, int64(d))
	atomic.AddInt64(&e.rows, int64(rowNum))
}

// SetRowNum sets the row num.
func (e *RuntimeStats) SetRowNum(rowNum int64) {
	atomic.StoreInt64(&e.rows, rowNum)
}

func (e *RuntimeStats) String() string {
	if e == nil {
		return ""
	}
	return fmt.Sprintf("time:%v, loops:%d, rows:%d", time.Duration(e.consume), e.loop, e.rows)
}
