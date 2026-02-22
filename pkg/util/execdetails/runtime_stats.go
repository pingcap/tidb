// Copyright 2025 PingCAP, Inc.
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
	"bytes"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tipb/go-tipb"
	"github.com/tikv/client-go/v2/util"
)

const (
	// TpBasicRuntimeStats is the tp for BasicRuntimeStats.
	TpBasicRuntimeStats int = iota
	// TpRuntimeStatsWithCommit is the tp for RuntimeStatsWithCommit.
	TpRuntimeStatsWithCommit
	// TpRuntimeStatsWithConcurrencyInfo is the tp for RuntimeStatsWithConcurrencyInfo.
	TpRuntimeStatsWithConcurrencyInfo
	// TpSnapshotRuntimeStats is the tp for SnapshotRuntimeStats.
	TpSnapshotRuntimeStats
	// TpHashJoinRuntimeStats is the tp for HashJoinRuntimeStats.
	TpHashJoinRuntimeStats
	// TpHashJoinRuntimeStatsV2 is the tp for hashJoinRuntimeStatsV2.
	TpHashJoinRuntimeStatsV2
	// TpIndexLookUpJoinRuntimeStats is the tp for IndexLookUpJoinRuntimeStats.
	TpIndexLookUpJoinRuntimeStats
	// TpRuntimeStatsWithSnapshot is the tp for RuntimeStatsWithSnapshot.
	TpRuntimeStatsWithSnapshot
	// TpJoinRuntimeStats is the tp for JoinRuntimeStats.
	TpJoinRuntimeStats
	// TpSelectResultRuntimeStats is the tp for SelectResultRuntimeStats.
	TpSelectResultRuntimeStats
	// TpInsertRuntimeStat is the tp for InsertRuntimeStat
	TpInsertRuntimeStat
	// TpIndexLookUpRunTimeStats is the tp for IndexLookUpRunTimeStats
	TpIndexLookUpRunTimeStats
	// TpSlowQueryRuntimeStat is the tp for SlowQueryRuntimeStat
	TpSlowQueryRuntimeStat
	// TpHashAggRuntimeStat is the tp for HashAggRuntimeStat
	TpHashAggRuntimeStat
	// TpIndexMergeRunTimeStats is the tp for IndexMergeRunTimeStats
	TpIndexMergeRunTimeStats
	// TpBasicCopRunTimeStats is the tp for BasicCopRunTimeStats
	TpBasicCopRunTimeStats
	// TpUpdateRuntimeStats is the tp for UpdateRuntimeStats
	TpUpdateRuntimeStats
	// TpFKCheckRuntimeStats is the tp for FKCheckRuntimeStats
	TpFKCheckRuntimeStats
	// TpFKCascadeRuntimeStats is the tp for FKCascadeRuntimeStats
	TpFKCascadeRuntimeStats
	// TpRURuntimeStats is the tp for RURuntimeStats
	TpRURuntimeStats
)

// RuntimeStats is used to express the executor runtime information.
type RuntimeStats interface {
	String() string
	Merge(RuntimeStats)
	Clone() RuntimeStats
	Tp() int
}

type basicCopRuntimeStats struct {
	loop      int32
	rows      int64
	threads   int32
	procTimes Percentile[Duration]
	// executor extra infos
	tiflashStats *TiflashStats
}

// String implements the RuntimeStats interface.
func (e *basicCopRuntimeStats) String() string {
	buf := bytes.NewBuffer(make([]byte, 0, 16))
	buf.WriteString("time:")
	buf.WriteString(FormatDuration(time.Duration(e.procTimes.sumVal)))
	buf.WriteString(", loops:")
	buf.WriteString(strconv.Itoa(int(e.loop)))
	if e.tiflashStats != nil {
		buf.WriteString(", threads:")
		buf.WriteString(strconv.Itoa(int(e.threads)))
		if !e.tiflashStats.waitSummary.CanBeIgnored() {
			buf.WriteString(", ")
			buf.WriteString(e.tiflashStats.waitSummary.String())
		}
		if !e.tiflashStats.networkSummary.Empty() {
			buf.WriteString(", ")
			buf.WriteString(e.tiflashStats.networkSummary.String())
		}
		buf.WriteString(", ")
		buf.WriteString(e.tiflashStats.scanContext.String())
	}
	return buf.String()
}

// Clone implements the RuntimeStats interface.
func (e *basicCopRuntimeStats) Clone() RuntimeStats {
	stats := &basicCopRuntimeStats{
		loop:      e.loop,
		rows:      e.rows,
		threads:   e.threads,
		procTimes: e.procTimes,
	}
	if e.tiflashStats != nil {
		stats.tiflashStats = &TiflashStats{
			scanContext:    e.tiflashStats.scanContext.Clone(),
			waitSummary:    e.tiflashStats.waitSummary.Clone(),
			networkSummary: e.tiflashStats.networkSummary.Clone(),
		}
	}
	return stats
}

// Merge implements the RuntimeStats interface.
func (e *basicCopRuntimeStats) Merge(rs RuntimeStats) {
	tmp, ok := rs.(*basicCopRuntimeStats)
	if !ok {
		return
	}
	e.loop += tmp.loop
	e.rows += tmp.rows
	e.threads += tmp.threads
	if tmp.procTimes.Size() > 0 {
		e.procTimes.MergePercentile(&tmp.procTimes)
	}
	if tmp.tiflashStats != nil {
		if e.tiflashStats == nil {
			e.tiflashStats = &TiflashStats{}
		}
		e.tiflashStats.scanContext.Merge(tmp.tiflashStats.scanContext)
		e.tiflashStats.waitSummary.Merge(tmp.tiflashStats.waitSummary)
		e.tiflashStats.networkSummary.Merge(tmp.tiflashStats.networkSummary)
	}
}

// mergeExecSummary likes Merge, but it merges ExecutorExecutionSummary directly.
func (e *basicCopRuntimeStats) mergeExecSummary(summary *tipb.ExecutorExecutionSummary) {
	e.loop += (int32(*summary.NumIterations))
	e.rows += (int64(*summary.NumProducedRows))
	e.threads += int32(summary.GetConcurrency())
	e.procTimes.Add(Duration(int64(*summary.TimeProcessedNs)))
	if tiflashScanContext := summary.GetTiflashScanContext(); tiflashScanContext != nil {
		if e.tiflashStats == nil {
			e.tiflashStats = &TiflashStats{}
		}
		e.tiflashStats.scanContext.mergeExecSummary(tiflashScanContext)
	}
	if tiflashWaitSummary := summary.GetTiflashWaitSummary(); tiflashWaitSummary != nil {
		if e.tiflashStats == nil {
			e.tiflashStats = &TiflashStats{}
		}
		e.tiflashStats.waitSummary.mergeExecSummary(tiflashWaitSummary, *summary.TimeProcessedNs)
	}
	if tiflashNetworkSummary := summary.GetTiflashNetworkSummary(); tiflashNetworkSummary != nil {
		if e.tiflashStats == nil {
			e.tiflashStats = &TiflashStats{}
		}
		e.tiflashStats.networkSummary.mergeExecSummary(tiflashNetworkSummary)
	}
}

// Tp implements the RuntimeStats interface.
func (*basicCopRuntimeStats) Tp() int {
	return TpBasicCopRunTimeStats
}

// StmtCopRuntimeStats stores the cop runtime stats of the total statement
type StmtCopRuntimeStats struct {
	// TiflashNetworkStats stats all mpp tasks' network traffic info, nil if no any mpp tasks' network traffic
	TiflashNetworkStats *TiFlashNetworkTrafficSummary
}

// mergeExecSummary merges ExecutorExecutionSummary into stmt cop runtime stats directly.
func (e *StmtCopRuntimeStats) mergeExecSummary(summary *tipb.ExecutorExecutionSummary) {
	if tiflashNetworkSummary := summary.GetTiflashNetworkSummary(); tiflashNetworkSummary != nil {
		if e.TiflashNetworkStats == nil {
			e.TiflashNetworkStats = &TiFlashNetworkTrafficSummary{}
		}
		e.TiflashNetworkStats.mergeExecSummary(tiflashNetworkSummary)
	}
}

// CopRuntimeStats collects cop tasks' execution info.
type CopRuntimeStats struct {
	// stats stores the runtime statistics of coprocessor tasks.
	// The key of the map is the tikv-server address. Because a tikv-server can
	// have many region leaders, several coprocessor tasks can be sent to the
	// same tikv-server instance. We have to use a list to maintain all tasks
	// executed on each instance.
	stats      basicCopRuntimeStats
	scanDetail util.ScanDetail
	timeDetail util.TimeDetail
	storeType  kv.StoreType
}

// GetActRows return total rows of CopRuntimeStats.
func (crs *CopRuntimeStats) GetActRows() int64 {
	return crs.stats.rows
}

// GetTasks return total tasks of CopRuntimeStats
func (crs *CopRuntimeStats) GetTasks() int32 {
	return int32(crs.stats.procTimes.size)
}

var zeroTimeDetail = util.TimeDetail{}

func (crs *CopRuntimeStats) String() string {
	procTimes := crs.stats.procTimes
	totalTasks := procTimes.size
	isTiFlashCop := crs.storeType == kv.TiFlash
	buf := bytes.NewBuffer(make([]byte, 0, 16))
	{
		printTiFlashSpecificInfo := func() {
			if isTiFlashCop {
				buf.WriteString(", ")
				buf.WriteString("threads:")
				buf.WriteString(strconv.Itoa(int(crs.stats.threads)))
				buf.WriteString("}")
				if crs.stats.tiflashStats != nil {
					if !crs.stats.tiflashStats.waitSummary.CanBeIgnored() {
						buf.WriteString(", ")
						buf.WriteString(crs.stats.tiflashStats.waitSummary.String())
					}
					if !crs.stats.tiflashStats.networkSummary.Empty() {
						buf.WriteString(", ")
						buf.WriteString(crs.stats.tiflashStats.networkSummary.String())
					}
					if !crs.stats.tiflashStats.scanContext.Empty() {
						buf.WriteString(", ")
						buf.WriteString(crs.stats.tiflashStats.scanContext.String())
					}
				}
			} else {
				buf.WriteString("}")
			}
		}
		if totalTasks == 1 {
			buf.WriteString(crs.storeType.Name())
			buf.WriteString("_task:{time:")
			buf.WriteString(FormatDuration(time.Duration(procTimes.GetPercentile(0))))
			buf.WriteString(", loops:")
			buf.WriteString(strconv.Itoa(int(crs.stats.loop)))
			printTiFlashSpecificInfo()
		} else if totalTasks > 0 {
			buf.WriteString(crs.storeType.Name())
			buf.WriteString("_task:{proc max:")
			buf.WriteString(FormatDuration(time.Duration(procTimes.GetMax().GetFloat64())))
			buf.WriteString(", min:")
			buf.WriteString(FormatDuration(time.Duration(procTimes.GetMin().GetFloat64())))
			buf.WriteString(", avg: ")
			buf.WriteString(FormatDuration(time.Duration(int64(procTimes.Sum()) / int64(totalTasks))))
			buf.WriteString(", p80:")
			buf.WriteString(FormatDuration(time.Duration(procTimes.GetPercentile(0.8))))
			buf.WriteString(", p95:")
			buf.WriteString(FormatDuration(time.Duration(procTimes.GetPercentile(0.95))))
			buf.WriteString(", iters:")
			buf.WriteString(strconv.Itoa(int(crs.stats.loop)))
			buf.WriteString(", tasks:")
			buf.WriteString(strconv.Itoa(totalTasks))
			printTiFlashSpecificInfo()
		}
	}
	if !isTiFlashCop {
		detail := crs.scanDetail.String()
		if detail != "" {
			buf.WriteString(", ")
			buf.WriteString(detail)
		}
		if crs.timeDetail != zeroTimeDetail {
			timeDetailStr := crs.timeDetail.String()
			if timeDetailStr != "" {
				buf.WriteString(", ")
				buf.WriteString(timeDetailStr)
			}
		}
	}
	return buf.String()
}

// BasicRuntimeStats is the basic runtime stats.
type BasicRuntimeStats struct {
	// the count of executors with the same id
	executorCount atomic.Int32
	// executor's Next() called times.
	loop atomic.Int32
	// executor consume time, including open, next, and close time.
	consume atomic.Int64
	// executor open time.
	open atomic.Int64
	// executor close time.
	close atomic.Int64
	// executor return row count.
	rows atomic.Int64
}

// GetActRows return total rows of BasicRuntimeStats.
func (e *BasicRuntimeStats) GetActRows() int64 {
	return e.rows.Load()
}

// Clone implements the RuntimeStats interface.
// BasicRuntimeStats shouldn't implement Clone interface because all executors with the same executor_id
// should share the same BasicRuntimeStats, duplicated BasicRuntimeStats are easy to cause mistakes.
func (*BasicRuntimeStats) Clone() RuntimeStats {
	panic("BasicRuntimeStats should not implement Clone function")
}

// Merge implements the RuntimeStats interface.
func (e *BasicRuntimeStats) Merge(rs RuntimeStats) {
	tmp, ok := rs.(*BasicRuntimeStats)
	if !ok {
		return
	}
	e.loop.Add(tmp.loop.Load())
	e.consume.Add(tmp.consume.Load())
	e.open.Add(tmp.open.Load())
	e.close.Add(tmp.close.Load())
	e.rows.Add(tmp.rows.Load())
}

// Tp implements the RuntimeStats interface.
func (*BasicRuntimeStats) Tp() int {
	return TpBasicRuntimeStats
}

// RootRuntimeStats is the executor runtime stats that combine with multiple runtime stats.
type RootRuntimeStats struct {
	basic    *BasicRuntimeStats
	groupRss []RuntimeStats
}

// NewRootRuntimeStats returns a new RootRuntimeStats
func NewRootRuntimeStats() *RootRuntimeStats {
	return &RootRuntimeStats{}
}

// GetActRows return total rows of RootRuntimeStats.
func (e *RootRuntimeStats) GetActRows() int64 {
	if e.basic == nil {
		return 0
	}
	return e.basic.rows.Load()
}

// MergeStats merges stats in the RootRuntimeStats and return the stats suitable for display directly.
func (e *RootRuntimeStats) MergeStats() (basic *BasicRuntimeStats, groups []RuntimeStats) {
	return e.basic, e.groupRss
}

// String implements the RuntimeStats interface.
func (e *RootRuntimeStats) String() string {
	basic, groups := e.MergeStats()
	strs := make([]string, 0, len(groups)+1)
	if basic != nil {
		strs = append(strs, basic.String())
	}
	for _, group := range groups {
		str := group.String()
		if len(str) > 0 {
			strs = append(strs, str)
		}
	}
	return strings.Join(strs, ", ")
}

// Record records executor's execution.
func (e *BasicRuntimeStats) Record(d time.Duration, rowNum int) {
	e.loop.Add(1)
	e.consume.Add(int64(d))
	e.rows.Add(int64(rowNum))
}

// RecordOpen records executor's open time.
func (e *BasicRuntimeStats) RecordOpen(d time.Duration) {
	e.consume.Add(int64(d))
	e.open.Add(int64(d))
}

// RecordClose records executor's close time.
func (e *BasicRuntimeStats) RecordClose(d time.Duration) {
	e.consume.Add(int64(d))
	e.close.Add(int64(d))
}

// SetRowNum sets the row num.
func (e *BasicRuntimeStats) SetRowNum(rowNum int64) {
	e.rows.Store(rowNum)
}

// String implements the RuntimeStats interface.
func (e *BasicRuntimeStats) String() string {
	if e == nil {
		return ""
	}
	var str strings.Builder
	timePrefix := ""
	if e.executorCount.Load() > 1 {
		timePrefix = "total_"
	}
	totalTime := e.consume.Load()
	openTime := e.open.Load()
	closeTime := e.close.Load()
	str.WriteString(timePrefix)
	str.WriteString("time:")
	str.WriteString(FormatDuration(time.Duration(totalTime)))
	str.WriteString(", ")
	str.WriteString(timePrefix)
	str.WriteString("open:")
	str.WriteString(FormatDuration(time.Duration(openTime)))
	str.WriteString(", ")
	str.WriteString(timePrefix)
	str.WriteString("close:")
	str.WriteString(FormatDuration(time.Duration(closeTime)))
	str.WriteString(", loops:")
	str.WriteString(strconv.FormatInt(int64(e.loop.Load()), 10))
	return str.String()
}

// GetTime get the int64 total time
func (e *BasicRuntimeStats) GetTime() int64 {
	return e.consume.Load()
}

// RuntimeStatsColl collects executors's execution info.
type RuntimeStatsColl struct {
	rootStats    map[int]*RootRuntimeStats
	copStats     map[int]*CopRuntimeStats
	stmtCopStats StmtCopRuntimeStats
	mu           sync.Mutex
}

// NewRuntimeStatsColl creates new executor collector.
// Reuse the object to reduce allocation when *RuntimeStatsColl is not nil.
