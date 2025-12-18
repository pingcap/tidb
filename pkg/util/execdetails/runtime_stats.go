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
	"slices"
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
func NewRuntimeStatsColl(reuse *RuntimeStatsColl) *RuntimeStatsColl {
	if reuse != nil {
		// Reuse map is cheaper than create a new map object.
		// Go compiler optimize this cleanup code pattern to a clearmap() function.
		reuse.mu.Lock()
		defer reuse.mu.Unlock()
		for k := range reuse.rootStats {
			delete(reuse.rootStats, k)
		}
		for k := range reuse.copStats {
			delete(reuse.copStats, k)
		}
		return reuse
	}
	return &RuntimeStatsColl{
		rootStats: make(map[int]*RootRuntimeStats),
		copStats:  make(map[int]*CopRuntimeStats),
	}
}

// RegisterStats register execStat for a executor.
func (e *RuntimeStatsColl) RegisterStats(planID int, info RuntimeStats) {
	e.mu.Lock()
	defer e.mu.Unlock()
	stats, ok := e.rootStats[planID]
	if !ok {
		stats = NewRootRuntimeStats()
		e.rootStats[planID] = stats
	}
	tp := info.Tp()
	found := false
	for _, rss := range stats.groupRss {
		if rss.Tp() == tp {
			rss.Merge(info)
			found = true
			break
		}
	}
	if !found {
		stats.groupRss = append(stats.groupRss, info)
	}
}

// GetBasicRuntimeStats gets basicRuntimeStats for a executor
// When rootStat/rootStat's basicRuntimeStats is nil, the behavior is decided by initNewExecutorStats argument:
// 1. If true, it created a new one, and increase basicRuntimeStats' executorCount
// 2. Else, it returns nil
func (e *RuntimeStatsColl) GetBasicRuntimeStats(planID int, initNewExecutorStats bool) *BasicRuntimeStats {
	e.mu.Lock()
	defer e.mu.Unlock()
	stats, ok := e.rootStats[planID]
	if !ok && initNewExecutorStats {
		stats = NewRootRuntimeStats()
		e.rootStats[planID] = stats
	}
	if stats == nil {
		return nil
	}

	if stats.basic == nil && initNewExecutorStats {
		stats.basic = &BasicRuntimeStats{}
		stats.basic.executorCount.Add(1)
	} else if stats.basic != nil && initNewExecutorStats {
		stats.basic.executorCount.Add(1)
	}
	return stats.basic
}

// GetStmtCopRuntimeStats gets execStat for a executor.
func (e *RuntimeStatsColl) GetStmtCopRuntimeStats() StmtCopRuntimeStats {
	return e.stmtCopStats
}

// GetRootStats gets execStat for a executor.
func (e *RuntimeStatsColl) GetRootStats(planID int) *RootRuntimeStats {
	e.mu.Lock()
	defer e.mu.Unlock()
	runtimeStats, exists := e.rootStats[planID]
	if !exists {
		runtimeStats = NewRootRuntimeStats()
		e.rootStats[planID] = runtimeStats
	}
	return runtimeStats
}

// GetPlanActRows returns the actual rows of the plan.
func (e *RuntimeStatsColl) GetPlanActRows(planID int) int64 {
	e.mu.Lock()
	defer e.mu.Unlock()
	runtimeStats, exists := e.rootStats[planID]
	if !exists {
		return 0
	}
	return runtimeStats.GetActRows()
}

// GetCopStats gets the CopRuntimeStats specified by planID.
func (e *RuntimeStatsColl) GetCopStats(planID int) *CopRuntimeStats {
	e.mu.Lock()
	defer e.mu.Unlock()
	copStats, ok := e.copStats[planID]
	if !ok {
		return nil
	}
	return copStats
}

// GetCopStatsTaskNum returns total tasks of CopRuntimeStats that specified by planID
func (e *RuntimeStatsColl) GetCopStatsTaskNum(planID int) int32 {
	e.mu.Lock()
	defer e.mu.Unlock()
	copStats, ok := e.copStats[planID]
	if !ok {
		return 0
	}
	return copStats.GetTasks()
}

// GetCopCountAndRows returns the total cop-tasks count and total rows of all cop-tasks.
func (e *RuntimeStatsColl) GetCopCountAndRows(planID int) (int32, int64) {
	e.mu.Lock()
	defer e.mu.Unlock()
	copStats, ok := e.copStats[planID]
	if !ok {
		return 0, 0
	}
	return copStats.GetTasks(), copStats.GetActRows()
}

func getPlanIDFromExecutionSummary(summary *tipb.ExecutorExecutionSummary) (int, bool) {
	if summary.GetExecutorId() != "" {
		strs := strings.Split(summary.GetExecutorId(), "_")
		if id, err := strconv.Atoi(strs[len(strs)-1]); err == nil {
			return id, true
		}
	}
	return 0, false
}

// RecordCopStats records a specific cop tasks's execution detail.
func (e *RuntimeStatsColl) RecordCopStats(planID int, storeType kv.StoreType, scan *util.ScanDetail, time util.TimeDetail, summary *tipb.ExecutorExecutionSummary) int {
	e.mu.Lock()
	defer e.mu.Unlock()
	copStats, ok := e.copStats[planID]
	if !ok {
		copStats = &CopRuntimeStats{
			timeDetail: time,
			storeType:  storeType,
		}
		if scan != nil {
			copStats.scanDetail = *scan
		}
		e.copStats[planID] = copStats
	} else {
		if scan != nil {
			copStats.scanDetail.Merge(scan)
		}
		copStats.timeDetail.Merge(&time)
	}
	if summary != nil {
		// for TiFlash cop response, ExecutorExecutionSummary contains executor id, so if there is a valid executor id in
		// summary, use it overwrite the planID
		id, valid := getPlanIDFromExecutionSummary(summary)
		if valid && id != planID {
			planID = id
			copStats, ok = e.copStats[planID]
			if !ok {
				copStats = &CopRuntimeStats{
					storeType: storeType,
				}
				e.copStats[planID] = copStats
			}
		}
		copStats.stats.mergeExecSummary(summary)
		e.stmtCopStats.mergeExecSummary(summary)
	}
	return planID
}

// RecordOneCopTask records a specific cop tasks's execution summary.
func (e *RuntimeStatsColl) RecordOneCopTask(planID int, storeType kv.StoreType, summary *tipb.ExecutorExecutionSummary) int {
	// for TiFlash cop response, ExecutorExecutionSummary contains executor id, so if there is a valid executor id in
	// summary, use it overwrite the planID
	if id, valid := getPlanIDFromExecutionSummary(summary); valid {
		planID = id
	}
	e.mu.Lock()
	defer e.mu.Unlock()
	copStats, ok := e.copStats[planID]
	if !ok {
		copStats = &CopRuntimeStats{
			storeType: storeType,
		}
		e.copStats[planID] = copStats
	}
	copStats.stats.mergeExecSummary(summary)
	e.stmtCopStats.mergeExecSummary(summary)
	return planID
}

// ExistsRootStats checks if the planID exists in the rootStats collection.
func (e *RuntimeStatsColl) ExistsRootStats(planID int) bool {
	e.mu.Lock()
	defer e.mu.Unlock()
	_, exists := e.rootStats[planID]
	return exists
}

// ExistsCopStats checks if the planID exists in the copStats collection.
func (e *RuntimeStatsColl) ExistsCopStats(planID int) bool {
	e.mu.Lock()
	defer e.mu.Unlock()
	_, exists := e.copStats[planID]
	return exists
}

// ConcurrencyInfo is used to save the concurrency information of the executor operator
type ConcurrencyInfo struct {
	concurrencyName string
	concurrencyNum  int
}

// NewConcurrencyInfo creates new executor's concurrencyInfo.
func NewConcurrencyInfo(name string, num int) *ConcurrencyInfo {
	return &ConcurrencyInfo{name, num}
}

// RuntimeStatsWithConcurrencyInfo is the BasicRuntimeStats with ConcurrencyInfo.
type RuntimeStatsWithConcurrencyInfo struct {
	// executor concurrency information
	concurrency []*ConcurrencyInfo
	// protect concurrency
	sync.Mutex
}

// Tp implements the RuntimeStats interface.
func (*RuntimeStatsWithConcurrencyInfo) Tp() int {
	return TpRuntimeStatsWithConcurrencyInfo
}

// SetConcurrencyInfo sets the concurrency informations.
// We must clear the concurrencyInfo first when we call the SetConcurrencyInfo.
// When the num <= 0, it means the exector operator is not executed parallel.
func (e *RuntimeStatsWithConcurrencyInfo) SetConcurrencyInfo(infos ...*ConcurrencyInfo) {
	e.Lock()
	defer e.Unlock()
	e.concurrency = e.concurrency[:0]
	e.concurrency = append(e.concurrency, infos...)
}

// Clone implements the RuntimeStats interface.
func (e *RuntimeStatsWithConcurrencyInfo) Clone() RuntimeStats {
	newRs := &RuntimeStatsWithConcurrencyInfo{
		concurrency: make([]*ConcurrencyInfo, 0, len(e.concurrency)),
	}
	newRs.concurrency = append(newRs.concurrency, e.concurrency...)
	return newRs
}

// String implements the RuntimeStats interface.
func (e *RuntimeStatsWithConcurrencyInfo) String() string {
	buf := bytes.NewBuffer(make([]byte, 0, 8))
	if len(e.concurrency) > 0 {
		for i, concurrency := range e.concurrency {
			if i > 0 {
				buf.WriteString(", ")
			}
			if concurrency.concurrencyNum > 0 {
				buf.WriteString(concurrency.concurrencyName)
				buf.WriteByte(':')
				buf.WriteString(strconv.Itoa(concurrency.concurrencyNum))
			} else {
				buf.WriteString(concurrency.concurrencyName)
				buf.WriteString(":OFF")
			}
		}
	}
	return buf.String()
}

// Merge implements the RuntimeStats interface.
func (*RuntimeStatsWithConcurrencyInfo) Merge(RuntimeStats) {}

// RuntimeStatsWithCommit is the RuntimeStats with commit detail.
type RuntimeStatsWithCommit struct {
	Commit   *util.CommitDetails
	LockKeys *util.LockKeysDetails
	TxnCnt   int
}

// Tp implements the RuntimeStats interface.
func (*RuntimeStatsWithCommit) Tp() int {
	return TpRuntimeStatsWithCommit
}

// MergeCommitDetails merges the commit details.
func (e *RuntimeStatsWithCommit) MergeCommitDetails(detail *util.CommitDetails) {
	if detail == nil {
		return
	}
	if e.Commit == nil {
		e.Commit = detail
		e.TxnCnt = 1
		return
	}
	e.Commit.Merge(detail)
	e.TxnCnt++
}

// Merge implements the RuntimeStats interface.
func (e *RuntimeStatsWithCommit) Merge(rs RuntimeStats) {
	tmp, ok := rs.(*RuntimeStatsWithCommit)
	if !ok {
		return
	}
	e.TxnCnt += tmp.TxnCnt
	if tmp.Commit != nil {
		if e.Commit == nil {
			e.Commit = &util.CommitDetails{}
		}
		e.Commit.Merge(tmp.Commit)
	}

	if tmp.LockKeys != nil {
		if e.LockKeys == nil {
			e.LockKeys = &util.LockKeysDetails{}
		}
		e.LockKeys.Merge(tmp.LockKeys)
	}
}

// Clone implements the RuntimeStats interface.
func (e *RuntimeStatsWithCommit) Clone() RuntimeStats {
	newRs := RuntimeStatsWithCommit{
		TxnCnt: e.TxnCnt,
	}
	if e.Commit != nil {
		newRs.Commit = e.Commit.Clone()
	}
	if e.LockKeys != nil {
		newRs.LockKeys = e.LockKeys.Clone()
	}
	return &newRs
}

// String implements the RuntimeStats interface.
func (e *RuntimeStatsWithCommit) String() string {
	buf := bytes.NewBuffer(make([]byte, 0, 32))
	if e.Commit != nil {
		buf.WriteString("commit_txn: {")
		// Only print out when there are more than 1 transaction.
		if e.TxnCnt > 1 {
			buf.WriteString("count: ")
			buf.WriteString(strconv.Itoa(e.TxnCnt))
			buf.WriteString(", ")
		}
		if e.Commit.PrewriteTime > 0 {
			buf.WriteString("prewrite:")
			buf.WriteString(FormatDuration(e.Commit.PrewriteTime))
		}
		if e.Commit.WaitPrewriteBinlogTime > 0 {
			buf.WriteString(", wait_prewrite_binlog:")
			buf.WriteString(FormatDuration(e.Commit.WaitPrewriteBinlogTime))
		}
		if e.Commit.GetCommitTsTime > 0 {
			buf.WriteString(", get_commit_ts:")
			buf.WriteString(FormatDuration(e.Commit.GetCommitTsTime))
		}
		if e.Commit.CommitTime > 0 {
			buf.WriteString(", commit:")
			buf.WriteString(FormatDuration(e.Commit.CommitTime))
		}
		e.Commit.Mu.Lock()
		commitBackoffTime := e.Commit.Mu.CommitBackoffTime
		if commitBackoffTime > 0 {
			buf.WriteString(", backoff: {time: ")
			buf.WriteString(FormatDuration(time.Duration(commitBackoffTime)))
			if len(e.Commit.Mu.PrewriteBackoffTypes) > 0 {
				buf.WriteString(", prewrite type: ")
				e.formatBackoff(buf, e.Commit.Mu.PrewriteBackoffTypes)
			}
			if len(e.Commit.Mu.CommitBackoffTypes) > 0 {
				buf.WriteString(", commit type: ")
				e.formatBackoff(buf, e.Commit.Mu.CommitBackoffTypes)
			}
			buf.WriteString("}")
		}
		if e.Commit.Mu.SlowestPrewrite.ReqTotalTime > 0 {
			buf.WriteString(", slowest_prewrite_rpc: {total: ")
			buf.WriteString(strconv.FormatFloat(e.Commit.Mu.SlowestPrewrite.ReqTotalTime.Seconds(), 'f', 3, 64))
			buf.WriteString("s, region_id: ")
			buf.WriteString(strconv.FormatUint(e.Commit.Mu.SlowestPrewrite.Region, 10))
			buf.WriteString(", store: ")
			buf.WriteString(e.Commit.Mu.SlowestPrewrite.StoreAddr)
			buf.WriteString(", ")
			buf.WriteString(e.Commit.Mu.SlowestPrewrite.ExecDetails.String())
			buf.WriteString("}")
		}
		if e.Commit.Mu.CommitPrimary.ReqTotalTime > 0 {
			buf.WriteString(", commit_primary_rpc: {total: ")
			buf.WriteString(strconv.FormatFloat(e.Commit.Mu.CommitPrimary.ReqTotalTime.Seconds(), 'f', 3, 64))
			buf.WriteString("s, region_id: ")
			buf.WriteString(strconv.FormatUint(e.Commit.Mu.CommitPrimary.Region, 10))
			buf.WriteString(", store: ")
			buf.WriteString(e.Commit.Mu.CommitPrimary.StoreAddr)
			buf.WriteString(", ")
			buf.WriteString(e.Commit.Mu.CommitPrimary.ExecDetails.String())
			buf.WriteString("}")
		}
		e.Commit.Mu.Unlock()
		if e.Commit.ResolveLock.ResolveLockTime > 0 {
			buf.WriteString(", resolve_lock: ")
			buf.WriteString(FormatDuration(time.Duration(e.Commit.ResolveLock.ResolveLockTime)))
		}

		prewriteRegionNum := atomic.LoadInt32(&e.Commit.PrewriteRegionNum)
		if prewriteRegionNum > 0 {
			buf.WriteString(", region_num:")
			buf.WriteString(strconv.FormatInt(int64(prewriteRegionNum), 10))
		}
		if e.Commit.WriteKeys > 0 {
			buf.WriteString(", write_keys:")
			buf.WriteString(strconv.FormatInt(int64(e.Commit.WriteKeys), 10))
		}
		if e.Commit.WriteSize > 0 {
			buf.WriteString(", write_byte:")
			buf.WriteString(strconv.FormatInt(int64(e.Commit.WriteSize), 10))
		}
		if e.Commit.TxnRetry > 0 {
			buf.WriteString(", txn_retry:")
			buf.WriteString(strconv.FormatInt(int64(e.Commit.TxnRetry), 10))
		}
		buf.WriteString("}")
	}
	if e.LockKeys != nil {
		if buf.Len() > 0 {
			buf.WriteString(", ")
		}
		buf.WriteString("lock_keys: {")
		if e.LockKeys.TotalTime > 0 {
			buf.WriteString("time:")
			buf.WriteString(FormatDuration(e.LockKeys.TotalTime))
		}
		if e.LockKeys.RegionNum > 0 {
			buf.WriteString(", region:")
			buf.WriteString(strconv.FormatInt(int64(e.LockKeys.RegionNum), 10))
		}
		if e.LockKeys.LockKeys > 0 {
			buf.WriteString(", keys:")
			buf.WriteString(strconv.FormatInt(int64(e.LockKeys.LockKeys), 10))
		}
		if e.LockKeys.ResolveLock.ResolveLockTime > 0 {
			buf.WriteString(", resolve_lock:")
			buf.WriteString(FormatDuration(time.Duration(e.LockKeys.ResolveLock.ResolveLockTime)))
		}
		e.LockKeys.Mu.Lock()
		if e.LockKeys.BackoffTime > 0 {
			buf.WriteString(", backoff: {time: ")
			buf.WriteString(FormatDuration(time.Duration(e.LockKeys.BackoffTime)))
			if len(e.LockKeys.Mu.BackoffTypes) > 0 {
				buf.WriteString(", type: ")
				e.formatBackoff(buf, e.LockKeys.Mu.BackoffTypes)
			}
			buf.WriteString("}")
		}
		if e.LockKeys.Mu.SlowestReqTotalTime > 0 {
			buf.WriteString(", slowest_rpc: {total: ")
			buf.WriteString(strconv.FormatFloat(e.LockKeys.Mu.SlowestReqTotalTime.Seconds(), 'f', 3, 64))
			buf.WriteString("s, region_id: ")
			buf.WriteString(strconv.FormatUint(e.LockKeys.Mu.SlowestRegion, 10))
			buf.WriteString(", store: ")
			buf.WriteString(e.LockKeys.Mu.SlowestStoreAddr)
			buf.WriteString(", ")
			buf.WriteString(e.LockKeys.Mu.SlowestExecDetails.String())
			buf.WriteString("}")
		}
		e.LockKeys.Mu.Unlock()
		if e.LockKeys.LockRPCTime > 0 {
			buf.WriteString(", lock_rpc:")
			buf.WriteString(time.Duration(e.LockKeys.LockRPCTime).String())
		}
		if e.LockKeys.LockRPCCount > 0 {
			buf.WriteString(", rpc_count:")
			buf.WriteString(strconv.FormatInt(e.LockKeys.LockRPCCount, 10))
		}
		if e.LockKeys.RetryCount > 0 {
			buf.WriteString(", retry_count:")
			buf.WriteString(strconv.FormatInt(int64(e.LockKeys.RetryCount), 10))
		}

		buf.WriteString("}")
	}
	return buf.String()
}

func (*RuntimeStatsWithCommit) formatBackoff(buf *bytes.Buffer, backoffTypes []string) {
	if len(backoffTypes) == 0 {
		return
	}
	tpMap := make(map[string]struct{})
	tpArray := []string{}
	for _, tpStr := range backoffTypes {
		_, ok := tpMap[tpStr]
		if ok {
			continue
		}
		tpMap[tpStr] = struct{}{}
		tpArray = append(tpArray, tpStr)
	}
	slices.Sort(tpArray)
	buf.WriteByte('[')
	for i, tp := range tpArray {
		if i > 0 {
			buf.WriteString(" ")
		}
		buf.WriteString(tp)
	}
	buf.WriteByte(']')
}

// RURuntimeStats is a wrapper of util.RUDetails,
// which implements the RuntimeStats interface.
type RURuntimeStats struct {
	*util.RUDetails
}

// String implements the RuntimeStats interface.
func (e *RURuntimeStats) String() string {
	if e.RUDetails != nil {
		buf := bytes.NewBuffer(make([]byte, 0, 8))
		buf.WriteString("RU:")
		buf.WriteString(strconv.FormatFloat(e.RRU()+e.WRU(), 'f', 2, 64))
		return buf.String()
	}
	return ""
}

// Clone implements the RuntimeStats interface.
func (e *RURuntimeStats) Clone() RuntimeStats {
	return &RURuntimeStats{RUDetails: e.RUDetails.Clone()}
}

// Merge implements the RuntimeStats interface.
func (e *RURuntimeStats) Merge(other RuntimeStats) {
	if tmp, ok := other.(*RURuntimeStats); ok {
		if e.RUDetails != nil {
			e.RUDetails.Merge(tmp.RUDetails)
		} else {
			e.RUDetails = tmp.RUDetails.Clone()
		}
	}
}

// Tp implements the RuntimeStats interface.
func (*RURuntimeStats) Tp() int {
	return TpRURuntimeStats
}
