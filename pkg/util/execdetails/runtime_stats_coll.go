// Copyright 2021 PingCAP, Inc.
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

	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tipb/go-tipb"
	"github.com/tikv/client-go/v2/util"
)

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
