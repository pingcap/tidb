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

package stmtstats

import (
	"context"
	"sync"
	"time"

	"github.com/tikv/client-go/v2/util"
	"go.uber.org/atomic"
)

var _ StatementObserver = &StatementStats{}

// StatementObserver is an abstract interface as a callback to the corresponding
// position of TiDB's SQL statement execution process. StatementStats implements
// StatementObserver and performs counting such as SQLExecCount/SQLDuration internally.
// The caller only needs to be responsible for calling different methods at the
// corresponding locations, without paying attention to implementation details.
type StatementObserver interface {
	// OnExecutionBegin should be called before statement execution.
	OnExecutionBegin(sqlDigest, planDigest []byte, info *ExecBeginInfo)

	// OnExecutionFinished should be called after the statement is executed.
	// WARNING: Currently Only call StatementObserver API when TopSQL is enabled,
	// there is no guarantee that both OnExecutionBegin and OnExecutionFinished will be called for a SQL,
	// such as TopSQL is enabled during a SQL execution.
	OnExecutionFinished(sqlDigest, planDigest []byte, info *ExecFinishInfo)
}

// ExecBeginInfo carries optional execution-begin context for extensible stats collection.
type ExecBeginInfo struct {
	InNetworkBytes uint64
	User           string
	TopRUEnabled   bool
	Ctx            context.Context
}

// ExecFinishInfo carries optional execution-finish context for extensible stats collection.
type ExecFinishInfo struct {
	OutNetworkBytes uint64
	ExecDuration    time.Duration
	User            string
	TopRUEnabled    bool
	RUDetails       *util.RUDetails
}

// StatementStats is a counter used locally in each session.
// We can use StatementStats to count data such as "the number of SQL executions",
// and it is expected that these statistics will eventually be collected and merged
// in the background.
type StatementStats struct {
	data     StatementStatsMap
	finished *atomic.Bool
	mu       sync.Mutex

	// RU tracking fields for TopRU (separate from TopSQL stmtstats).
	finishedRUBuffer RUIncrementMap // Completed SQL RU deltas drained by aggregator ticks.
	// execCtx tracks the currently active SQL execution in this session.
	// TiDB session execution is serialized, so at most one active context is kept.
	execCtx *ExecutionContext
}

// CreateStatementStats try to create and register an StatementStats.
func CreateStatementStats() *StatementStats {
	stats := &StatementStats{
		data:             StatementStatsMap{},
		finished:         atomic.NewBool(false),
		finishedRUBuffer: RUIncrementMap{},
	}
	globalAggregator.register(stats)
	return stats
}

// OnExecutionBegin implements StatementObserver.OnExecutionBegin.
func (s *StatementStats) OnExecutionBegin(sqlDigest, planDigest []byte, info *ExecBeginInfo) {
	s.mu.Lock()
	defer s.mu.Unlock()
	item := s.GetOrCreateStatementStatsItem(sqlDigest, planDigest)

	item.ExecCount++
	if info != nil {
		item.NetworkInBytes += info.InNetworkBytes
		if info.TopRUEnabled {
			s.addRUOnBeginLocked(info.User, sqlDigest, planDigest, info.Ctx)
		}
	}
	// Count more data here.
}

func (s *StatementStats) addRUOnBeginLocked(user string, sqlDigest, planDigest []byte, ctx context.Context) {
	key := RUKey{
		User:       user,
		SQLDigest:  BinaryDigest(sqlDigest),
		PlanDigest: BinaryDigest(planDigest),
	}
	// Cache RUDetails at begin time to avoid per-tick context.Value() lookups.
	var ruDetails *util.RUDetails
	if ctx != nil {
		if raw := ctx.Value(util.RUDetailsCtxKey); raw != nil {
			ruDetails, _ = raw.(*util.RUDetails)
		}
	}
	// Replace stale execution context defensively.
	s.execCtx = &ExecutionContext{
		RUDetails: ruDetails,
		Key:       key,
	}
	// ExecCount is begin-based, aligned with TopSQL semantics.
	incr := s.getOrCreateRUIncrementLocked(key)
	incr.ExecCount++
}

// OnExecutionFinished implements StatementObserver.OnExecutionFinished.
func (s *StatementStats) OnExecutionFinished(sqlDigest, planDigest []byte, info *ExecFinishInfo) {
	if info == nil {
		return
	}
	ns := info.ExecDuration.Nanoseconds()
	if ns < 0 {
		s.mu.Lock()
		defer s.mu.Unlock()
		s.clearRUExecCtxLocked()
		return
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	item := s.GetOrCreateStatementStatsItem(sqlDigest, planDigest)

	item.SumDurationNs += uint64(ns)
	item.DurationCount++
	item.NetworkOutBytes += info.OutNetworkBytes
	if info.TopRUEnabled {
		s.addRUOnFinishLocked(info.User, sqlDigest, planDigest, info.RUDetails, info.ExecDuration)
	} else {
		s.clearRUExecCtxLocked()
	}
	// Count more data here.
}

func (s *StatementStats) addRUOnFinishLocked(user string, sqlDigest, planDigest []byte, ru *util.RUDetails, execDuration time.Duration) {
	if s.execCtx == nil {
		// No matching begin was recorded, so delta cannot be computed correctly.
		return
	}
	key := RUKey{
		User:       user,
		SQLDigest:  BinaryDigest(sqlDigest),
		PlanDigest: BinaryDigest(planDigest),
	}
	if s.execCtx.Key != key {
		// A newer execution has replaced the active context.
		return
	}
	defer s.clearRUExecCtxLocked()
	if ru == nil {
		return
	}

	currentTotalRU := ru.RRU() + ru.WRU()
	if currentTotalRU <= 0 {
		return
	}

	lastTotalRU := s.execCtx.LastRUTotal
	s.execCtx.LastRUTotal = currentTotalRU
	deltaRU := currentTotalRU - lastTotalRU
	if deltaRU <= 0 {
		// Counter reset or the value was already sampled.
		return
	}
	incr := s.getOrCreateRUIncrementLocked(key)
	incr.TotalRU += deltaRU
	incr.ExecDuration += uint64(execDuration.Nanoseconds())
}

func (s *StatementStats) getOrCreateRUIncrementLocked(key RUKey) *RUIncrement {
	incr, ok := s.finishedRUBuffer[key]
	if !ok {
		incr = &RUIncrement{}
		s.finishedRUBuffer[key] = incr
	}
	return incr
}

func (s *StatementStats) clearRUExecCtxLocked() {
	s.execCtx = nil
}

func (s *StatementStats) sampleActiveRUDeltaLocked(result RUIncrementMap) RUIncrementMap {
	if s.execCtx == nil || s.execCtx.RUDetails == nil {
		return result
	}

	currentTotalRU := s.execCtx.RUDetails.RRU() + s.execCtx.RUDetails.WRU()
	deltaRU := currentTotalRU - s.execCtx.LastRUTotal
	if deltaRU > 0 {
		incr, ok := result[s.execCtx.Key]
		if !ok {
			incr = &RUIncrement{}
			result[s.execCtx.Key] = incr
		}
		incr.TotalRU += deltaRU
	}
	// Keep LastRUTotal in sync even when delta <= 0 (e.g. counter reset).
	s.execCtx.LastRUTotal = currentTotalRU
	return result
}

// GetOrCreateStatementStatsItem creates the corresponding StatementStatsItem
// for the specified SQLPlanDigest and timestamp if it does not exist before.
// GetOrCreateStatementStatsItem is just a helper function, not responsible for
// concurrency control, so GetOrCreateStatementStatsItem is **not** thread-safe.
func (s *StatementStats) GetOrCreateStatementStatsItem(sqlDigest, planDigest []byte) *StatementStatsItem {
	key := newSQLPlanDigest(sqlDigest, planDigest)
	item, ok := s.data[key]
	if !ok {
		s.data[key] = NewStatementStatsItem()
		item = s.data[key]
	}
	return item
}

// addKvExecCount is used to count the number of executions of a certain SQLPlanDigest for a certain target.
// addKvExecCount is thread-safe.
func (s *StatementStats) addKvExecCount(sqlDigest, planDigest []byte, target string, n uint64) {
	s.mu.Lock()
	defer s.mu.Unlock()
	item := s.GetOrCreateStatementStatsItem(sqlDigest, planDigest)
	item.KvStatsItem.KvExecCount[target] += n
}

// Take takes out all existing StatementStatsMap data from StatementStats.
// Take is thread-safe.
func (s *StatementStats) Take() StatementStatsMap {
	s.mu.Lock()
	defer s.mu.Unlock()
	data := s.data
	s.data = StatementStatsMap{}
	return data
}

// SetFinished marks this StatementStats as "finished" and no more counting or
// aggregation should happen. Associated resources will be cleaned up, like background
// aggregators.
// Generally, as the StatementStats is created when a session starts, SetFinished
// should be called when the session ends.
func (s *StatementStats) SetFinished() {
	s.finished.Store(true)
}

// Finished returns whether the StatementStats has been finished.
func (s *StatementStats) Finished() bool {
	return s.finished.Load()
}

// MergeRUInto drains finishedRUBuffer and returns accumulated RU increments.
// In-flight RU is sampled in the same call.
func (s *StatementStats) MergeRUInto() RUIncrementMap {
	s.mu.Lock()
	defer s.mu.Unlock()

	result := s.finishedRUBuffer
	s.finishedRUBuffer = RUIncrementMap{}
	return s.sampleActiveRUDeltaLocked(result)
}

// BinaryDigest is converted from parser.Digest.Bytes(), and the purpose
// is to be used as the key of the map.
type BinaryDigest string

// SQLPlanDigest is used as the key of StatementStatsMap to
// distinguish different sql.
type SQLPlanDigest struct {
	SQLDigest  BinaryDigest
	PlanDigest BinaryDigest
}

func newSQLPlanDigest(sqlDigest, planDigest []byte) SQLPlanDigest {
	return SQLPlanDigest{
		SQLDigest:  BinaryDigest(sqlDigest),
		PlanDigest: BinaryDigest(planDigest),
	}
}

// StatementStatsMap is the local data type of StatementStats.
type StatementStatsMap map[SQLPlanDigest]*StatementStatsItem

// Merge merges other into StatementStatsMap.
// Values with the same SQLPlanDigest will be merged.
//
// After executing Merge, some pointers in other may be referenced
// by m. So after calling Merge, it is best not to continue to use
// other unless you understand what you are doing.
func (m StatementStatsMap) Merge(other StatementStatsMap) {
	if m == nil || other == nil {
		return
	}
	for newDigest, newItem := range other {
		item, ok := m[newDigest]
		if !ok {
			m[newDigest] = newItem
			continue
		}
		item.Merge(newItem)
	}
}

// StatementStatsItem represents a set of mergeable statistics.
// StatementStatsItem is used in a larger data structure to represent
// the stats of a certain SQLPlanDigest under a certain timestamp.
// If there are more indicators that need to be added in the future,
// please add it in StatementStatsItem and implement its aggregation
// in the Merge method.
type StatementStatsItem struct {
	// KvStatsItem contains all indicators of kv layer.
	KvStatsItem KvStatementStatsItem
	// ExecCount represents the number of SQL executions of TiDB.
	ExecCount uint64
	// SumDurationNs is the total number of durations in nanoseconds.
	SumDurationNs uint64
	// DurationCount represents the number of SQL executions specially
	// used to calculate SQLDuration.
	DurationCount uint64
	// NetworkInBytes represents the total number of network input bytes from client.
	NetworkInBytes uint64
	// NetworkOutBytes represents the total number of network input bytes to client.
	NetworkOutBytes uint64
}

// NewStatementStatsItem creates an empty StatementStatsItem.
func NewStatementStatsItem() *StatementStatsItem {
	return &StatementStatsItem{
		KvStatsItem: NewKvStatementStatsItem(),
	}
}

// Merge merges other into StatementStatsItem.
//
// After executing Merge, some pointers in other may be referenced
// by i. So after calling Merge, it is best not to continue to use
// other unless you understand what you are doing.
//
// If you add additional indicators, you need to add their merge code here.
func (i *StatementStatsItem) Merge(other *StatementStatsItem) {
	if i == nil || other == nil {
		return
	}
	i.ExecCount += other.ExecCount
	i.SumDurationNs += other.SumDurationNs
	i.DurationCount += other.DurationCount
	i.NetworkInBytes += other.NetworkInBytes
	i.NetworkOutBytes += other.NetworkOutBytes
	i.KvStatsItem.Merge(other.KvStatsItem)
}

// KvStatementStatsItem is part of StatementStatsItem, it only contains
// indicators of kv layer.
type KvStatementStatsItem struct {
	// KvExecCount represents the number of SQL executions of TiKV.
	KvExecCount map[string]uint64
}

// NewKvStatementStatsItem creates an empty KvStatementStatsItem.
func NewKvStatementStatsItem() KvStatementStatsItem {
	return KvStatementStatsItem{
		KvExecCount: map[string]uint64{},
	}
}

// Merge merges other into KvStatementStatsItem.
//
// After executing Merge, some pointers in other may be referenced
// by i. So after calling Merge, it is best not to continue to use
// other unless you understand what you are doing.
//
// If you add additional indicators, you need to add their merge code here.
func (i *KvStatementStatsItem) Merge(other KvStatementStatsItem) {
	if i.KvExecCount == nil {
		i.KvExecCount = other.KvExecCount
	} else {
		for target, count := range other.KvExecCount {
			i.KvExecCount[target] += count
		}
	}
}
