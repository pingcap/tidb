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
	"sync"
	"time"

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
	OnExecutionBegin(sqlDigest, planDigest []byte)

	// OnExecutionFinished should be called after the statement is executed.
	// WARNING: Currently Only call StatementObserver API when TopSQL is enabled,
	// there is no guarantee that both OnExecutionBegin and OnExecutionFinished will be called for a SQL,
	// such as TopSQL is enabled during a SQL execution.
	OnExecutionFinished(sqlDigest, planDigest []byte, execDuration time.Duration)
}

// StatementStats is a counter used locally in each session.
// We can use StatementStats to count data such as "the number of SQL executions",
// and it is expected that these statistics will eventually be collected and merged
// in the background.
type StatementStats struct {
	mu       sync.Mutex
	data     StatementStatsMap
	finished *atomic.Bool
}

// CreateStatementStats try to create and register an StatementStats.
func CreateStatementStats() *StatementStats {
	stats := &StatementStats{
		data:     StatementStatsMap{},
		finished: atomic.NewBool(false),
	}
	globalAggregator.register(stats)
	return stats
}

// OnExecutionBegin implements StatementObserver.OnExecutionBegin.
func (s *StatementStats) OnExecutionBegin(sqlDigest, planDigest []byte) {
	s.mu.Lock()
	defer s.mu.Unlock()
	item := s.GetOrCreateStatementStatsItem(sqlDigest, planDigest)

	item.ExecCount++
	// Count more data here.
}

// OnExecutionFinished implements StatementObserver.OnExecutionFinished.
func (s *StatementStats) OnExecutionFinished(sqlDigest, planDigest []byte, execDuration time.Duration) {
	ns := execDuration.Nanoseconds()
	if ns < 0 {
		return
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	item := s.GetOrCreateStatementStatsItem(sqlDigest, planDigest)

	item.SumDurationNs += uint64(ns)
	item.DurationCount++
	// Count more data here.
}

// GetOrCreateStatementStatsItem creates the corresponding StatementStatsItem
// for the specified SQLPlanDigest and timestamp if it does not exist before.
// GetOrCreateStatementStatsItem is just a helper function, not responsible for
// concurrency control, so GetOrCreateStatementStatsItem is **not** thread-safe.
func (s *StatementStats) GetOrCreateStatementStatsItem(sqlDigest, planDigest []byte) *StatementStatsItem {
	key := SQLPlanDigest{SQLDigest: BinaryDigest(sqlDigest), PlanDigest: BinaryDigest(planDigest)}
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

// BinaryDigest is converted from parser.Digest.Bytes(), and the purpose
// is to be used as the key of the map.
type BinaryDigest string

// SQLPlanDigest is used as the key of StatementStatsMap to
// distinguish different sql.
type SQLPlanDigest struct {
	SQLDigest  BinaryDigest
	PlanDigest BinaryDigest
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
	// ExecCount represents the number of SQL executions of TiDB.
	ExecCount uint64

	// SumDurationNs is the total number of durations in nanoseconds.
	SumDurationNs uint64

	// DurationCount represents the number of SQL executions specially
	// used to calculate SQLDuration.
	DurationCount uint64

	// KvStatsItem contains all indicators of kv layer.
	KvStatsItem KvStatementStatsItem
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
