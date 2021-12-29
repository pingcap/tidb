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
	OnExecutionBegin(sqlDigest, planDigest BinaryDigest)

	// OnExecutionFinished should be called after the statement is executed.
	OnExecutionFinished(sqlDigest, planDigest BinaryDigest)
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
func (s *StatementStats) OnExecutionBegin(sqlDigest, planDigest BinaryDigest) {
	s.mu.Lock()
	defer s.mu.Unlock()
	item := s.getOrCreate(sqlDigest, planDigest)

	item.ExecCount++
	// Count more data here.
}

// OnExecutionFinished implements StatementObserver.OnExecutionFinished.
func (s *StatementStats) OnExecutionFinished(sqlDigest, planDigest BinaryDigest) {
	// Count more data here.
}

// getOrCreate creates the corresponding statementStatsItem
// for the specified sqlPlanDigest and timestamp if it does not exist before.
// getOrCreate is just a helper function, not responsible for
// concurrency control, so getOrCreate is **not** thread-safe.
func (s *StatementStats) getOrCreate(sqlDigest, planDigest BinaryDigest) *statementStatsItem {
	key := sqlPlanDigest{SQLDigest: sqlDigest, PlanDigest: planDigest}
	item, ok := s.data[key]
	if !ok {
		s.data[key] = NewStatementStatsItem()
		item = s.data[key]
	}
	return item
}

// take takes out all existing StatementStatsMap data from StatementStats.
// take is thread-safe.
func (s *StatementStats) take() StatementStatsMap {
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

// sqlPlanDigest is used as the key of StatementStatsMap to
// distinguish different sql.
type sqlPlanDigest struct {
	SQLDigest  BinaryDigest
	PlanDigest BinaryDigest
}

// StatementStatsMap is the local data type of StatementStats.
type StatementStatsMap map[sqlPlanDigest]*statementStatsItem

// Merge merges other into StatementStatsMap.
// Values with the same sqlPlanDigest will be merged.
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

// statementStatsItem represents a set of mergeable statistics.
// statementStatsItem is used in a larger data structure to represent
// the stats of a certain sqlPlanDigest under a certain timestamp.
// If there are more indicators that need to be added in the future,
// please add it in statementStatsItem and implement its aggregation
// in the Merge method.
type statementStatsItem struct {
	// ExecCount represents the number of SQL executions of TiDB.
	ExecCount uint64

	// KvStatsItem contains all indicators of kv layer.
	KvStatsItem kvStatementStatsItem
}

// NewStatementStatsItem creates an empty statementStatsItem.
func NewStatementStatsItem() *statementStatsItem {
	return &statementStatsItem{
		KvStatsItem: newKvStatementStatsItem(),
	}
}

// Merge merges other into statementStatsItem.
//
// After executing Merge, some pointers in other may be referenced
// by i. So after calling Merge, it is best not to continue to use
// other unless you understand what you are doing.
//
// If you add additional indicators, you need to add their merge code here.
func (i *statementStatsItem) Merge(other *statementStatsItem) {
	if i == nil || other == nil {
		return
	}
	i.ExecCount += other.ExecCount
	i.KvStatsItem.Merge(other.KvStatsItem)
}

// kvStatementStatsItem is part of statementStatsItem, it only contains
// indicators of kv layer.
type kvStatementStatsItem struct {
	// KvExecCount represents the number of SQL executions of TiKV.
	KvExecCount map[string]uint64
}

// newKvStatementStatsItem creates an empty kvStatementStatsItem.
func newKvStatementStatsItem() kvStatementStatsItem {
	return kvStatementStatsItem{
		KvExecCount: map[string]uint64{},
	}
}

// Merge merges other into kvStatementStatsItem.
//
// After executing Merge, some pointers in other may be referenced
// by i. So after calling Merge, it is best not to continue to use
// other unless you understand what you are doing.
//
// If you add additional indicators, you need to add their merge code here.
func (i *kvStatementStatsItem) Merge(other kvStatementStatsItem) {
	if i.KvExecCount == nil {
		i.KvExecCount = other.KvExecCount
	} else {
		for target, count := range other.KvExecCount {
			i.KvExecCount[target] += count
		}
	}
}
