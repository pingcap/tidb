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

// nowFunc returns the current time; it's overridden in tests.
var nowFunc = time.Now

// StatementObserver is an abstract interface as a callback to the corresponding
// position of TiDB's SQL statement execution process. StatementStats implements
// StatementObserver and performs counting such as SQLExecCount/SQLDuration internally.
// The caller only needs to be responsible for calling different methods at the
// corresponding locations, without paying attention to implementation details.
type StatementObserver interface {
	// OnDispatchBegin should be called on the begin of TiDB dispatch command.
	OnDispatchBegin()

	// OnDispatchFinish should be called on TiDB finished dispatch command.
	OnDispatchFinish()

	// OnHandleQueryBegin should be called on the begin of TiDB handle query.
	OnHandleQueryBegin()

	// OnHandleQueryFinish should be called on TiDB finished handle query.
	OnHandleQueryFinish()

	// OnHandleStmtBegin should be called on the begin of TiDB handle stmt.
	OnHandleStmtBegin()

	// OnHandleStmtFinish should be called on TiDB finished handle stmt.
	OnHandleStmtFinish()

	// OnStmtReadyToExecute should be called on statement ready to execute with physical plan.
	// It's ok to call this function multiple time, but only the first time take effect. This is design for execution retry.
	OnStmtReadyToExecute(sqlDigest, planDigest []byte)

	// OnHandleStmtBegin should be called on the begin of TiDB handle stmt.
	OnHandleStmtExecuteBegin()

	// OnHandleStmtFinish should be called on TiDB finished stmt execute.
	OnHandleStmtExecuteFinish()

	// OnHandleStmtBegin should be called on the begin of TiDB handle stmt fetch.
	OnHandleStmtFetchBegin()

	// OnHandleStmtFinish should be called on TiDB finished stmt fetch.
	OnHandleStmtFetchFinish(sqlDigest, planDigest []byte)

	// OnHandleStmtBegin should be called on the begin of TiDB use db.
	OnUseDBBegin()

	// OnHandleStmtFinish should be called on TiDB finished use db.
	OnUseDBFinish()
}

// StatementStats is a counter used locally in each session.
// We can use StatementStats to count data such as "the number of SQL executions",
// and it is expected that these statistics will eventually be collected and merged
// in the background.
type StatementStats struct {
	ctx      StatementExecutionContext
	mu       sync.Mutex
	data     StatementStatsMap
	finished *atomic.Bool
}

// CreateStatementStats try to create and register an StatementStats.
func CreateStatementStats() *StatementStats {
	stats := &StatementStats{
		//state:    stmtExecStateInitial,
		data:     StatementStatsMap{},
		finished: atomic.NewBool(false),
	}
	globalAggregator.register(stats)
	return stats
}

// OnDispatchBegin implements StatementObserver.OnDispatchBegin.
func (s *StatementStats) OnDispatchBegin() {
	acceptLastStates := []ExecState{execStateDispatchFinish, execStateUseDBFinish, execStateInitial}
	s.ctx.state.tryGoTo(execStateDispatchBegin, acceptLastStates)
	s.ctx.state = execStateDispatchBegin
	s.ctx.cmdDispatchBegin = nowFunc()
	s.ctx.sqlDigest = nil
	s.ctx.planDigest = nil
}

// OnDispatchFinish implements StatementObserver.OnDispatchFinish.
func (s *StatementStats) OnDispatchFinish() {
	acceptLastStates := []ExecState{execStateHandleQueryFinish, execStateHandleStmtExecuteFinish,
		execStateHandleStmtFetchFinish, execStateUseDBFinish, execStateDispatchBegin, execStateStmtReadyToExecute}
	s.ctx.state.tryGoTo(execStateDispatchFinish, acceptLastStates)
}

// OnHandleQueryBegin implements StatementObserver.OnHandleQueryBegin.
func (s *StatementStats) OnHandleQueryBegin() {
	acceptLastStates := []ExecState{execStateDispatchBegin}
	valid := s.ctx.state.tryGoTo(execStateHandleQueryBegin, acceptLastStates)
	if !valid {
		return
	}
	s.ctx.handleStmtIdx = 0
}

// OnHandleQueryFinish implements StatementObserver.OnHandleQueryFinish.
func (s *StatementStats) OnHandleQueryFinish() {
	acceptLastStates := []ExecState{execStateHandleQueryBegin, execStateHandleStmtFinish}
	s.ctx.state.tryGoTo(execStateHandleQueryFinish, acceptLastStates)
}

// OnHandleStmtBegin implements StatementObserver.OnHandleStmtBegin.
func (s *StatementStats) OnHandleStmtBegin() {
	acceptLastStates := []ExecState{execStateHandleQueryBegin, execStateHandleStmtFinish}
	valid := s.ctx.state.tryGoTo(execStateHandleStmtBegin, acceptLastStates)
	if !valid {
		return
	}
	s.ctx.state = execStateHandleStmtBegin
	s.ctx.handleStmtBegin = nowFunc()
	s.ctx.sqlDigest = nil
	s.ctx.planDigest = nil
}

// OnHandleStmtFinish implements StatementObserver.OnHandleStmtFinish.
func (s *StatementStats) OnHandleStmtFinish() {
	acceptLastStates := []ExecState{execStateHandleStmtBegin, execStateStmtReadyToExecute}
	valid := s.ctx.state.tryGoTo(execStateHandleStmtFinish, acceptLastStates)
	if !valid {
		return
	}

	if s.ctx.handleStmtIdx == 0 {
		s.onStmtFinished(s.ctx.cmdDispatchBegin)
	} else {
		s.onStmtFinished(s.ctx.handleStmtBegin)
	}
	s.ctx.handleStmtIdx++
}

// OnStmtReadyToExecute implements StatementObserver.OnStmtReadyToExecute.
func (s *StatementStats) OnStmtReadyToExecute(sqlDigest, planDigest []byte) {
	if s.ctx.state == execStateInitial {
		// TODO: remove this after support internal statement.
		return
	}
	acceptLastStates := []ExecState{execStateHandleStmtBegin, execStateHandleStmtExecuteBegin, execStateUseDBBegin, execStateStmtReadyToExecute}
	valid := s.ctx.state.tryGoTo(execStateStmtReadyToExecute, acceptLastStates)
	if !valid {
		return
	}

	if len(sqlDigest) == 0 {
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	// Only record execution count when SQL digest first ready.
	if len(s.ctx.sqlDigest) != 0 {
		return
	}
	s.ctx.sqlDigest = sqlDigest
	s.ctx.planDigest = planDigest
	item := s.GetOrCreateStatementStatsItem(sqlDigest, planDigest)
	item.ExecCount++
}

// OnHandleStmtExecuteBegin implements StatementObserver.OnHandleStmtExecuteBegin.
func (s *StatementStats) OnHandleStmtExecuteBegin() {
	acceptLastStates := []ExecState{execStateDispatchBegin}
	s.ctx.state.tryGoTo(execStateHandleStmtExecuteBegin, acceptLastStates)
}

// OnHandleStmtExecuteFinish implements StatementObserver.OnHandleStmtExecuteFinish.
func (s *StatementStats) OnHandleStmtExecuteFinish() {
	acceptLastStates := []ExecState{execStateHandleStmtExecuteBegin, execStateStmtReadyToExecute}
	valid := s.ctx.state.tryGoTo(execStateHandleStmtExecuteFinish, acceptLastStates)
	if !valid {
		return
	}

	s.onStmtFinished(s.ctx.cmdDispatchBegin)
}

// OnHandleStmtFetchBegin implements StatementObserver.OnHandleStmtFetchBegin.
func (s *StatementStats) OnHandleStmtFetchBegin() {
	acceptLastStates := []ExecState{execStateDispatchBegin}
	s.ctx.state.tryGoTo(execStateHandleStmtFetchBegin, acceptLastStates)
}

// OnHandleStmtFetchFinish implements StatementObserver.OnHandleStmtFetchFinish.
func (s *StatementStats) OnHandleStmtFetchFinish(sqlDigest, planDigest []byte) {
	acceptLastStates := []ExecState{execStateHandleStmtFetchBegin}
	valid := s.ctx.state.tryGoTo(execStateHandleStmtFetchFinish, acceptLastStates)
	if !valid {
		return
	}

	s.ctx.sqlDigest, s.ctx.planDigest = sqlDigest, planDigest
	s.onStmtFinished(s.ctx.cmdDispatchBegin)
}

// OnUseDBBegin implements StatementObserver.OnUseDBBegin.
func (s *StatementStats) OnUseDBBegin() {
	acceptLastStates := []ExecState{execStateDispatchBegin, execStateInitial}
	s.ctx.state.tryGoTo(execStateUseDBBegin, acceptLastStates)
}

// OnUseDBFinish implements StatementObserver.OnUseDBFinish.
func (s *StatementStats) OnUseDBFinish() {
	acceptLastStates := []ExecState{execStateStmtReadyToExecute, execStateUseDBBegin}
	s.ctx.state.tryGoTo(execStateUseDBFinish, acceptLastStates)
}

func (s *StatementStats) onStmtFinished(begin time.Time) {
	if len(s.ctx.sqlDigest) == 0 {
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	item := s.GetOrCreateStatementStatsItem(s.ctx.sqlDigest, s.ctx.planDigest)
	cost := nowFunc().Sub(begin).Nanoseconds()
	if cost > 0 {
		item.SumExecNanoDuration += uint64(cost)
	}
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

	// SumExecNanoDuration represents the nanoseconds of SQL executions of TiDB.
	SumExecNanoDuration uint64

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
	i.SumExecNanoDuration += other.SumExecNanoDuration
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

// StatementExecutionContext represents a single statement execution information.
type StatementExecutionContext struct {
	state            ExecState
	sqlDigest        []byte
	planDigest       []byte
	cmdDispatchBegin time.Time
	handleStmtBegin  time.Time
	handleStmtIdx    int
}
