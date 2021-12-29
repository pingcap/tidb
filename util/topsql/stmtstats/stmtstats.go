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

	"github.com/breeswish/go-litefsm"
	"github.com/pingcap/tidb/util/logutil"
	"go.uber.org/atomic"
	"go.uber.org/zap"
)

var _ StatementObserver = &StatementStats{}

// nowFunc returns the current time; it's overridden in tests.
var nowFunc = time.Now

// StatementStats is a counter used locally in each session.
// We can use StatementStats to count data such as "the number of SQL executions",
// and it is expected that these statistics will eventually be collected and merged
// in the background.
type StatementStats struct {
	mu       sync.Mutex
	data     StatementStatsMap
	finished *atomic.Bool

	// cmdState is used to know exactly where we are when the session is created from a connection.
	// When different commands are invoked in the connection, we need to count differently.
	cmdState              *litefsm.StateMachine
	cmdQueryBeginAt       time.Time
	cmdQueryProcessStmtAt time.Time
	cmdStmtExecuteBeginAt time.Time
	cmdStmtFetchBeginAt   time.Time
	executeBeginAt        time.Time
	sqlDigest             []byte
	planDigest            []byte
}

const (
	stateInitial                   litefsm.State = "Initial"
	stateCmdDispatchBegin          litefsm.State = "CmdDispatchBegin"
	stateCmdDispatchFinish         litefsm.State = "CmdDispatchFinish"
	stateCmdQueryBegin             litefsm.State = "CmdQueryBegin"
	stateCmdQueryFinish            litefsm.State = "CmdQueryFinish"
	stateCmdQueryProcessStmtBegin  litefsm.State = "CmdQueryProcessStmtBegin"
	stateCmdQueryProcessStmtFinish litefsm.State = "CmdQueryProcessStmtFinish"
	stateCmdStmtExecuteBegin       litefsm.State = "CmdStmtExecuteBegin"
	stateCmdStmtExecuteFinish      litefsm.State = "CmdStmtExecuteFinish"
	stateCmdStmtFetchBegin         litefsm.State = "CmdStmtFetchBegin"
	stateCmdStmtFetchFinish        litefsm.State = "CmdStmtFetchFinish"
)

var cmdTransitions = litefsm.NewTransitions()

func init() {
	// For each SQL connection, we continuously receive Cmd from the MySQL wire protocol.
	cmdTransitions.
		AddTransitFrom(stateInitial).
		Into(stateCmdDispatchBegin).
		ThenInto(stateCmdDispatchFinish).
		ThenInto(stateCmdDispatchBegin)
	// Dispatch may go into CmdQuery.
	cmdTransitions.
		AddTransitFrom(stateCmdDispatchBegin).
		Into(stateCmdQueryBegin).
		ThenInto(stateCmdQueryFinish).
		ThenInto(stateCmdDispatchFinish)
	// CmdQuery may contain zero, one or more statements.
	cmdTransitions.
		AddTransitFrom(stateCmdQueryBegin).
		Into(stateCmdQueryProcessStmtBegin).
		ThenInto(stateCmdQueryProcessStmtFinish).
		ThenInto(stateCmdQueryProcessStmtBegin).
		ThenInto(stateCmdQueryProcessStmtFinish).
		ThenInto(stateCmdQueryFinish)
	// Dispatch may go into CmdStmtExecute.
	cmdTransitions.
		AddTransitFrom(stateCmdDispatchBegin).
		Into(stateCmdStmtExecuteBegin).
		ThenInto(stateCmdStmtExecuteFinish).
		ThenInto(stateCmdDispatchFinish)
	// Dispatch may go into CmdStmtFetch.
	cmdTransitions.
		AddTransitFrom(stateCmdDispatchBegin).
		Into(stateCmdStmtFetchBegin).
		ThenInto(stateCmdStmtFetchFinish).
		ThenInto(stateCmdDispatchFinish)
}

func (s *StatementStats) resetDigests() {
	s.sqlDigest = nil
	s.planDigest = nil
}

// OnCmdDispatchBegin implements StatementObserver.
func (s *StatementStats) OnCmdDispatchBegin() {
	if err := s.cmdState.Goto(stateCmdDispatchBegin); err != nil {
		logutil.BgLogger().Warn("[stmt-stats] observer invalid", zap.Error(err))
		return
	}
	s.resetDigests()
}

// OnCmdDispatchFinish implements StatementObserver.
func (s *StatementStats) OnCmdDispatchFinish() {
	if err := s.cmdState.Goto(stateCmdDispatchFinish); err != nil {
		logutil.BgLogger().Warn("[stmt-stats] observer invalid", zap.Error(err))
		return
	}
}

// OnCmdQueryBegin implements StatementObserver.
func (s *StatementStats) OnCmdQueryBegin() {
	if err := s.cmdState.Goto(stateCmdQueryBegin); err != nil {
		logutil.BgLogger().Warn("[stmt-stats] observer invalid", zap.Error(err))
		return
	}
	s.cmdQueryBeginAt = nowFunc()
	s.resetDigests()
}

// OnCmdQueryFinish implements StatementObserver.
func (s *StatementStats) OnCmdQueryFinish() {
	if err := s.cmdState.Goto(stateCmdQueryFinish); err != nil {
		logutil.BgLogger().Warn("[stmt-stats] observer invalid", zap.Error(err))
		return
	}
}

// OnCmdQueryProcessStmtBegin implements StatementObserver.
func (s *StatementStats) OnCmdQueryProcessStmtBegin(stmtIdx int) {
	if err := s.cmdState.Goto(stateCmdQueryProcessStmtBegin); err != nil {
		logutil.BgLogger().Warn("[stmt-stats] observer invalid", zap.Error(err))
		return
	}
	s.cmdQueryProcessStmtAt = nowFunc()
	s.resetDigests()
}

// OnCmdQueryProcessStmtFinish implements StatementObserver.
func (s *StatementStats) OnCmdQueryProcessStmtFinish(stmtIdx int) {
	if err := s.cmdState.Goto(stateCmdQueryProcessStmtFinish); err != nil {
		logutil.BgLogger().Warn("[stmt-stats] observer invalid", zap.Error(err))
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.sqlDigest == nil {
		return
	}
	var stmtDuration time.Duration
	if stmtIdx == 0 {
		stmtDuration = nowFunc().Sub(s.cmdQueryBeginAt)
	} else {
		stmtDuration = nowFunc().Sub(s.cmdQueryProcessStmtAt)
	}
	s.GetOrCreate(s.sqlDigest, s.planDigest).addDuration(stmtDuration)
}

// OnCmdStmtExecuteBegin implements StatementObserver.
func (s *StatementStats) OnCmdStmtExecuteBegin() {
	if err := s.cmdState.Goto(stateCmdStmtExecuteBegin); err != nil {
		logutil.BgLogger().Warn("[stmt-stats] observer invalid", zap.Error(err))
		return
	}
	s.cmdStmtExecuteBeginAt = nowFunc()
	s.resetDigests()
}

// OnCmdStmtExecuteFinish implements StatementObserver.
func (s *StatementStats) OnCmdStmtExecuteFinish() {
	if err := s.cmdState.Goto(stateCmdStmtExecuteFinish); err != nil {
		logutil.BgLogger().Warn("[stmt-stats] observer invalid", zap.Error(err))
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.sqlDigest == nil {
		return
	}
	duration := nowFunc().Sub(s.cmdStmtExecuteBeginAt)
	s.GetOrCreate(s.sqlDigest, s.planDigest).addDuration(duration)
}

// OnCmdStmtFetchBegin implements StatementObserver.
func (s *StatementStats) OnCmdStmtFetchBegin() {
	if err := s.cmdState.Goto(stateCmdStmtFetchBegin); err != nil {
		logutil.BgLogger().Warn("[stmt-stats] observer invalid", zap.Error(err))
		return
	}
	s.cmdStmtFetchBeginAt = nowFunc()
	s.resetDigests()
}

// OnCmdStmtFetchFinish implements StatementObserver.
func (s *StatementStats) OnCmdStmtFetchFinish(sqlDigest []byte, planDigest []byte) {
	if err := s.cmdState.Goto(stateCmdStmtFetchFinish); err != nil {
		logutil.BgLogger().Warn("[stmt-stats] observer invalid", zap.Error(err))
		return
	}
	if sqlDigest == nil {
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	duration := nowFunc().Sub(s.cmdStmtFetchBeginAt)
	s.GetOrCreate(sqlDigest, planDigest).addDuration(duration)
}

// OnDigestKnown implements StatementObserver.
func (s *StatementStats) OnDigestKnown(sqlDigest []byte, planDigest []byte) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.sqlDigest == nil {
		s.sqlDigest = sqlDigest
		s.planDigest = planDigest
		s.GetOrCreate(sqlDigest, planDigest).ExecCount++
	}
}

// OnExecuteBegin implements StatementObserver.
func (s *StatementStats) OnExecuteBegin() {
	cmdState := s.cmdState.Current()
	if cmdState == stateCmdQueryProcessStmtBegin || cmdState == stateCmdStmtExecuteBegin || cmdState == stateCmdStmtFetchBegin {
		// This execution happens in a wire protocol that we are already counting the duration, so
		// we don't do anything and simply let the wire protocol events to handle it.
		return
	}
	s.executeBeginAt = nowFunc()
	s.resetDigests()
}

// OnExecuteFinish implements StatementObserver.
func (s *StatementStats) OnExecuteFinish() {
	cmdState := s.cmdState.Current()
	if cmdState == stateCmdQueryProcessStmtBegin || cmdState == stateCmdStmtExecuteBegin || cmdState == stateCmdStmtFetchBegin {
		return
	}
	// This may happen when the session is not created from a connection, for example, background internal sessions.
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.sqlDigest == nil {
		return
	}
	duration := nowFunc().Sub(s.executeBeginAt)
	s.GetOrCreate(s.sqlDigest, s.planDigest).addDuration(duration)
}

// CreateStatementStats try to create and register an StatementStats.
func CreateStatementStats() *StatementStats {
	stats := &StatementStats{
		data:     StatementStatsMap{},
		finished: atomic.NewBool(false),
		cmdState: litefsm.NewStateMachine(cmdTransitions, stateInitial),
	}
	globalAggregator.register(stats)
	return stats
}

// GetOrCreate creates the corresponding StatementStatsItem
// for the specified SQLPlanDigest and timestamp if it does not exist before.
// GetOrCreate is just a helper function, not responsible for
// concurrency control, so GetOrCreate is **not** thread-safe.
func (s *StatementStats) GetOrCreate(sqlDigest, planDigest []byte) *StatementStatsItem {
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
	item := s.GetOrCreate(sqlDigest, planDigest)
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

	// KvStatsItem contains all indicators of kv layer.
	KvStatsItem KvStatementStatsItem
}

func (i *StatementStatsItem) addDuration(d time.Duration) {
	durationMs := d.Nanoseconds()
	if durationMs < 0 {
		return
	}
	i.SumDurationNs += uint64(durationMs)
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
