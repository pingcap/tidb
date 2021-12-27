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
	"fmt"
	"sync"
	"time"

	"github.com/pingcap/tidb/util/logutil"
	"go.uber.org/atomic"
	"go.uber.org/zap"
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
	// OnReceiveCmd should be called on TiDB receive command request.
	OnReceiveCmd()

	// OnSQLAndPlanDigestFirstReady should be called on statement SQL and plan digest are ready.
	// This function only use to record the SQL and plan digest, it doesn't matter if you delay calling this function.
	OnSQLAndPlanDigestFirstReady(sqlDigest, planDigest []byte)

	// OnExecFinished should be called after the statement execution finish.
	OnExecFinished()
}

// StatementStats is a counter used locally in each session.
// We can use StatementStats to count data such as "the number of SQL executions",
// and it is expected that these statistics will eventually be collected and merged
// in the background.
type StatementStats struct {
	seCtx StatementExecutionContext
	state StmtExecState

	mu       sync.Mutex
	data     StatementStatsMap
	finished *atomic.Bool
}

// StmtExecState is the state of StatementStats
type StmtExecState int

var (
	stmtExecStateInitial       StmtExecState = 0
	stmtExecStateCmdReceived   StmtExecState = 1
	stmtExecStateDigestIsReady StmtExecState = 2
	stmtExecStateFinished      StmtExecState = 3
)

// String implements Stringer interface.
func (s StmtExecState) String() string {
	switch s {
	case stmtExecStateInitial:
		return "initial"
	case stmtExecStateCmdReceived:
		return "cmd_received"
	case stmtExecStateDigestIsReady:
		return "digest_is_ready"
	case stmtExecStateFinished:
		return "finish"
	default:
		return fmt.Sprintf("unknow_%d", int(s))
	}
}

// CreateStatementStats try to create and register an StatementStats.
func CreateStatementStats() *StatementStats {
	stats := &StatementStats{
		state:    stmtExecStateInitial,
		data:     StatementStatsMap{},
		finished: atomic.NewBool(false),
	}
	globalAggregator.register(stats)
	return stats
}

// OnReceiveCmd implements StatementObserver.OnParseBegin.
func (s *StatementStats) OnReceiveCmd() {
	s.state = stmtExecStateCmdReceived
	s.seCtx.reset()
}

// OnSQLAndPlanDigestFirstReady implements StatementObserver.OnSQLAndPlanDigestFirstReady.
func (s *StatementStats) OnSQLAndPlanDigestFirstReady(sqlDigest, planDigest []byte) {
	if s.state != stmtExecStateCmdReceived {
		logutil.BgLogger().Warn("[stmt-stats] unexpect state",
			zap.String("expect", stmtExecStateCmdReceived.String()),
			zap.String("got", s.state.String()))
		return
	}
	s.state = stmtExecStateDigestIsReady

	s.mu.Lock()
	defer s.mu.Unlock()
	if len(s.seCtx.sqlDigest) != 0 {
		return
	}
	s.seCtx.sqlDigest = sqlDigest
	s.seCtx.planDigest = planDigest
	item := s.GetOrCreateStatementStatsItem(sqlDigest, planDigest)
	item.ExecCount++
}

// OnExecFinished implements StatementObserver.OnExecFinished.
func (s *StatementStats) OnExecFinished() {
	if s.state != stmtExecStateDigestIsReady {
		logutil.BgLogger().Warn("[stmt-stats] unexpect state",
			zap.String("expect", stmtExecStateDigestIsReady.String()),
			zap.String("got", s.state.String()))
		return
	}
	s.state = stmtExecStateFinished

	s.mu.Lock()
	defer s.mu.Unlock()
	if len(s.seCtx.sqlDigest) == 0 {
		return
	}
	item := s.GetOrCreateStatementStatsItem(s.seCtx.sqlDigest, s.seCtx.planDigest)
	cost := nowFunc().Sub(s.seCtx.begin).Nanoseconds()
	if cost > 0 {
		item.SumExecNanoDuration += uint64(cost)
	}
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
	sqlDigest  []byte
	planDigest []byte
	// begin is the time when receive the statement request.
	begin time.Time
	// Add more required variables here
}

var zeroTime = time.Time{}

func (sec *StatementExecutionContext) reset() {
	sec.sqlDigest = nil
	sec.planDigest = nil
	sec.begin = nowFunc()
}
