// Copyright 2023 PingCAP, Inc.
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

package usage

import (
	"sync"
	"time"

	"github.com/pingcap/tidb/parser/model"
	"github.com/pingcap/tidb/sessionctx/variable"
)

func merge(s *SessionStatsItem, deltaMap *TableDelta, colMap *StatsUsage) {
	deltaMap.Merge(s.mapper.GetDeltaAndReset())
	colMap.Merge(s.statsUsage.GetUsageAndReset())
}

// SessionStatsItem is a list item that holds the delta mapper. If you want to write or read mapper, you must lock it.
type SessionStatsItem struct {
	mapper     *TableDelta
	statsUsage *StatsUsage
	next       *SessionStatsItem
	sync.Mutex

	// deleted is set to true when a session is closed. Every time we sweep the list, we will remove the useless collector.
	deleted bool
}

// Delete only sets the deleted flag true, it will be deleted from list when DumpStatsDeltaToKV is called.
func (s *SessionStatsItem) Delete() {
	s.Lock()
	defer s.Unlock()
	s.deleted = true
}

// Update will updates the delta and count for one table id.
func (s *SessionStatsItem) Update(id int64, delta int64, count int64, colSize *map[int64]int64) {
	s.Lock()
	defer s.Unlock()
	s.mapper.Update(id, delta, count, colSize)
}

// ClearForTest clears the mapper for test.
func (s *SessionStatsItem) ClearForTest() {
	s.Lock()
	defer s.Unlock()
	s.mapper = NewTableDelta()
	s.statsUsage = NewStatsUsage()
	s.next = nil
	s.deleted = false
}

// UpdateColStatsUsage updates the last time when the column stats are used(needed).
func (s *SessionStatsItem) UpdateColStatsUsage(colMap map[model.TableItemID]time.Time) {
	s.Lock()
	defer s.Unlock()
	s.statsUsage.Merge(colMap)
}

// SessionStatsList is a list of SessionStatsItem, which is used to collect stats usage and table delta information from sessions.
// TODO: merge SessionIndexUsage into this list.
/*
                            [session1]                [session2]                        [sessionN]
                                |                         |                                 |
                            update into              update into                       update into
                                |                         |                                 |
                                v                         v                                 v
[StatsList.Head] --> [session1.StatsItem] --> [session2.StatsItem] --> ... --> [sessionN.StatsItem]
                                |                         |                                 |
                                +-------------------------+---------------------------------+
                                                          |
                                        collect and dump into storage periodically
                                                          |
                                                          v
                                                      [storage]
*/
type SessionStatsList struct {
	// tableDelta contains all the delta map from collectors when we dump them to KV.
	tableDelta *TableDelta

	// statsUsage contains all the column stats usage information from collectors when we dump them to KV.
	statsUsage *StatsUsage

	// listHead contains all the stats collector required by session.
	listHead *SessionStatsItem
}

// NewSessionStatsList initializes a new SessionStatsList.
func NewSessionStatsList() *SessionStatsList {
	return &SessionStatsList{
		tableDelta: NewTableDelta(),
		statsUsage: NewStatsUsage(),
		listHead: &SessionStatsItem{
			mapper:     NewTableDelta(),
			statsUsage: NewStatsUsage(),
		},
	}
}

// NewSessionStatsItem allocates a stats collector for a session.
func (sl *SessionStatsList) NewSessionStatsItem() *SessionStatsItem {
	sl.listHead.Lock()
	defer sl.listHead.Unlock()
	newCollector := &SessionStatsItem{
		mapper:     NewTableDelta(),
		next:       sl.listHead.next,
		statsUsage: NewStatsUsage(),
	}
	sl.listHead.next = newCollector
	return newCollector
}

// SweepSessionStatsList will loop over the list, merge each session's local stats into handle
// and remove closed session's collector.
func (sl *SessionStatsList) SweepSessionStatsList() {
	deltaMap := NewTableDelta()
	colMap := NewStatsUsage()
	prev := sl.listHead
	prev.Lock()
	for curr := prev.next; curr != nil; curr = curr.next {
		curr.Lock()
		// Merge the session stats into deltaMap respectively.
		merge(curr, deltaMap, colMap)
		if curr.deleted {
			prev.next = curr.next
			// Since the session is already closed, we can safely unlock it here.
			curr.Unlock()
		} else {
			// Unlock the previous lock, so we only holds at most two session's lock at the same time.
			prev.Unlock()
			prev = curr
		}
	}
	prev.Unlock()
	sl.tableDelta.Merge(deltaMap.GetDeltaAndReset())
	sl.statsUsage.Merge(colMap.GetUsageAndReset())
}

// SessionTableDelta returns the current *TableDelta.
func (sl *SessionStatsList) SessionTableDelta() *TableDelta {
	return sl.tableDelta
}

// SessionStatsUsage returns the current *StatsUsage.
func (sl *SessionStatsList) SessionStatsUsage() *StatsUsage {
	return sl.statsUsage
}

// ResetSessionStatsList resets this list.
func (sl *SessionStatsList) ResetSessionStatsList() {
	sl.listHead.ClearForTest()
	sl.tableDelta.Reset()
	sl.statsUsage.Reset()
}

// TableDelta is used to collect tables' change information.
// All methods of it are thread-safe.
type TableDelta struct {
	delta map[int64]variable.TableDelta // map[tableID]delta
	lock  sync.Mutex
}

// NewTableDelta creates a new TableDelta.
func NewTableDelta() *TableDelta {
	return &TableDelta{
		delta: make(map[int64]variable.TableDelta),
	}
}

// Reset resets the TableDelta.
func (m *TableDelta) Reset() {
	m.lock.Lock()
	defer m.lock.Unlock()
	m.delta = make(map[int64]variable.TableDelta)
}

// GetDeltaAndReset gets the delta and resets the TableDelta.
func (m *TableDelta) GetDeltaAndReset() map[int64]variable.TableDelta {
	m.lock.Lock()
	defer m.lock.Unlock()
	ret := m.delta
	m.delta = make(map[int64]variable.TableDelta)
	return ret
}

// Update updates the delta of the table.
func (m *TableDelta) Update(id int64, delta int64, count int64, colSize *map[int64]int64) {
	m.lock.Lock()
	defer m.lock.Unlock()
	UpdateTableDeltaMap(m.delta, id, delta, count, colSize)
}

// Merge merges the deltaMap into the TableDelta.
func (m *TableDelta) Merge(deltaMap map[int64]variable.TableDelta) {
	if len(deltaMap) == 0 {
		return
	}
	m.lock.Lock()
	defer m.lock.Unlock()
	for id, item := range deltaMap {
		UpdateTableDeltaMap(m.delta, id, item.Delta, item.Count, &item.ColSize)
	}
}

// UpdateTableDeltaMap updates the delta of the table.
func UpdateTableDeltaMap(m map[int64]variable.TableDelta, id int64, delta int64, count int64, colSize *map[int64]int64) {
	item := m[id]
	item.Delta += delta
	item.Count += count
	if item.ColSize == nil {
		item.ColSize = make(map[int64]int64)
	}
	if colSize != nil {
		for key, val := range *colSize {
			item.ColSize[key] += val
		}
	}
	m[id] = item
}

// StatsUsage maps (tableID, columnID) to the last time when the column stats are used(needed).
// All methods of it are thread-safe.
type StatsUsage struct {
	usage map[model.TableItemID]time.Time
	lock  sync.RWMutex
}

// NewStatsUsage creates a new StatsUsage.
func NewStatsUsage() *StatsUsage {
	return &StatsUsage{
		usage: make(map[model.TableItemID]time.Time),
	}
}

// Reset resets the StatsUsage.
func (m *StatsUsage) Reset() {
	m.lock.Lock()
	defer m.lock.Unlock()
	m.usage = make(map[model.TableItemID]time.Time)
}

// GetUsageAndReset gets the usage and resets the StatsUsage.
func (m *StatsUsage) GetUsageAndReset() map[model.TableItemID]time.Time {
	m.lock.Lock()
	defer m.lock.Unlock()
	ret := m.usage
	m.usage = make(map[model.TableItemID]time.Time)
	return ret
}

// Merge merges the usageMap into the StatsUsage.
func (m *StatsUsage) Merge(other map[model.TableItemID]time.Time) {
	if len(other) == 0 {
		return
	}
	m.lock.Lock()
	defer m.lock.Unlock()
	for id, t := range other {
		if mt, ok := m.usage[id]; !ok || mt.Before(t) {
			m.usage[id] = t
		}
	}
}
