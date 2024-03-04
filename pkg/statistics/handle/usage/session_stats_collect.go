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
	"cmp"
	"slices"
	"strings"
	"sync"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/pkg/infoschema"
	"github.com/pingcap/tidb/pkg/metrics"
	"github.com/pingcap/tidb/pkg/parser/model"
	"github.com/pingcap/tidb/pkg/sessionctx"
	"github.com/pingcap/tidb/pkg/sessionctx/variable"
	"github.com/pingcap/tidb/pkg/statistics/handle/storage"
	utilstats "github.com/pingcap/tidb/pkg/statistics/handle/util"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util"
	"github.com/pingcap/tidb/pkg/util/sqlescape"
)

var (
	// DumpStatsDeltaRatio is the lower bound of `Modify Count / Table Count` for stats delta to be dumped.
	DumpStatsDeltaRatio = 1 / 10000.0
	// dumpStatsMaxDuration is the max duration since last update.
	dumpStatsMaxDuration = time.Hour

	// batchInsertSize is the batch size used by internal SQL to insert values to some system table.
	batchInsertSize = 10
)

// needDumpStatsDelta checks whether to dump stats delta.
// 1. If the table doesn't exist or is a mem table or system table, then return false.
// 2. If the mode is DumpAll, then return true.
// 3. If the stats delta haven't been dumped in the past hour, then return true.
// 4. If the table stats is pseudo or empty or `Modify Count / Table Count` exceeds the threshold.
func (s *statsUsageImpl) needDumpStatsDelta(is infoschema.InfoSchema, dumpAll bool, id int64, item variable.TableDelta, currentTime time.Time) bool {
	tbl, ok := s.statsHandle.TableInfoByID(is, id)
	if !ok {
		return false
	}
	dbInfo, ok := infoschema.SchemaByTable(is, tbl.Meta())
	if !ok {
		return false
	}
	if util.IsMemOrSysDB(dbInfo.Name.L) {
		return false
	}
	if dumpAll {
		return true
	}
	if item.InitTime.IsZero() {
		item.InitTime = currentTime
	}
	if currentTime.Sub(item.InitTime) > dumpStatsMaxDuration {
		// Dump the stats to kv at least once an hour.
		return true
	}
	statsTbl := s.statsHandle.GetPartitionStats(tbl.Meta(), id)
	if statsTbl.Pseudo || statsTbl.RealtimeCount == 0 || float64(item.Count)/float64(statsTbl.RealtimeCount) > DumpStatsDeltaRatio {
		// Dump the stats when there are many modifications.
		return true
	}
	return false
}

// DumpStatsDeltaToKV sweeps the whole list and updates the global map, then we dumps every table that held in map to KV.
// If the mode is `DumpDelta`, it will only dump that delta info that `Modify Count / Table Count` greater than a ratio.
func (s *statsUsageImpl) DumpStatsDeltaToKV(dumpAll bool) error {
	start := time.Now()
	defer func() {
		dur := time.Since(start)
		metrics.StatsDeltaUpdateHistogram.Observe(dur.Seconds())
	}()
	s.SweepSessionStatsList()
	deltaMap := s.SessionTableDelta().GetDeltaAndReset()
	defer func() {
		s.SessionTableDelta().Merge(deltaMap)
	}()

	return utilstats.CallWithSCtx(s.statsHandle.SPool(), func(sctx sessionctx.Context) error {
		is := sctx.GetDomainInfoSchema().(infoschema.InfoSchema)
		currentTime := time.Now()
		for id, item := range deltaMap {
			if !s.needDumpStatsDelta(is, dumpAll, id, item, currentTime) {
				continue
			}
			updated, err := s.dumpTableStatCountToKV(is, id, item)
			if err != nil {
				return errors.Trace(err)
			}
			if updated {
				UpdateTableDeltaMap(deltaMap, id, -item.Delta, -item.Count, nil)
			}
			if err = storage.DumpTableStatColSizeToKV(sctx, id, item); err != nil {
				delete(deltaMap, id)
				return errors.Trace(err)
			}
			if updated {
				delete(deltaMap, id)
			} else {
				m := deltaMap[id]
				m.ColSize = nil
				deltaMap[id] = m
			}
		}
		return nil
	})
}

// dumpTableStatDeltaToKV dumps a single delta with some table to KV and updates the version.
// For a partitioned table, we will update its global-stats as well.
func (s *statsUsageImpl) dumpTableStatCountToKV(is infoschema.InfoSchema, physicalTableID int64, delta variable.TableDelta) (updated bool, err error) {
	statsVersion := uint64(0)
	defer func() {
		if err == nil && statsVersion != 0 {
			s.statsHandle.RecordHistoricalStatsMeta(physicalTableID, statsVersion, "flush stats", false)
		}
	}()
	if delta.Count == 0 {
		return true, nil
	}

	err = utilstats.CallWithSCtx(s.statsHandle.SPool(), func(sctx sessionctx.Context) error {
		statsVersion, err = utilstats.GetStartTS(sctx)
		if err != nil {
			return errors.Trace(err)
		}

		tbl, _, _ := is.FindTableByPartitionID(physicalTableID)
		// Check if the table and its partitions are locked.
		tidAndPid := make([]int64, 0, 2)
		if tbl != nil {
			tidAndPid = append(tidAndPid, tbl.Meta().ID)
		}
		tidAndPid = append(tidAndPid, physicalTableID)
		lockedTables, err := s.statsHandle.GetLockedTables(tidAndPid...)
		if err != nil {
			return err
		}

		var affectedRows uint64
		// If it's a partitioned table and its global-stats exists,
		// update its count and modify_count as well.
		if tbl != nil {
			// We need to check if the table and the partition are locked.
			isTableLocked := false
			isPartitionLocked := false
			tableID := tbl.Meta().ID
			if _, ok := lockedTables[tableID]; ok {
				isTableLocked = true
			}
			if _, ok := lockedTables[physicalTableID]; ok {
				isPartitionLocked = true
			}
			tableOrPartitionLocked := isTableLocked || isPartitionLocked
			if err = storage.UpdateStatsMeta(sctx, statsVersion, delta,
				physicalTableID, tableOrPartitionLocked); err != nil {
				return err
			}
			affectedRows += sctx.GetSessionVars().StmtCtx.AffectedRows()
			// If the partition is locked, we don't need to update the global-stats.
			// We will update its global-stats when the partition is unlocked.
			// 1. If table is locked and partition is locked, we only stash the delta in the partition's lock info.
			//    we will update its global-stats when the partition is unlocked.
			// 2. If table is locked and partition is not locked(new partition after lock), we only stash the delta in the table's lock info.
			//    we will update its global-stats when the table is unlocked. We don't need to specially handle this case.
			//    Because updateStatsMeta will insert a new record if the record doesn't exist.
			// 3. If table is not locked and partition is locked, we only stash the delta in the partition's lock info.
			//    we will update its global-stats when the partition is unlocked.
			// 4. If table is not locked and partition is not locked, we update the global-stats.
			// To sum up, we only need to update the global-stats when the table and the partition are not locked.
			if !isTableLocked && !isPartitionLocked {
				// If it's a partitioned table and its global-stats exists, update its count and modify_count as well.
				if err = storage.UpdateStatsMeta(sctx, statsVersion, delta, tableID, isTableLocked); err != nil {
					return err
				}
				affectedRows += sctx.GetSessionVars().StmtCtx.AffectedRows()
			}
		} else {
			// This is a non-partitioned table.
			// Check if it's locked.
			isTableLocked := false
			if _, ok := lockedTables[physicalTableID]; ok {
				isTableLocked = true
			}
			if err = storage.UpdateStatsMeta(sctx, statsVersion, delta,
				physicalTableID, isTableLocked); err != nil {
				return err
			}
			affectedRows += sctx.GetSessionVars().StmtCtx.AffectedRows()
		}

		updated = affectedRows > 0
		return nil
	}, utilstats.FlagWrapTxn)
	return
}

// DumpColStatsUsageToKV sweeps the whole list, updates the column stats usage map and dumps it to KV.
func (s *statsUsageImpl) DumpColStatsUsageToKV() error {
	if !variable.EnableColumnTracking.Load() {
		return nil
	}
	s.SweepSessionStatsList()
	colMap := s.SessionStatsUsage().GetUsageAndReset()
	defer func() {
		s.SessionStatsUsage().Merge(colMap)
	}()
	type pair struct {
		lastUsedAt string
		tblColID   model.TableItemID
	}
	pairs := make([]pair, 0, len(colMap))
	for id, t := range colMap {
		pairs = append(pairs, pair{tblColID: id, lastUsedAt: t.UTC().Format(types.TimeFormat)})
	}
	slices.SortFunc(pairs, func(i, j pair) int {
		if i.tblColID.TableID == j.tblColID.TableID {
			return cmp.Compare(i.tblColID.ID, j.tblColID.ID)
		}
		return cmp.Compare(i.tblColID.TableID, j.tblColID.TableID)
	})
	// Use batch insert to reduce cost.
	for i := 0; i < len(pairs); i += batchInsertSize {
		end := i + batchInsertSize
		if end > len(pairs) {
			end = len(pairs)
		}
		sql := new(strings.Builder)
		sqlescape.MustFormatSQL(sql, "INSERT INTO mysql.column_stats_usage (table_id, column_id, last_used_at) VALUES ")
		for j := i; j < end; j++ {
			// Since we will use some session from session pool to execute the insert statement, we pass in UTC time here and covert it
			// to the session's time zone when executing the insert statement. In this way we can make the stored time right.
			sqlescape.MustFormatSQL(sql, "(%?, %?, CONVERT_TZ(%?, '+00:00', @@TIME_ZONE))", pairs[j].tblColID.TableID, pairs[j].tblColID.ID, pairs[j].lastUsedAt)
			if j < end-1 {
				sqlescape.MustFormatSQL(sql, ",")
			}
		}
		sqlescape.MustFormatSQL(sql, " ON DUPLICATE KEY UPDATE last_used_at = CASE WHEN last_used_at IS NULL THEN VALUES(last_used_at) ELSE GREATEST(last_used_at, VALUES(last_used_at)) END")
		if err := utilstats.CallWithSCtx(s.statsHandle.SPool(), func(sctx sessionctx.Context) error {
			_, _, err := utilstats.ExecRows(sctx, sql.String())
			return err
		}); err != nil {
			return errors.Trace(err)
		}

		for j := i; j < end; j++ {
			delete(colMap, pairs[j].tblColID)
		}
	}
	return nil
}

// NewSessionStatsItem allocates a stats collector for a session.
func (s *statsUsageImpl) NewSessionStatsItem() any {
	return s.SessionStatsList.NewSessionStatsItem()
}

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
