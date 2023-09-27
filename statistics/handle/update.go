// Copyright 2017 PingCAP, Inc.
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

package handle

import (
	"cmp"
	"context"
	"fmt"
	"slices"
	"strings"
	"sync"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/infoschema"
	"github.com/pingcap/tidb/parser/model"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/sessionctx/variable"
	"github.com/pingcap/tidb/statistics/handle/autoanalyze"
	"github.com/pingcap/tidb/statistics/handle/cache"
	"github.com/pingcap/tidb/statistics/handle/usage"
	utilstats "github.com/pingcap/tidb/statistics/handle/util"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util"
	"github.com/pingcap/tidb/util/sqlexec"
)

func merge(s *SessionStatsCollector, deltaMap *usage.TableDelta, colMap *usage.StatsUsage) {
	deltaMap.Merge(s.mapper.GetDeltaAndReset())
	colMap.Merge(s.statsUsage.GetUsageAndReset())
}

// SessionStatsCollector is a list item that holds the delta mapper. If you want to write or read mapper, you must lock it.
type SessionStatsCollector struct {
	mapper     *usage.TableDelta
	statsUsage *usage.StatsUsage
	next       *SessionStatsCollector
	sync.Mutex

	// deleted is set to true when a session is closed. Every time we sweep the list, we will remove the useless collector.
	deleted bool
}

// NewSessionStatsCollector initializes a new SessionStatsCollector.
func NewSessionStatsCollector() *SessionStatsCollector {
	return &SessionStatsCollector{
		mapper:     usage.NewTableDelta(),
		statsUsage: usage.NewStatsUsage(),
	}
}

// Delete only sets the deleted flag true, it will be deleted from list when DumpStatsDeltaToKV is called.
func (s *SessionStatsCollector) Delete() {
	s.Lock()
	defer s.Unlock()
	s.deleted = true
}

// Update will updates the delta and count for one table id.
func (s *SessionStatsCollector) Update(id int64, delta int64, count int64, colSize *map[int64]int64) {
	s.Lock()
	defer s.Unlock()
	s.mapper.Update(id, delta, count, colSize)
}

// ClearForTest clears the mapper for test.
func (s *SessionStatsCollector) ClearForTest() {
	s.Lock()
	defer s.Unlock()
	s.mapper = usage.NewTableDelta()
	s.statsUsage = usage.NewStatsUsage()
	s.next = nil
	s.deleted = false
}

// UpdateColStatsUsage updates the last time when the column stats are used(needed).
func (s *SessionStatsCollector) UpdateColStatsUsage(colMap map[model.TableItemID]time.Time) {
	s.Lock()
	defer s.Unlock()
	s.statsUsage.Merge(colMap)
}

// NewSessionStatsCollector allocates a stats collector for a session.
func (h *Handle) NewSessionStatsCollector() *SessionStatsCollector {
	h.listHead.Lock()
	defer h.listHead.Unlock()
	newCollector := &SessionStatsCollector{
		mapper:     usage.NewTableDelta(),
		next:       h.listHead.next,
		statsUsage: usage.NewStatsUsage(),
	}
	h.listHead.next = newCollector
	return newCollector
}

// NewSessionIndexUsageCollector will add a new SessionIndexUsageCollector into linked list headed by idxUsageListHead.
// idxUsageListHead always points to an empty SessionIndexUsageCollector as a sentinel node. So we let idxUsageListHead.next
// points to new item. It's helpful to sweepIdxUsageList.
func (h *Handle) NewSessionIndexUsageCollector() *usage.SessionIndexUsageCollector {
	return usage.NewSessionIndexUsageCollector(h.idxUsageListHead)
}

// batchInsertSize is the batch size used by internal SQL to insert values to some system table.
const batchInsertSize = 10

// maxInsertLength is the length limit for internal insert SQL.
const maxInsertLength = 1024 * 1024

// DumpIndexUsageToKV will dump in-memory index usage information to KV.
func (h *Handle) DumpIndexUsageToKV() error {
	return h.callWithSCtx(func(sctx sessionctx.Context) error {
		return usage.DumpIndexUsageToKV(sctx, h.idxUsageListHead)
	})
}

func (h *Handle) callWithSCtx(f func(sctx sessionctx.Context) error) (err error) {
	se, err := h.pool.Get()
	if err != nil {
		return err
	}
	defer func() {
		if err == nil { // only recycle when no error
			h.pool.Put(se)
		}
	}()
	sctx := se.(sessionctx.Context)
	if err := UpdateSCtxVarsForStats(sctx); err != nil { // update stats variables automatically
		return err
	}
	return f(sctx)
}

// GCIndexUsage will delete the usage information of those indexes that do not exist.
func (h *Handle) GCIndexUsage() error {
	return h.callWithSCtx(usage.GCIndexUsageOnKV)
}

var (
	// DumpStatsDeltaRatio is the lower bound of `Modify Count / Table Count` for stats delta to be dumped.
	DumpStatsDeltaRatio = 1 / 10000.0
	// dumpStatsMaxDuration is the max duration since last update.
	dumpStatsMaxDuration = time.Hour
)

// needDumpStatsDelta checks whether to dump stats delta.
// 1. If the table doesn't exist or is a mem table or system table, then return false.
// 2. If the mode is DumpAll, then return true.
// 3. If the stats delta haven't been dumped in the past hour, then return true.
// 4. If the table stats is pseudo or empty or `Modify Count / Table Count` exceeds the threshold.
func (h *Handle) needDumpStatsDelta(is infoschema.InfoSchema, mode dumpMode, id int64, item variable.TableDelta, currentTime time.Time) bool {
	tbl, ok := h.getTableByPhysicalID(is, id)
	if !ok {
		return false
	}
	dbInfo, ok := is.SchemaByTable(tbl.Meta())
	if !ok {
		return false
	}
	if util.IsMemOrSysDB(dbInfo.Name.L) {
		return false
	}
	if mode == DumpAll {
		return true
	}
	if item.InitTime.IsZero() {
		item.InitTime = currentTime
	}
	if currentTime.Sub(item.InitTime) > dumpStatsMaxDuration {
		// Dump the stats to kv at least once an hour.
		return true
	}
	statsTbl := h.GetPartitionStats(tbl.Meta(), id)
	if statsTbl.Pseudo || statsTbl.RealtimeCount == 0 || float64(item.Count)/float64(statsTbl.RealtimeCount) > DumpStatsDeltaRatio {
		// Dump the stats when there are many modifications.
		return true
	}
	return false
}

type dumpMode bool

const (
	// DumpAll indicates dump all the delta info in to kv.
	DumpAll dumpMode = true
	// DumpDelta indicates dump part of the delta info in to kv.
	DumpDelta dumpMode = false
)

// sweepList will loop over the list, merge each session's local stats into handle
// and remove closed session's collector.
func (h *Handle) sweepList() {
	deltaMap := usage.NewTableDelta()
	colMap := usage.NewStatsUsage()
	prev := h.listHead
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
	h.tableDelta.Merge(deltaMap.GetDeltaAndReset())
	h.statsUsage.Merge(colMap.GetUsageAndReset())
}

// DumpStatsDeltaToKV sweeps the whole list and updates the global map, then we dumps every table that held in map to KV.
// If the mode is `DumpDelta`, it will only dump that delta info that `Modify Count / Table Count` greater than a ratio.
func (h *Handle) DumpStatsDeltaToKV(mode dumpMode) error {
	h.sweepList()
	deltaMap := h.tableDelta.GetDeltaAndReset()
	defer func() {
		h.tableDelta.Merge(deltaMap)
	}()

	se, err := h.pool.Get()
	if err != nil {
		return err
	}
	defer h.pool.Put(se)
	sctx := se.(sessionctx.Context)
	is := sctx.GetDomainInfoSchema().(infoschema.InfoSchema)
	currentTime := time.Now()
	for id, item := range deltaMap {
		if !h.needDumpStatsDelta(is, mode, id, item, currentTime) {
			continue
		}
		updated, err := h.dumpTableStatCountToKV(is, id, item)
		if err != nil {
			return errors.Trace(err)
		}
		if updated {
			usage.UpdateTableDeltaMap(deltaMap, id, -item.Delta, -item.Count, nil)
		}
		if err = h.dumpTableStatColSizeToKV(id, item); err != nil {
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
}

// dumpTableStatDeltaToKV dumps a single delta with some table to KV and updates the version.
func (h *Handle) dumpTableStatCountToKV(is infoschema.InfoSchema, physicalTableID int64, delta variable.TableDelta) (updated bool, err error) {
	statsVersion := uint64(0)
	defer func() {
		if err == nil && statsVersion != 0 {
			h.recordHistoricalStatsMeta(physicalTableID, statsVersion, StatsMetaHistorySourceFlushStats)
		}
	}()
	if delta.Count == 0 {
		return true, nil
	}

	se, err := h.pool.Get()
	if err != nil {
		return false, err
	}
	defer h.pool.Put(se)
	exec := se.(sqlexec.SQLExecutor)
	sctx := se.(sessionctx.Context)
	ctx := utilstats.StatsCtx(context.Background())
	_, err = exec.ExecuteInternal(ctx, "begin")
	if err != nil {
		return false, errors.Trace(err)
	}
	defer func() {
		err = utilstats.FinishTransaction(ctx, exec, err)
	}()

	statsVersion, err = getSessionTxnStartTS(se)
	if err != nil {
		return false, errors.Trace(err)
	}

	tbl, _, _ := is.FindTableByPartitionID(physicalTableID)
	// Check if the table and its partitions are locked.
	tidAndPid := make([]int64, 0, 2)
	if tbl != nil {
		tidAndPid = append(tidAndPid, tbl.Meta().ID)
	}
	tidAndPid = append(tidAndPid, physicalTableID)
	lockedTables, err := h.GetLockedTables(tidAndPid...)
	if err != nil {
		return
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
		if err = updateStatsMeta(ctx, exec, statsVersion, delta,
			physicalTableID, tableOrPartitionLocked); err != nil {
			return
		}
		affectedRows += sctx.GetSessionVars().StmtCtx.AffectedRows()
		// If only the partition is locked, we don't need to update the global-stats.
		// We will update its global-stats when the partition is unlocked.
		if isTableLocked || !isPartitionLocked {
			// If it's a partitioned table and its global-stats exists, update its count and modify_count as well.
			if err = updateStatsMeta(ctx, exec, statsVersion, delta, tableID, isTableLocked); err != nil {
				return
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
		if err = updateStatsMeta(ctx, exec, statsVersion, delta,
			physicalTableID, isTableLocked); err != nil {
			return
		}
		affectedRows += sctx.GetSessionVars().StmtCtx.AffectedRows()
	}

	updated = affectedRows > 0
	return
}

func updateStatsMeta(
	ctx context.Context,
	exec sqlexec.SQLExecutor,
	startTS uint64,
	delta variable.TableDelta,
	id int64,
	isLocked bool,
) (err error) {
	if isLocked {
		if delta.Delta < 0 {
			_, err = exec.ExecuteInternal(ctx, "update mysql.stats_table_locked set version = %?, count = count - %?, modify_count = modify_count + %? where table_id = %? and count >= %?",
				startTS, -delta.Delta, delta.Count, id, -delta.Delta)
		} else {
			_, err = exec.ExecuteInternal(ctx, "update mysql.stats_table_locked set version = %?, count = count + %?, modify_count = modify_count + %? where table_id = %?",
				startTS, delta.Delta, delta.Count, id)
		}
	} else {
		if delta.Delta < 0 {
			// use INSERT INTO ... ON DUPLICATE KEY UPDATE here to fill missing stats_meta.
			_, err = exec.ExecuteInternal(ctx, "insert into mysql.stats_meta (version, table_id, modify_count, count) values (%?, %?, %?, 0) on duplicate key "+
				"update version = values(version), modify_count = modify_count + values(modify_count), count = if(count > %?, count - %?, 0)",
				startTS, id, delta.Count, -delta.Delta, -delta.Delta)
		} else {
			// use INSERT INTO ... ON DUPLICATE KEY UPDATE here to fill missing stats_meta.
			_, err = exec.ExecuteInternal(ctx, "insert into mysql.stats_meta (version, table_id, modify_count, count) values (%?, %?, %?, %?) on duplicate key "+
				"update version = values(version), modify_count = modify_count + values(modify_count), count = count + values(count)", startTS,
				id, delta.Count, delta.Delta)
		}
		cache.TableRowStatsCache.Invalidate(id)
	}
	return err
}

func (h *Handle) dumpTableStatColSizeToKV(id int64, delta variable.TableDelta) error {
	if len(delta.ColSize) == 0 {
		return nil
	}
	values := make([]string, 0, len(delta.ColSize))
	for histID, deltaColSize := range delta.ColSize {
		if deltaColSize == 0 {
			continue
		}
		values = append(values, fmt.Sprintf("(%d, 0, %d, 0, %d)", id, histID, deltaColSize))
	}
	if len(values) == 0 {
		return nil
	}
	ctx := utilstats.StatsCtx(context.Background())
	sql := fmt.Sprintf("insert into mysql.stats_histograms (table_id, is_index, hist_id, distinct_count, tot_col_size) "+
		"values %s on duplicate key update tot_col_size = tot_col_size + values(tot_col_size)", strings.Join(values, ","))
	_, _, err := h.execRestrictedSQL(ctx, sql)
	return errors.Trace(err)
}

// DumpColStatsUsageToKV sweeps the whole list, updates the column stats usage map and dumps it to KV.
func (h *Handle) DumpColStatsUsageToKV() error {
	if !variable.EnableColumnTracking.Load() {
		return nil
	}
	h.sweepList()
	colMap := h.statsUsage.GetUsageAndReset()
	defer func() {
		h.statsUsage.Merge(colMap)
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
		sqlexec.MustFormatSQL(sql, "INSERT INTO mysql.column_stats_usage (table_id, column_id, last_used_at) VALUES ")
		for j := i; j < end; j++ {
			// Since we will use some session from session pool to execute the insert statement, we pass in UTC time here and covert it
			// to the session's time zone when executing the insert statement. In this way we can make the stored time right.
			sqlexec.MustFormatSQL(sql, "(%?, %?, CONVERT_TZ(%?, '+00:00', @@TIME_ZONE))", pairs[j].tblColID.TableID, pairs[j].tblColID.ID, pairs[j].lastUsedAt)
			if j < end-1 {
				sqlexec.MustFormatSQL(sql, ",")
			}
		}
		sqlexec.MustFormatSQL(sql, " ON DUPLICATE KEY UPDATE last_used_at = CASE WHEN last_used_at IS NULL THEN VALUES(last_used_at) ELSE GREATEST(last_used_at, VALUES(last_used_at)) END")
		if _, _, err := h.execRestrictedSQL(context.Background(), sql.String()); err != nil {
			return errors.Trace(err)
		}
		for j := i; j < end; j++ {
			delete(colMap, pairs[j].tblColID)
		}
	}
	return nil
}

const (
	// StatsOwnerKey is the stats owner path that is saved to etcd.
	StatsOwnerKey = "/tidb/stats/owner"
	// StatsPrompt is the prompt for stats owner manager.
	StatsPrompt = "stats"
)

// HandleAutoAnalyze analyzes the newly created table or index.
func (h *Handle) HandleAutoAnalyze(is infoschema.InfoSchema) (analyzed bool) {
	_ = h.callWithSCtx(func(sctx sessionctx.Context) error {
		analyzed = autoanalyze.HandleAutoAnalyze(sctx, &autoanalyze.Opt{
			StatsLease:              h.Lease(),
			GetLockedTables:         h.GetLockedTables,
			GetTableStats:           h.GetTableStats,
			GetPartitionStats:       h.GetPartitionStats,
			SysProcTracker:          h.sysProcTracker,
			AutoAnalyzeProcIDGetter: h.autoAnalyzeProcIDGetter,
		}, is)
		return nil
	})
	return
}

// GetCurrentPruneMode returns the current latest partitioning table prune mode.
func (h *Handle) GetCurrentPruneMode() (string, error) {
	se, err := h.pool.Get()
	if err != nil {
		return "", err
	}
	defer h.pool.Put(se)
	sctx := se.(sessionctx.Context)
	if err := UpdateSCtxVarsForStats(sctx); err != nil {
		return "", err
	}
	return sctx.GetSessionVars().PartitionPruneMode.Load(), nil
}
