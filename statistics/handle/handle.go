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
// See the License for the specific language governing permissions and
// limitations under the License.

package handle

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/ngaut/pools"
	"github.com/pingcap/errors"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/parser/ast"
	"github.com/pingcap/parser/model"
	"github.com/pingcap/parser/mysql"
	"github.com/pingcap/tidb/ddl/util"
	"github.com/pingcap/tidb/infoschema"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/sessionctx/stmtctx"
	"github.com/pingcap/tidb/statistics"
	"github.com/pingcap/tidb/store/tikv/oracle"
	"github.com/pingcap/tidb/table"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util/chunk"
	"github.com/pingcap/tidb/util/logutil"
	"github.com/pingcap/tidb/util/sqlexec"
	atomic2 "go.uber.org/atomic"
	"go.uber.org/zap"
)

// statsCache caches the tables in memory for Handle.
type statsCache struct {
	tables map[int64]*statistics.Table
	// version is the latest version of cache.
	version uint64
}

// Handle can update stats info periodically.
type Handle struct {
	mu struct {
		sync.Mutex
		ctx sessionctx.Context
		// rateMap contains the error rate delta from feedback.
		rateMap errorRateDeltaMap
		// pid2tid is the map from partition ID to table ID.
		pid2tid map[int64]int64
		// schemaVersion is the version of information schema when `pid2tid` is built.
		schemaVersion int64
	}

	// It can be read by multiple readers at the same time without acquiring lock, but it can be
	// written only after acquiring the lock.
	statsCache struct {
		sync.Mutex
		atomic.Value
	}

	pool sessionPool

	// ddlEventCh is a channel to notify a ddl operation has happened.
	// It is sent only by owner or the drop stats executor, and read by stats handle.
	ddlEventCh chan *util.Event
	// listHead contains all the stats collector required by session.
	listHead *SessionStatsCollector
	// globalMap contains all the delta map from collectors when we dump them to KV.
	globalMap tableDeltaMap
	// feedback is used to store query feedback info.
	feedback *statistics.QueryFeedbackMap

	lease atomic2.Duration
<<<<<<< HEAD
=======

	// idxUsageListHead contains all the index usage collectors required by session.
	idxUsageListHead *SessionIndexUsageCollector

	// StatsLoad is used to load stats concurrently
	StatsLoad StatsLoad

	// sysProcTracker is used to track sys process like analyze
	sysProcTracker sessionctx.SysProcTracker
	// serverIDGetter is used to get server ID for generating auto analyze ID.
	serverIDGetter func() uint64
	// tableLocked used to store locked tables
	tableLocked []int64

	InitStatsDone chan struct{}
}

// GetTableLockedAndClearForTest for unit test only
func (h *Handle) GetTableLockedAndClearForTest() []int64 {
	tableLocked := h.tableLocked
	h.tableLocked = make([]int64, 0)
	return tableLocked
}

// LoadLockedTables load locked tables from store
func (h *Handle) LoadLockedTables() error {
	h.mu.Lock()
	defer h.mu.Unlock()
	ctx := kv.WithInternalSourceType(context.Background(), kv.InternalTxnStats)
	rows, _, err := h.execRestrictedSQL(ctx, "select table_id from mysql.stats_table_locked")
	if err != nil {
		return errors.Trace(err)
	}

	h.tableLocked = make([]int64, len(rows))
	for i, row := range rows {
		h.tableLocked[i] = row.GetInt64(0)
	}

	return nil
}

// AddLockedTables add locked tables id  to store
func (h *Handle) AddLockedTables(tids []int64, pids []int64, tables []*ast.TableName) (string, error) {
	h.mu.Lock()
	defer h.mu.Unlock()

	ctx := kv.WithInternalSourceType(context.Background(), kv.InternalTxnStats)

	exec := h.mu.ctx.(sqlexec.SQLExecutor)

	_, err := exec.ExecuteInternal(ctx, "begin pessimistic")
	if err != nil {
		return "", err
	}

	//load tables to check duplicate when insert
	rows, _, err := h.execRestrictedSQL(ctx, "select table_id from mysql.stats_table_locked")
	if err != nil {
		return "", err
	}

	dupTables := make([]string, 0)
	tableLocked := make([]int64, 0)
	for _, row := range rows {
		tableLocked = append(tableLocked, row.GetInt64(0))
	}

	strTids := fmt.Sprintf("%v", tids)
	logutil.BgLogger().Info("[stats] lock table ", zap.String("tableIDs", strTids))
	for i, tid := range tids {
		_, err = exec.ExecuteInternal(ctx, "insert into mysql.stats_table_locked(table_id) select %? from dual where not exists(select table_id from mysql.stats_table_locked where table_id = %?)", tid, tid)
		if err != nil {
			logutil.BgLogger().Error("[stats] error occurred when insert mysql.stats_table_locked ", zap.Error(err))
			return "", err
		}
		// update handle
		if !isTableLocked(tableLocked, tid) {
			tableLocked = append(tableLocked, tid)
		} else {
			dupTables = append(dupTables, tables[i].Schema.L+"."+tables[i].Name.L)
		}
	}

	//insert related partitions while don't warning duplicate partitions
	for _, tid := range pids {
		_, err = exec.ExecuteInternal(ctx, "insert into mysql.stats_table_locked(table_id) select %? from dual where not exists(select table_id from mysql.stats_table_locked where table_id = %?)", tid, tid)
		if err != nil {
			logutil.BgLogger().Error("[stats] error occurred when insert mysql.stats_table_locked ", zap.Error(err))
			return "", err
		}
		if !isTableLocked(tableLocked, tid) {
			tableLocked = append(tableLocked, tid)
		}
	}

	err = finishTransaction(ctx, exec, err)
	if err != nil {
		return "", err
	}
	// update handle.tableLocked after transaction success, if txn failed, tableLocked won't be updated
	h.tableLocked = tableLocked

	if len(dupTables) > 0 {
		tables := dupTables[0]
		for i, table := range dupTables {
			if i == 0 {
				continue
			}
			tables += ", " + table
		}
		var msg string
		if len(tids) > 1 {
			if len(tids) > len(dupTables) {
				msg = "skip locking locked tables: " + tables + ", other tables locked successfully"
			} else {
				msg = "skip locking locked tables: " + tables
			}
		} else {
			msg = "skip locking locked table: " + tables
		}
		return msg, err
	}
	return "", err
}

// getStatsDeltaFromTableLocked get count, modify_count and version for the given table from mysql.stats_table_locked.
func (h *Handle) getStatsDeltaFromTableLocked(ctx context.Context, tableID int64) (int64, int64, uint64, error) {
	rows, _, err := h.execRestrictedSQL(ctx, "select count, modify_count, version from mysql.stats_table_locked where table_id = %?", tableID)
	if err != nil {
		return 0, 0, 0, err
	}

	if len(rows) == 0 {
		return 0, 0, 0, nil
	}
	count := rows[0].GetInt64(0)
	modifyCount := rows[0].GetInt64(1)
	version := rows[0].GetUint64(2)
	return count, modifyCount, version, nil
}

// RemoveLockedTables remove tables from table locked array
func (h *Handle) RemoveLockedTables(tids []int64, pids []int64, tables []*ast.TableName) (string, error) {
	h.mu.Lock()
	defer h.mu.Unlock()

	ctx := kv.WithInternalSourceType(context.Background(), kv.InternalTxnStats)

	exec := h.mu.ctx.(sqlexec.SQLExecutor)
	_, err := exec.ExecuteInternal(ctx, "begin pessimistic")
	if err != nil {
		return "", err
	}

	//load tables to check unlock the unlock table
	rows, _, err := h.execRestrictedSQL(ctx, "select table_id from mysql.stats_table_locked")
	if err != nil {
		return "", err
	}

	nonlockedTables := make([]string, 0)
	tableLocked := make([]int64, 0)
	for _, row := range rows {
		tableLocked = append(tableLocked, row.GetInt64(0))
	}

	strTids := fmt.Sprintf("%v", tids)
	logutil.BgLogger().Info("[stats] unlock table ", zap.String("tableIDs", strTids))
	for i, tid := range tids {
		// get stats delta during table locked
		count, modifyCount, version, err := h.getStatsDeltaFromTableLocked(ctx, tid)
		if err != nil {
			logutil.BgLogger().Error("[stats] error occurred when getStatsDeltaFromTableLocked", zap.Error(err))
			return "", err
		}
		// update stats_meta with stats delta
		_, err = exec.ExecuteInternal(ctx, "update mysql.stats_meta set version = %?, count = count + %?, modify_count = modify_count + %? where table_id = %?", version, count, modifyCount, tid)
		if err != nil {
			logutil.BgLogger().Error("[stats] error occurred when update mysql.stats_meta", zap.Error(err))
			return "", err
		}

		_, err = exec.ExecuteInternal(ctx, "delete from mysql.stats_table_locked where table_id = %?", tid)
		if err != nil {
			logutil.BgLogger().Error("[stats] error occurred when delete from mysql.stats_table_locked ", zap.Error(err))
			return "", err
		}
		var exist bool
		exist, tableLocked = removeIfTableLocked(tableLocked, tid)
		if !exist {
			nonlockedTables = append(nonlockedTables, tables[i].Schema.L+"."+tables[i].Name.L)
		}
	}
	//delete related partitions while don't warning delete empty partitions
	for _, tid := range pids {
		// get stats delta during table locked
		count, modifyCount, version, err := h.getStatsDeltaFromTableLocked(ctx, tid)
		if err != nil {
			logutil.BgLogger().Error("[stats] error occurred when getStatsDeltaFromTableLocked", zap.Error(err))
			return "", err
		}
		// update stats_meta with stats delta
		_, err = exec.ExecuteInternal(ctx, "update mysql.stats_meta set version = %?, count = count + %?, modify_count = modify_count + %? where table_id = %?", version, count, modifyCount, tid)
		if err != nil {
			logutil.BgLogger().Error("[stats] error occurred when update mysql.stats_meta", zap.Error(err))
			return "", err
		}

		_, err = exec.ExecuteInternal(ctx, "delete from mysql.stats_table_locked where table_id = %?", tid)
		if err != nil {
			logutil.BgLogger().Error("[stats] error occurred when delete from mysql.stats_table_locked ", zap.Error(err))
			return "", err
		}
		_, tableLocked = removeIfTableLocked(tableLocked, tid)
	}

	err = finishTransaction(ctx, exec, err)
	if err != nil {
		return "", err
	}
	// update handle.tableLocked after transaction success, if txn failed, tableLocked won't be updated
	h.tableLocked = tableLocked

	if len(nonlockedTables) > 0 {
		tables := nonlockedTables[0]
		for i, table := range nonlockedTables {
			if i == 0 {
				continue
			}
			tables += ", " + table
		}
		var msg string
		if len(tids) > 1 {
			if len(tids) > len(nonlockedTables) {
				msg = "skip unlocking non-locked tables: " + tables + ", other tables unlocked successfully"
			} else {
				msg = "skip unlocking non-locked tables: " + tables
			}
		} else {
			msg = "skip unlocking non-locked table: " + tables
		}
		return msg, err
	}
	return "", err
}

// IsTableLocked check whether table is locked in handle with Handle.Mutex
func (h *Handle) IsTableLocked(tableID int64) bool {
	h.mu.RLock()
	defer h.mu.RUnlock()
	return h.isTableLocked(tableID)
}

// IsTableLocked check whether table is locked in handle without Handle.Mutex
func (h *Handle) isTableLocked(tableID int64) bool {
	return isTableLocked(h.tableLocked, tableID)
}

// isTableLocked check whether table is locked
func isTableLocked(tableLocked []int64, tableID int64) bool {
	return lockTableIndexOf(tableLocked, tableID) > -1
}

// lockTableIndexOf get the locked table's index in the array
func lockTableIndexOf(tableLocked []int64, tableID int64) int {
	for idx, id := range tableLocked {
		if id == tableID {
			return idx
		}
	}
	return -1
}

// removeIfTableLocked try to remove the table from table locked array
func removeIfTableLocked(tableLocked []int64, tableID int64) (bool, []int64) {
	idx := lockTableIndexOf(tableLocked, tableID)
	if idx > -1 {
		tableLocked = append(tableLocked[:idx], tableLocked[idx+1:]...)
	}
	return idx > -1, tableLocked
>>>>>>> 50dd8b40f1c (*: provide a option to wait for init stats to finish before providing service during startup (#43381))
}

func (h *Handle) withRestrictedSQLExecutor(ctx context.Context, fn func(context.Context, sqlexec.RestrictedSQLExecutor) ([]chunk.Row, []*ast.ResultField, error)) ([]chunk.Row, []*ast.ResultField, error) {
	se, err := h.pool.Get()
	if err != nil {
		return nil, nil, errors.Trace(err)
	}
	defer h.pool.Put(se)

	exec := se.(sqlexec.RestrictedSQLExecutor)
	return fn(ctx, exec)
}

func (h *Handle) execRestrictedSQL(ctx context.Context, sql string, params ...interface{}) ([]chunk.Row, []*ast.ResultField, error) {
	return h.withRestrictedSQLExecutor(ctx, func(ctx context.Context, exec sqlexec.RestrictedSQLExecutor) ([]chunk.Row, []*ast.ResultField, error) {
		stmt, err := exec.ParseWithParams(ctx, sql, params...)
		if err != nil {
			return nil, nil, errors.Trace(err)
		}
		return exec.ExecRestrictedStmt(ctx, stmt)
	})
}

func (h *Handle) execRestrictedSQLWithSnapshot(ctx context.Context, sql string, snapshot uint64, params ...interface{}) ([]chunk.Row, []*ast.ResultField, error) {
	return h.withRestrictedSQLExecutor(ctx, func(ctx context.Context, exec sqlexec.RestrictedSQLExecutor) ([]chunk.Row, []*ast.ResultField, error) {
		stmt, err := exec.ParseWithParams(ctx, sql, params...)
		if err != nil {
			return nil, nil, errors.Trace(err)
		}
		return exec.ExecRestrictedStmt(ctx, stmt, sqlexec.ExecOptionWithSnapshot(snapshot))
	})
}

// Clear the statsCache, only for test.
func (h *Handle) Clear() {
	h.mu.Lock()
	h.statsCache.Store(statsCache{tables: make(map[int64]*statistics.Table)})
	for len(h.ddlEventCh) > 0 {
		<-h.ddlEventCh
	}
	h.feedback = statistics.NewQueryFeedbackMap()
	h.mu.ctx.GetSessionVars().InitChunkSize = 1
	h.mu.ctx.GetSessionVars().MaxChunkSize = 1
	h.mu.ctx.GetSessionVars().EnableChunkRPC = false
	h.mu.ctx.GetSessionVars().ProjectionConcurrency = 0
	h.listHead = &SessionStatsCollector{mapper: make(tableDeltaMap), rateMap: make(errorRateDeltaMap)}
	h.globalMap = make(tableDeltaMap)
	h.mu.rateMap = make(errorRateDeltaMap)
	h.mu.Unlock()
}

type sessionPool interface {
	Get() (pools.Resource, error)
	Put(pools.Resource)
}

// NewHandle creates a Handle for update stats.
func NewHandle(ctx sessionctx.Context, lease time.Duration, pool sessionPool) *Handle {
	handle := &Handle{
<<<<<<< HEAD
		ddlEventCh: make(chan *util.Event, 100),
		listHead:   &SessionStatsCollector{mapper: make(tableDeltaMap), rateMap: make(errorRateDeltaMap)},
		globalMap:  make(tableDeltaMap),
		feedback:   statistics.NewQueryFeedbackMap(),
		pool:       pool,
=======
		ddlEventCh:       make(chan *ddlUtil.Event, 1000),
		listHead:         &SessionStatsCollector{mapper: make(tableDeltaMap), rateMap: make(errorRateDeltaMap)},
		idxUsageListHead: &SessionIndexUsageCollector{mapper: make(indexUsageMap)},
		pool:             pool,
		sysProcTracker:   tracker,
		serverIDGetter:   serverIDGetter,
		InitStatsDone:    make(chan struct{}),
>>>>>>> 50dd8b40f1c (*: provide a option to wait for init stats to finish before providing service during startup (#43381))
	}
	handle.lease.Store(lease)
	handle.mu.ctx = ctx
	handle.mu.rateMap = make(errorRateDeltaMap)
	handle.statsCache.Store(statsCache{tables: make(map[int64]*statistics.Table)})
	return handle
}

// Lease returns the stats lease.
func (h *Handle) Lease() time.Duration {
	return h.lease.Load()
}

// SetLease sets the stats lease.
func (h *Handle) SetLease(lease time.Duration) {
	h.lease.Store(lease)
}

// GetQueryFeedback gets the query feedback. It is only used in test.
func (h *Handle) GetQueryFeedback() *statistics.QueryFeedbackMap {
	defer func() {
		h.feedback = statistics.NewQueryFeedbackMap()
	}()
	return h.feedback
}

// DurationToTS converts duration to timestamp.
func DurationToTS(d time.Duration) uint64 {
	return oracle.ComposeTS(d.Nanoseconds()/int64(time.Millisecond), 0)
}

// Update reads stats meta from store and updates the stats map.
func (h *Handle) Update(is infoschema.InfoSchema) error {
	oldCache := h.statsCache.Load().(statsCache)
	lastVersion := oldCache.version
	// We need this because for two tables, the smaller version may write later than the one with larger version.
	// Consider the case that there are two tables A and B, their version and commit time is (A0, A1) and (B0, B1),
	// and A0 < B0 < B1 < A1. We will first read the stats of B, and update the lastVersion to B0, but we cannot read
	// the table stats of A0 if we read stats that greater than lastVersion which is B0.
	// We can read the stats if the diff between commit time and version is less than three lease.
	offset := DurationToTS(3 * h.Lease())
	if oldCache.version >= offset {
		lastVersion = lastVersion - offset
	} else {
		lastVersion = 0
	}
	ctx := context.Background()
	rows, _, err := h.execRestrictedSQL(ctx, "SELECT version, table_id, modify_count, count from mysql.stats_meta where version > %? order by version", lastVersion)
	if err != nil {
		return errors.Trace(err)
	}

	tables := make([]*statistics.Table, 0, len(rows))
	deletedTableIDs := make([]int64, 0, len(rows))
	for _, row := range rows {
		version := row.GetUint64(0)
		physicalID := row.GetInt64(1)
		modifyCount := row.GetInt64(2)
		count := row.GetInt64(3)
		lastVersion = version
		h.mu.Lock()
		table, ok := h.getTableByPhysicalID(is, physicalID)
		h.mu.Unlock()
		if !ok {
			logutil.BgLogger().Debug("unknown physical ID in stats meta table, maybe it has been dropped", zap.Int64("ID", physicalID))
			deletedTableIDs = append(deletedTableIDs, physicalID)
			continue
		}
		tableInfo := table.Meta()
		if oldTbl, ok := oldCache.tables[physicalID]; ok && oldTbl.Version >= version && tableInfo.UpdateTS == oldTbl.TblInfoUpdateTS {
			continue
		}
		tbl, err := h.TableStatsFromStorage(tableInfo, physicalID, false, 0)
		// Error is not nil may mean that there are some ddl changes on this table, we will not update it.
		if err != nil {
			logutil.BgLogger().Error("[stats] error occurred when read table stats", zap.String("table", tableInfo.Name.O), zap.Error(err))
			continue
		}
		if tbl == nil {
			deletedTableIDs = append(deletedTableIDs, physicalID)
			continue
		}
		tbl.Version = version
		tbl.Count = count
		tbl.ModifyCount = modifyCount
		tbl.Name = getFullTableName(is, tableInfo)
		tbl.TblInfoUpdateTS = tableInfo.UpdateTS
		tables = append(tables, tbl)
	}
	h.updateStatsCache(oldCache.update(tables, deletedTableIDs, lastVersion))
	return nil
}

func (h *Handle) getTableByPhysicalID(is infoschema.InfoSchema, physicalID int64) (table.Table, bool) {
	if is.SchemaMetaVersion() != h.mu.schemaVersion {
		h.mu.schemaVersion = is.SchemaMetaVersion()
		h.mu.pid2tid = buildPartitionID2TableID(is)
	}
	if id, ok := h.mu.pid2tid[physicalID]; ok {
		return is.TableByID(id)
	}
	return is.TableByID(physicalID)
}

func buildPartitionID2TableID(is infoschema.InfoSchema) map[int64]int64 {
	mapper := make(map[int64]int64)
	for _, db := range is.AllSchemas() {
		tbls := db.Tables
		for _, tbl := range tbls {
			pi := tbl.GetPartitionInfo()
			if pi == nil {
				continue
			}
			for _, def := range pi.Definitions {
				mapper[def.ID] = tbl.ID
			}
		}
	}
	return mapper
}

// GetTableStats retrieves the statistics table from cache, and the cache will be updated by a goroutine.
func (h *Handle) GetTableStats(tblInfo *model.TableInfo) *statistics.Table {
	return h.GetPartitionStats(tblInfo, tblInfo.ID)
}

// GetPartitionStats retrieves the partition stats from cache.
func (h *Handle) GetPartitionStats(tblInfo *model.TableInfo, pid int64) *statistics.Table {
	statsCache := h.statsCache.Load().(statsCache)
	tbl, ok := statsCache.tables[pid]
	if !ok {
		tbl = statistics.PseudoTable(tblInfo)
		tbl.PhysicalID = pid
		h.updateStatsCache(statsCache.update([]*statistics.Table{tbl}, nil, statsCache.version))
		return tbl
	}
	return tbl
}

func (h *Handle) updateStatsCache(newCache statsCache) {
	h.statsCache.Lock()
	oldCache := h.statsCache.Load().(statsCache)
	if oldCache.version <= newCache.version {
		h.statsCache.Store(newCache)
	}
	h.statsCache.Unlock()
}

func (sc statsCache) copy() statsCache {
	newCache := statsCache{tables: make(map[int64]*statistics.Table, len(sc.tables)), version: sc.version}
	for k, v := range sc.tables {
		newCache.tables[k] = v
	}
	return newCache
}

// update updates the statistics table cache using copy on write.
func (sc statsCache) update(tables []*statistics.Table, deletedIDs []int64, newVersion uint64) statsCache {
	newCache := sc.copy()
	newCache.version = newVersion
	for _, tbl := range tables {
		id := tbl.PhysicalID
		newCache.tables[id] = tbl
	}
	for _, id := range deletedIDs {
		delete(newCache.tables, id)
	}
	return newCache
}

// LoadNeededHistograms will load histograms for those needed columns.
func (h *Handle) LoadNeededHistograms() (err error) {
	cols := statistics.HistogramNeededColumns.AllCols()
	reader, err := h.getStatsReader(0)
	if err != nil {
		return err
	}

	defer func() {
		err1 := h.releaseStatsReader(reader)
		if err1 != nil && err == nil {
			err = err1
		}
	}()

	for _, col := range cols {
		statsCache := h.statsCache.Load().(statsCache)
		tbl, ok := statsCache.tables[col.TableID]
		if !ok {
			continue
		}
		tbl = tbl.Copy()
		c, ok := tbl.Columns[col.ColumnID]
		if !ok || c.Len() > 0 {
			statistics.HistogramNeededColumns.Delete(col)
			continue
		}
		hg, err := h.histogramFromStorage(reader, col.TableID, c.ID, &c.Info.FieldType, c.NDV, 0, c.LastUpdateVersion, c.NullCount, c.TotColSize, c.Correlation)
		if err != nil {
			return errors.Trace(err)
		}
		cms, err := h.cmSketchFromStorage(reader, col.TableID, 0, col.ColumnID)
		if err != nil {
			return errors.Trace(err)
		}
		tbl.Columns[c.ID] = &statistics.Column{
			PhysicalID: col.TableID,
			Histogram:  *hg,
			Info:       c.Info,
			CMSketch:   cms,
			Count:      int64(hg.TotalRowCount()),
			IsHandle:   c.IsHandle,
		}
		h.updateStatsCache(statsCache.update([]*statistics.Table{tbl}, nil, statsCache.version))
		statistics.HistogramNeededColumns.Delete(col)
	}
	return nil
}

// LastUpdateVersion gets the last update version.
func (h *Handle) LastUpdateVersion() uint64 {
	return h.statsCache.Load().(statsCache).version
}

// SetLastUpdateVersion sets the last update version.
func (h *Handle) SetLastUpdateVersion(version uint64) {
	statsCache := h.statsCache.Load().(statsCache)
	h.updateStatsCache(statsCache.update(nil, nil, version))
}

// FlushStats flushes the cached stats update into store.
func (h *Handle) FlushStats() {
	for len(h.ddlEventCh) > 0 {
		e := <-h.ddlEventCh
		if err := h.HandleDDLEvent(e); err != nil {
			logutil.BgLogger().Error("[stats] handle ddl event fail", zap.Error(err))
		}
	}
	if err := h.DumpStatsDeltaToKV(DumpAll); err != nil {
		logutil.BgLogger().Error("[stats] dump stats delta fail", zap.Error(err))
	}
	if err := h.DumpStatsFeedbackToKV(); err != nil {
		logutil.BgLogger().Error("[stats] dump stats feedback fail", zap.Error(err))
	}
}

func (h *Handle) cmSketchFromStorage(reader *statsReader, tblID int64, isIndex, histID int64) (_ *statistics.CMSketch, err error) {
	rows, _, err := reader.read("select cm_sketch from mysql.stats_histograms where table_id = %? and is_index = %? and hist_id = %?", tblID, isIndex, histID)
	if err != nil || len(rows) == 0 {
		return nil, err
	}
	topNRows, _, err := reader.read("select HIGH_PRIORITY value, count from mysql.stats_top_n where table_id = %? and is_index = %? and hist_id = %?", tblID, isIndex, histID)
	if err != nil {
		return nil, err
	}
	return statistics.DecodeCMSketch(rows[0].GetBytes(0), topNRows)
}

func (h *Handle) indexStatsFromStorage(reader *statsReader, row chunk.Row, table *statistics.Table, tableInfo *model.TableInfo) error {
	histID := row.GetInt64(2)
	distinct := row.GetInt64(3)
	histVer := row.GetUint64(4)
	nullCount := row.GetInt64(5)
	idx := table.Indices[histID]
	errorRate := statistics.ErrorRate{}
	flag := row.GetInt64(8)
	lastAnalyzePos := row.GetDatum(10, types.NewFieldType(mysql.TypeBlob))
	if statistics.IsAnalyzed(flag) && !reader.isHistory() {
		h.mu.rateMap.clear(table.PhysicalID, histID, true)
	} else if idx != nil {
		errorRate = idx.ErrorRate
	}
	for _, idxInfo := range tableInfo.Indices {
		if histID != idxInfo.ID {
			continue
		}
		if idx == nil || idx.LastUpdateVersion < histVer {
			hg, err := h.histogramFromStorage(reader, table.PhysicalID, histID, types.NewFieldType(mysql.TypeBlob), distinct, 1, histVer, nullCount, 0, 0)
			if err != nil {
				return errors.Trace(err)
			}
			cms, err := h.cmSketchFromStorage(reader, table.PhysicalID, 1, idxInfo.ID)
			if err != nil {
				return errors.Trace(err)
			}
			idx = &statistics.Index{Histogram: *hg, CMSketch: cms, Info: idxInfo, ErrorRate: errorRate, StatsVer: row.GetInt64(7), Flag: flag}
			lastAnalyzePos.Copy(&idx.LastAnalyzePos)
		}
		break
	}
	if idx != nil {
		table.Indices[histID] = idx
	} else {
		logutil.BgLogger().Debug("we cannot find index id in table info. It may be deleted.", zap.Int64("indexID", histID), zap.String("table", tableInfo.Name.O))
	}
	return nil
}

func (h *Handle) columnStatsFromStorage(reader *statsReader, row chunk.Row, table *statistics.Table, tableInfo *model.TableInfo, loadAll bool) error {
	histID := row.GetInt64(2)
	distinct := row.GetInt64(3)
	histVer := row.GetUint64(4)
	nullCount := row.GetInt64(5)
	totColSize := row.GetInt64(6)
	correlation := row.GetFloat64(9)
	lastAnalyzePos := row.GetDatum(10, types.NewFieldType(mysql.TypeBlob))
	col := table.Columns[histID]
	errorRate := statistics.ErrorRate{}
	flag := row.GetInt64(8)
	if statistics.IsAnalyzed(flag) && !reader.isHistory() {
		h.mu.rateMap.clear(table.PhysicalID, histID, false)
	} else if col != nil {
		errorRate = col.ErrorRate
	}
	for _, colInfo := range tableInfo.Columns {
		if histID != colInfo.ID {
			continue
		}
		isHandle := tableInfo.PKIsHandle && mysql.HasPriKeyFlag(colInfo.Flag)
		// We will not load buckets if:
		// 1. Lease > 0, and:
		// 2. this column is not handle, and:
		// 3. the column doesn't has buckets before, and:
		// 4. loadAll is false.
		notNeedLoad := h.Lease() > 0 &&
			!isHandle &&
			(col == nil || col.Len() == 0 && col.LastUpdateVersion < histVer) &&
			!loadAll
		if notNeedLoad {
			count, err := h.columnCountFromStorage(reader, table.PhysicalID, histID)
			if err != nil {
				return errors.Trace(err)
			}
			col = &statistics.Column{
				PhysicalID: table.PhysicalID,
				Histogram:  *statistics.NewHistogram(histID, distinct, nullCount, histVer, &colInfo.FieldType, 0, totColSize),
				Info:       colInfo,
				Count:      count + nullCount,
				ErrorRate:  errorRate,
				IsHandle:   tableInfo.PKIsHandle && mysql.HasPriKeyFlag(colInfo.Flag),
				Flag:       flag,
			}
			lastAnalyzePos.Copy(&col.LastAnalyzePos)
			col.Histogram.Correlation = correlation
			break
		}
		if col == nil || col.LastUpdateVersion < histVer || loadAll {
			hg, err := h.histogramFromStorage(reader, table.PhysicalID, histID, &colInfo.FieldType, distinct, 0, histVer, nullCount, totColSize, correlation)
			if err != nil {
				return errors.Trace(err)
			}
			cms, err := h.cmSketchFromStorage(reader, table.PhysicalID, 0, colInfo.ID)
			if err != nil {
				return errors.Trace(err)
			}
			col = &statistics.Column{
				PhysicalID: table.PhysicalID,
				Histogram:  *hg,
				Info:       colInfo,
				CMSketch:   cms,
				Count:      int64(hg.TotalRowCount()),
				ErrorRate:  errorRate,
				IsHandle:   tableInfo.PKIsHandle && mysql.HasPriKeyFlag(colInfo.Flag),
				Flag:       flag,
			}
			lastAnalyzePos.Copy(&col.LastAnalyzePos)
			break
		}
		if col.TotColSize != totColSize {
			newCol := *col
			newCol.TotColSize = totColSize
			col = &newCol
		}
		break
	}
	if col != nil {
		table.Columns[col.ID] = col
	} else {
		// If we didn't find a Column or Index in tableInfo, we won't load the histogram for it.
		// But don't worry, next lease the ddl will be updated, and we will load a same table for two times to
		// avoid error.
		logutil.BgLogger().Debug("we cannot find column in table info now. It may be deleted", zap.Int64("colID", histID), zap.String("table", tableInfo.Name.O))
	}
	return nil
}

// TableStatsFromStorage loads table stats info from storage.
func (h *Handle) TableStatsFromStorage(tableInfo *model.TableInfo, physicalID int64, loadAll bool, snapshot uint64) (_ *statistics.Table, err error) {
	reader, err := h.getStatsReader(snapshot)
	if err != nil {
		return nil, err
	}
	defer func() {
		err1 := h.releaseStatsReader(reader)
		if err == nil && err1 != nil {
			err = err1
		}
	}()
	table, ok := h.statsCache.Load().(statsCache).tables[physicalID]
	// If table stats is pseudo, we also need to copy it, since we will use the column stats when
	// the average error rate of it is small.
	if !ok || snapshot > 0 {
		histColl := statistics.HistColl{
			PhysicalID:     physicalID,
			HavePhysicalID: true,
			Columns:        make(map[int64]*statistics.Column, len(tableInfo.Columns)),
			Indices:        make(map[int64]*statistics.Index, len(tableInfo.Indices)),
		}
		table = &statistics.Table{
			HistColl: histColl,
		}
	} else {
		// We copy it before writing to avoid race.
		table = table.Copy()
	}
	table.Pseudo = false
	rows, _, err := reader.read("select table_id, is_index, hist_id, distinct_count, version, null_count, tot_col_size, stats_ver, flag, correlation, last_analyze_pos from mysql.stats_histograms where table_id = %?", physicalID)
	// Check deleted table.
	if err != nil || len(rows) == 0 {
		return nil, nil
	}
	for _, row := range rows {
		if row.GetInt64(1) > 0 {
			err = h.indexStatsFromStorage(reader, row, table, tableInfo)
		} else {
			err = h.columnStatsFromStorage(reader, row, table, tableInfo, loadAll)
		}
		if err != nil {
			return nil, err
		}
	}
	return table, nil
}

// SaveStatsToStorage saves the stats to storage.
func (h *Handle) SaveStatsToStorage(tableID int64, count int64, isIndex int, hg *statistics.Histogram, cms *statistics.CMSketch, isAnalyzed int64) (err error) {
	h.mu.Lock()
	defer h.mu.Unlock()
	ctx := context.TODO()
	exec := h.mu.ctx.(sqlexec.SQLExecutor)
	_, err = exec.ExecuteInternal(ctx, "begin")
	if err != nil {
		return errors.Trace(err)
	}
	defer func() {
		err = finishTransaction(context.Background(), exec, err)
	}()
	txn, err := h.mu.ctx.Txn(true)
	if err != nil {
		return errors.Trace(err)
	}

	version := txn.StartTS()
	// If the count is less than 0, then we do not want to update the modify count and count.
	if count >= 0 {
		_, err = exec.ExecuteInternal(ctx, "replace into mysql.stats_meta (version, table_id, count) values (%?, %?, %?)", version, tableID, count)
	} else {
		_, err = exec.ExecuteInternal(ctx, "update mysql.stats_meta set version = %? where table_id = %?", version, tableID)
	}
	if err != nil {
		return err
	}
	data, err := statistics.EncodeCMSketchWithoutTopN(cms)
	if err != nil {
		return err
	}
	// Delete outdated data
	if _, err = exec.ExecuteInternal(ctx, "delete from mysql.stats_top_n where table_id = %? and is_index = %? and hist_id = %?", tableID, isIndex, hg.ID); err != nil {
		return err
	}
	for _, meta := range cms.TopN() {
		_, err = exec.ExecuteInternal(ctx, "insert into mysql.stats_top_n (table_id, is_index, hist_id, value, count) values (%?, %?, %?, %?, %?)", tableID, isIndex, hg.ID, meta.Data, meta.Count)
		if err != nil {
			return err
		}
	}
	flag := 0
	if isAnalyzed == 1 {
		flag = statistics.AnalyzeFlag
	}
	if _, err = exec.ExecuteInternal(ctx, "replace into mysql.stats_histograms (table_id, is_index, hist_id, distinct_count, version, null_count, cm_sketch, tot_col_size, stats_ver, flag, correlation) values (%?, %?, %?, %?, %?, %?, %?, %?, %?, %?, %?)",
		tableID, isIndex, hg.ID, hg.NDV, version, hg.NullCount, data, hg.TotColSize, statistics.CurStatsVersion, flag, hg.Correlation); err != nil {
		return err
	}
	if _, err = exec.ExecuteInternal(ctx, "delete from mysql.stats_buckets where table_id = %? and is_index = %? and hist_id = %?", tableID, isIndex, hg.ID); err != nil {
		return err
	}
	sc := h.mu.ctx.GetSessionVars().StmtCtx
	var lastAnalyzePos []byte
	for i := range hg.Buckets {
		count := hg.Buckets[i].Count
		if i > 0 {
			count -= hg.Buckets[i-1].Count
		}
		var upperBound types.Datum
		upperBound, err = hg.GetUpper(i).ConvertTo(sc, types.NewFieldType(mysql.TypeBlob))
		if err != nil {
			return
		}
		if i == len(hg.Buckets)-1 {
			lastAnalyzePos = upperBound.GetBytes()
		}
		var lowerBound types.Datum
		lowerBound, err = hg.GetLower(i).ConvertTo(sc, types.NewFieldType(mysql.TypeBlob))
		if err != nil {
			return
		}
		if _, err = exec.ExecuteInternal(ctx, "insert into mysql.stats_buckets(table_id, is_index, hist_id, bucket_id, count, repeats, lower_bound, upper_bound) values(%?, %?, %?, %?, %?, %?, %?, %?)", tableID, isIndex, hg.ID, i, count, hg.Buckets[i].Repeat, lowerBound.GetBytes(), upperBound.GetBytes()); err != nil {
			return err
		}
	}
	if isAnalyzed == 1 && len(lastAnalyzePos) > 0 {
		if _, err = exec.ExecuteInternal(ctx, "update mysql.stats_histograms set last_analyze_pos = %? where table_id = %? and is_index = %? and hist_id = %?", lastAnalyzePos, tableID, isIndex, hg.ID); err != nil {
			return err
		}
	}
	return
}

// SaveMetaToStorage will save stats_meta to storage.
func (h *Handle) SaveMetaToStorage(tableID, count, modifyCount int64) (err error) {
	h.mu.Lock()
	defer h.mu.Unlock()
	ctx := context.TODO()
	exec := h.mu.ctx.(sqlexec.SQLExecutor)
	_, err = exec.ExecuteInternal(ctx, "begin")
	if err != nil {
		return errors.Trace(err)
	}
	defer func() {
		err = finishTransaction(ctx, exec, err)
	}()
	txn, err := h.mu.ctx.Txn(true)
	if err != nil {
		return errors.Trace(err)
	}
	version := txn.StartTS()
	_, err = exec.ExecuteInternal(ctx, "replace into mysql.stats_meta (version, table_id, count, modify_count) values (%?, %?, %?, %?)", version, tableID, count, modifyCount)
	return err
}

func (h *Handle) histogramFromStorage(reader *statsReader, tableID int64, colID int64, tp *types.FieldType, distinct int64, isIndex int, ver uint64, nullCount int64, totColSize int64, corr float64) (_ *statistics.Histogram, err error) {
	rows, fields, err := reader.read("select count, repeats, lower_bound, upper_bound from mysql.stats_buckets where table_id = %? and is_index = %? and hist_id = %? order by bucket_id", tableID, isIndex, colID)
	if err != nil {
		return nil, errors.Trace(err)
	}
	bucketSize := len(rows)
	hg := statistics.NewHistogram(colID, distinct, nullCount, ver, tp, bucketSize, totColSize)
	hg.Correlation = corr
	totalCount := int64(0)
	for i := 0; i < bucketSize; i++ {
		count := rows[i].GetInt64(0)
		repeats := rows[i].GetInt64(1)
		var upperBound, lowerBound types.Datum
		if isIndex == 1 {
			lowerBound = rows[i].GetDatum(2, &fields[2].Column.FieldType)
			upperBound = rows[i].GetDatum(3, &fields[3].Column.FieldType)
		} else {
			sc := &stmtctx.StatementContext{TimeZone: time.UTC}
			d := rows[i].GetDatum(2, &fields[2].Column.FieldType)
			// When there's new collation data, the length of bounds of histogram(the collate key) might be
			// longer than the FieldType.Flen of this column.
			// We change it to TypeBlob to bypass the length check here.
			if tp.EvalType() == types.ETString && tp.Tp != mysql.TypeEnum && tp.Tp != mysql.TypeSet {
				tp = types.NewFieldType(mysql.TypeBlob)
			}
			lowerBound, err = d.ConvertTo(sc, tp)
			if err != nil {
				return nil, errors.Trace(err)
			}
			d = rows[i].GetDatum(3, &fields[3].Column.FieldType)
			upperBound, err = d.ConvertTo(sc, tp)
			if err != nil {
				return nil, errors.Trace(err)
			}
		}
		totalCount += count
		hg.AppendBucket(&lowerBound, &upperBound, totalCount, repeats)
	}
	hg.PreCalculateScalar()
	return hg, nil
}

func (h *Handle) columnCountFromStorage(reader *statsReader, tableID, colID int64) (int64, error) {
	rows, _, err := reader.read("select sum(count) from mysql.stats_buckets where table_id = %? and is_index = %? and hist_id = %?", tableID, 0, colID)
	if err != nil {
		return 0, errors.Trace(err)
	}
	if rows[0].IsNull(0) {
		return 0, nil
	}
	return rows[0].GetMyDecimal(0).ToInt()
}

func (h *Handle) statsMetaByTableIDFromStorage(tableID int64, snapshot uint64) (version uint64, modifyCount, count int64, err error) {
	ctx := context.Background()
	var rows []chunk.Row
	if snapshot == 0 {
		rows, _, err = h.execRestrictedSQL(ctx, "SELECT version, modify_count, count from mysql.stats_meta where table_id = %? order by version", tableID)
	} else {
		rows, _, err = h.execRestrictedSQLWithSnapshot(ctx, "SELECT version, modify_count, count from mysql.stats_meta where table_id = %? order by version", snapshot, tableID)
		if err != nil {
			return 0, 0, 0, err
		}
	}
	if err != nil || len(rows) == 0 {
		return
	}
	version = rows[0].GetUint64(0)
	modifyCount = rows[0].GetInt64(1)
	count = rows[0].GetInt64(2)
	return
}

// statsReader is used for simplify code that needs to read system tables in different sqls
// but requires the same transactions.
type statsReader struct {
	ctx      sqlexec.RestrictedSQLExecutor
	snapshot uint64
}

func (sr *statsReader) read(sql string, args ...interface{}) (rows []chunk.Row, fields []*ast.ResultField, err error) {
	ctx := context.TODO()
	stmt, err := sr.ctx.ParseWithParams(ctx, sql, args...)
	if err != nil {
		return nil, nil, errors.Trace(err)
	}
	if sr.snapshot > 0 {
		return sr.ctx.ExecRestrictedStmt(ctx, stmt, sqlexec.ExecOptionWithSnapshot(sr.snapshot))
	}
	return sr.ctx.ExecRestrictedStmt(ctx, stmt)
}

func (sr *statsReader) isHistory() bool {
	return sr.snapshot > 0
}

func (h *Handle) getStatsReader(snapshot uint64) (reader *statsReader, err error) {
	failpoint.Inject("mockGetStatsReaderFail", func(val failpoint.Value) {
		if val.(bool) {
			failpoint.Return(nil, errors.New("gofail genStatsReader error"))
		}
	})
	if snapshot > 0 {
		return &statsReader{ctx: h.mu.ctx.(sqlexec.RestrictedSQLExecutor), snapshot: snapshot}, nil
	}
	h.mu.Lock()
	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("getStatsReader panic %v", r)
		}
		if err != nil {
			h.mu.Unlock()
		}
	}()
	failpoint.Inject("mockGetStatsReaderPanic", nil)
	_, err = h.mu.ctx.(sqlexec.SQLExecutor).ExecuteInternal(context.TODO(), "begin")
	if err != nil {
		return nil, err
	}
	return &statsReader{ctx: h.mu.ctx.(sqlexec.RestrictedSQLExecutor)}, nil
}

func (h *Handle) releaseStatsReader(reader *statsReader) error {
	if reader.snapshot > 0 {
		return nil
	}
	_, err := h.mu.ctx.(sqlexec.SQLExecutor).ExecuteInternal(context.TODO(), "commit")
	h.mu.Unlock()
	return err
}
