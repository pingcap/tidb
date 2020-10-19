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
	"encoding/json"
	"fmt"
	"sync"
	"time"

	"github.com/cznic/mathutil"
	"github.com/pingcap/errors"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/parser/ast"
	"github.com/pingcap/parser/model"
	"github.com/pingcap/parser/mysql"
	"github.com/pingcap/parser/terror"
	"github.com/pingcap/tidb/ddl/util"
	"github.com/pingcap/tidb/infoschema"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/sessionctx/stmtctx"
	"github.com/pingcap/tidb/sessionctx/variable"
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
	statsCache *statsCache

	restrictedExec sqlexec.RestrictedSQLExecutor

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
}

// Clear4Test the statsCache, only for test.
func (h *Handle) Clear4Test() {
	h.mu.Lock()
	h.SetBytesLimit4Test(h.mu.ctx.GetSessionVars().MemQuotaStatistics)
	h.statsCache.Clear()
	for len(h.ddlEventCh) > 0 {
		<-h.ddlEventCh
	}
	h.feedback = statistics.NewQueryFeedbackMap()
	h.mu.ctx.GetSessionVars().InitChunkSize = 1
	h.mu.ctx.GetSessionVars().MaxChunkSize = 1
	h.mu.ctx.GetSessionVars().EnableChunkRPC = false
	h.mu.ctx.GetSessionVars().SetProjectionConcurrency(0)
	h.listHead = &SessionStatsCollector{mapper: make(tableDeltaMap), rateMap: make(errorRateDeltaMap)}
	h.globalMap = make(tableDeltaMap)
	h.mu.rateMap = make(errorRateDeltaMap)
	h.mu.Unlock()
}

// NewHandle creates a Handle for update stats.
func NewHandle(ctx sessionctx.Context, lease time.Duration) (*Handle, error) {
	handle := &Handle{
		ddlEventCh: make(chan *util.Event, 100),
		listHead:   &SessionStatsCollector{mapper: make(tableDeltaMap), rateMap: make(errorRateDeltaMap)},
		globalMap:  make(tableDeltaMap),
		feedback:   statistics.NewQueryFeedbackMap(),
	}
	handle.lease.Store(lease)
	// It is safe to use it concurrently because the exec won't touch the ctx.
	if exec, ok := ctx.(sqlexec.RestrictedSQLExecutor); ok {
		handle.restrictedExec = exec
	}
	handle.statsCache = newStatsCache(ctx.GetSessionVars().MemQuotaStatistics)
	handle.mu.ctx = ctx
	handle.mu.rateMap = make(errorRateDeltaMap)
	err := handle.RefreshVars()
	if err != nil {
		return nil, err
	}
	return handle, nil
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
	lastVersion := h.statsCache.GetVersion()
	// We need this because for two tables, the smaller version may write later than the one with larger version.
	// Consider the case that there are two tables A and B, their version and commit time is (A0, A1) and (B0, B1),
	// and A0 < B0 < B1 < A1. We will first read the stats of B, and update the lastVersion to B0, but we cannot read
	// the table stats of A0 if we read stats that greater than lastVersion which is B0.
	// We can read the stats if the diff between commit time and version is less than three lease.
	offset := DurationToTS(3 * h.Lease())
	if lastVersion >= offset {
		lastVersion = lastVersion - offset
	} else {
		lastVersion = 0
	}
	sql := fmt.Sprintf("SELECT version, table_id, modify_count, count from mysql.stats_meta where version > %d order by version", lastVersion)
	rows, _, err := h.restrictedExec.ExecRestrictedSQL(sql)
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
		tbl, err := h.tableStatsFromStorage(tableInfo, physicalID, false, nil)
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
		tables = append(tables, tbl)
	}
	h.statsCache.Update(tables, deletedTableIDs, lastVersion)
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

// GetMemConsumed returns the mem size of statscache consumed
func (h *Handle) GetMemConsumed() (size int64) {
	h.statsCache.mu.Lock()
	size = h.statsCache.memTracker.BytesConsumed()
	h.statsCache.mu.Unlock()
	return
}

// EraseTable4Test erase a table by ID and add new empty (with Meta) table.
// ONLY used for test.
func (h *Handle) EraseTable4Test(ID int64) {
	table, _ := h.statsCache.Lookup(ID)
	h.statsCache.Insert(table.CopyWithoutBucketsAndCMS())
}

// GetAllTableStatsMemUsage4Test get all the mem usage with true table.
// ONLY used for test.
func (h *Handle) GetAllTableStatsMemUsage4Test() int64 {
	data := h.statsCache.GetAll()
	allUsage := int64(0)
	for _, t := range data {
		allUsage += t.MemoryUsage()
	}
	return allUsage
}

// GetTableStats retrieves the statistics table from cache, and the cache will be updated by a goroutine.
func (h *Handle) GetTableStats(tblInfo *model.TableInfo) *statistics.Table {
	return h.GetPartitionStats(tblInfo, tblInfo.ID)
}

// GetPartitionStats retrieves the partition stats from cache.
func (h *Handle) GetPartitionStats(tblInfo *model.TableInfo, pid int64) *statistics.Table {
	tbl, ok := h.statsCache.Lookup(pid)
	if !ok {
		tbl = statistics.PseudoTable(tblInfo)
		tbl.PhysicalID = pid
		h.statsCache.Update([]*statistics.Table{tbl}, nil, h.statsCache.GetVersion())
		return tbl
	}
	return tbl
}

// SetBytesLimit4Test sets the bytes limit for this tracker. "bytesLimit <= 0" means no limit.
// Only used for test.
func (h *Handle) SetBytesLimit4Test(bytesLimit int64) {
	h.statsCache.mu.Lock()
	h.statsCache.memTracker.SetBytesLimit(bytesLimit)
	h.statsCache.memCapacity = bytesLimit
	h.statsCache.mu.Unlock()
}

// CanRuntimePrune indicates whether tbl support runtime prune for table and first partition id.
func (h *Handle) CanRuntimePrune(tid, p0Id int64) bool {
	if h == nil {
		return false
	}
	if tid == p0Id {
		return false
	}
	_, tblExists := h.statsCache.Lookup(tid)
	if tblExists {
		return true
	}
	_, partExists := h.statsCache.Lookup(p0Id)
	if !partExists {
		return true
	}
	return false

}

// LoadNeededHistograms will load histograms for those needed columns.
func (h *Handle) LoadNeededHistograms() (err error) {
	cols := statistics.HistogramNeededColumns.AllCols()
	idxs := statistics.HistogramNeededIndices.AllIdxs()
	reader, err := h.getStatsReader(nil)
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
		tbl, ok := h.statsCache.Lookup(col.TableID)
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
		h.statsCache.Update([]*statistics.Table{tbl}, nil, h.statsCache.GetVersion())
		statistics.HistogramNeededColumns.Delete(col)
	}

	for _, pidx := range idxs {
		tbl, ok := h.statsCache.Lookup(pidx.TableID)
		if !ok {
			continue
		}
		tbl = tbl.Copy()
		idx, ok := tbl.Indices[pidx.IndexID]
		if !ok || idx.Len() > 0 {
			statistics.HistogramNeededIndices.Delete(pidx)
			continue
		}
		hg, err := h.histogramFromStorage(reader, pidx.TableID, idx.ID, types.NewFieldType(mysql.TypeBlob), idx.NDV, 1, idx.LastUpdateVersion, idx.NullCount, 0, 0)
		if err != nil {
			return errors.Trace(err)
		}
		cms, err := h.cmSketchFromStorage(reader, pidx.TableID, 1, pidx.IndexID)
		if err != nil {
			return errors.Trace(err)
		}
		tbl.Indices[idx.ID] = &statistics.Index{
			Histogram:  *hg,
			CMSketch:   cms,
			PhysicalID: pidx.TableID,
			Info:       idx.Info,
			StatsVer:   idx.StatsVer,
			Flag:       idx.Flag,
		}
		h.statsCache.Update([]*statistics.Table{tbl}, nil, h.statsCache.GetVersion())
		statistics.HistogramNeededIndices.Delete(pidx)
	}
	return nil
}

// LastUpdateVersion gets the last update version.
func (h *Handle) LastUpdateVersion() uint64 {
	return h.statsCache.GetVersion()
}

// SetLastUpdateVersion sets the last update version.
func (h *Handle) SetLastUpdateVersion(version uint64) {
	h.statsCache.Update(nil, nil, version)
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
	selSQL := fmt.Sprintf("select cm_sketch from mysql.stats_histograms where table_id = %d and is_index = %d and hist_id = %d", tblID, isIndex, histID)
	rows, _, err := reader.read(selSQL)
	if err != nil || len(rows) == 0 {
		return nil, err
	}
	selSQL = fmt.Sprintf("select HIGH_PRIORITY value, count from mysql.stats_top_n where table_id = %d and is_index = %d and hist_id = %d", tblID, isIndex, histID)
	topNRows, _, err := reader.read(selSQL)
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
			idx = &statistics.Index{Histogram: *hg, CMSketch: cms, Info: idxInfo, ErrorRate: errorRate, StatsVer: row.GetInt64(7), Flag: flag, PhysicalID: table.PhysicalID}
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

// tableStatsFromStorage loads table stats info from storage.
func (h *Handle) tableStatsFromStorage(tableInfo *model.TableInfo, physicalID int64, loadAll bool, historyStatsExec sqlexec.RestrictedSQLExecutor) (_ *statistics.Table, err error) {
	reader, err := h.getStatsReader(historyStatsExec)
	if err != nil {
		return nil, err
	}
	defer func() {
		err1 := h.releaseStatsReader(reader)
		if err == nil && err1 != nil {
			err = err1
		}
	}()
	table, ok := h.statsCache.Lookup(physicalID)

	// If table stats is pseudo, we also need to copy it, since we will use the column stats when
	// the average error rate of it is small.
	if !ok || historyStatsExec != nil {
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
	selSQL := fmt.Sprintf("select table_id, is_index, hist_id, distinct_count, version, null_count, tot_col_size, stats_ver, flag, correlation, last_analyze_pos from mysql.stats_histograms where table_id = %d", physicalID)
	rows, _, err := reader.read(selSQL)
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
	return h.extendedStatsFromStorage(reader, table, physicalID, loadAll)
}

func (h *Handle) extendedStatsFromStorage(reader *statsReader, table *statistics.Table, physicalID int64, loadAll bool) (*statistics.Table, error) {
	lastVersion := uint64(0)
	if table.ExtendedStats != nil && !loadAll {
		lastVersion = table.ExtendedStats.LastUpdateVersion
	} else {
		table.ExtendedStats = statistics.NewExtendedStatsColl()
	}
	sql := fmt.Sprintf("select stats_name, db, status, type, column_ids, scalar_stats, blob_stats, version from mysql.stats_extended where table_id = %d and status in (%d, %d) and version > %d", physicalID, StatsStatusAnalyzed, StatsStatusDeleted, lastVersion)
	rows, _, err := reader.read(sql)
	if err != nil || len(rows) == 0 {
		return table, nil
	}
	for _, row := range rows {
		lastVersion = mathutil.MaxUint64(lastVersion, row.GetUint64(7))
		key := statistics.ExtendedStatsKey{
			StatsName: row.GetString(0),
			DB:        row.GetString(1),
		}
		status := uint8(row.GetInt64(2))
		if status == StatsStatusDeleted {
			delete(table.ExtendedStats.Stats, key)
		} else {
			item := &statistics.ExtendedStatsItem{
				Tp:         uint8(row.GetInt64(3)),
				ScalarVals: row.GetFloat64(5),
				StringVals: row.GetString(6),
			}
			colIDs := row.GetString(4)
			err := json.Unmarshal([]byte(colIDs), &item.ColIDs)
			if err != nil {
				logutil.BgLogger().Error("[stats] decode column IDs failed", zap.String("column_ids", colIDs), zap.Error(err))
				return nil, err
			}
			table.ExtendedStats.Stats[key] = item
		}
	}
	table.ExtendedStats.LastUpdateVersion = lastVersion
	return table, nil
}

// SaveStatsToStorage saves the stats to storage.
func (h *Handle) SaveStatsToStorage(tableID int64, count int64, isIndex int, hg *statistics.Histogram, cms *statistics.CMSketch, isAnalyzed int64) (err error) {
	h.mu.Lock()
	defer h.mu.Unlock()
	ctx := context.TODO()
	exec := h.mu.ctx.(sqlexec.SQLExecutor)
	_, err = exec.Execute(ctx, "begin")
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
	sqls := make([]string, 0, 4)
	// If the count is less than 0, then we do not want to update the modify count and count.
	if count >= 0 {
		sqls = append(sqls, fmt.Sprintf("replace into mysql.stats_meta (version, table_id, count) values (%d, %d, %d)", version, tableID, count))
	} else {
		sqls = append(sqls, fmt.Sprintf("update mysql.stats_meta set version = %d where table_id = %d", version, tableID))
	}
	data, err := statistics.EncodeCMSketchWithoutTopN(cms)
	if err != nil {
		return
	}
	// Delete outdated data
	sqls = append(sqls, fmt.Sprintf("delete from mysql.stats_top_n where table_id = %d and is_index = %d and hist_id = %d", tableID, isIndex, hg.ID))
	for _, meta := range cms.TopN() {
		sqls = append(sqls, fmt.Sprintf("insert into mysql.stats_top_n (table_id, is_index, hist_id, value, count) values (%d, %d, %d, X'%X', %d)", tableID, isIndex, hg.ID, meta.Data, meta.Count))
	}
	flag := 0
	if isAnalyzed == 1 {
		flag = statistics.AnalyzeFlag
	}
	sqls = append(sqls, fmt.Sprintf("replace into mysql.stats_histograms (table_id, is_index, hist_id, distinct_count, version, null_count, cm_sketch, tot_col_size, stats_ver, flag, correlation) values (%d, %d, %d, %d, %d, %d, X'%X', %d, %d, %d, %f)",
		tableID, isIndex, hg.ID, hg.NDV, version, hg.NullCount, data, hg.TotColSize, statistics.CurStatsVersion, flag, hg.Correlation))
	sqls = append(sqls, fmt.Sprintf("delete from mysql.stats_buckets where table_id = %d and is_index = %d and hist_id = %d", tableID, isIndex, hg.ID))
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
		sqls = append(sqls, fmt.Sprintf("insert into mysql.stats_buckets(table_id, is_index, hist_id, bucket_id, count, repeats, lower_bound, upper_bound) values(%d, %d, %d, %d, %d, %d, X'%X', X'%X')", tableID, isIndex, hg.ID, i, count, hg.Buckets[i].Repeat, lowerBound.GetBytes(), upperBound.GetBytes()))
	}
	if isAnalyzed == 1 && len(lastAnalyzePos) > 0 {
		sqls = append(sqls, fmt.Sprintf("update mysql.stats_histograms set last_analyze_pos = X'%X' where table_id = %d and is_index = %d and hist_id = %d", lastAnalyzePos, tableID, isIndex, hg.ID))
	}
	return execSQLs(context.Background(), exec, sqls)
}

// SaveMetaToStorage will save stats_meta to storage.
func (h *Handle) SaveMetaToStorage(tableID, count, modifyCount int64) (err error) {
	h.mu.Lock()
	defer h.mu.Unlock()
	ctx := context.TODO()
	exec := h.mu.ctx.(sqlexec.SQLExecutor)
	_, err = exec.Execute(ctx, "begin")
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
	var sql string
	version := txn.StartTS()
	sql = fmt.Sprintf("replace into mysql.stats_meta (version, table_id, count, modify_count) values (%d, %d, %d, %d)", version, tableID, count, modifyCount)
	_, err = exec.Execute(ctx, sql)
	return
}

func (h *Handle) histogramFromStorage(reader *statsReader, tableID int64, colID int64, tp *types.FieldType, distinct int64, isIndex int, ver uint64, nullCount int64, totColSize int64, corr float64) (_ *statistics.Histogram, err error) {
	selSQL := fmt.Sprintf("select count, repeats, lower_bound, upper_bound from mysql.stats_buckets where table_id = %d and is_index = %d and hist_id = %d order by bucket_id", tableID, isIndex, colID)
	rows, fields, err := reader.read(selSQL)
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
	selSQL := fmt.Sprintf("select sum(count) from mysql.stats_buckets where table_id = %d and is_index = %d and hist_id = %d", tableID, 0, colID)
	rows, _, err := reader.read(selSQL)
	if err != nil {
		return 0, errors.Trace(err)
	}
	if rows[0].IsNull(0) {
		return 0, nil
	}
	return rows[0].GetMyDecimal(0).ToInt()
}

func (h *Handle) statsMetaByTableIDFromStorage(tableID int64, historyStatsExec sqlexec.RestrictedSQLExecutor) (version uint64, modifyCount, count int64, err error) {
	selSQL := fmt.Sprintf("SELECT version, modify_count, count from mysql.stats_meta where table_id = %d order by version", tableID)
	var rows []chunk.Row
	if historyStatsExec == nil {
		rows, _, err = h.restrictedExec.ExecRestrictedSQL(selSQL)
	} else {
		rows, _, err = historyStatsExec.ExecRestrictedSQLWithSnapshot(selSQL)
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
	ctx     sessionctx.Context
	history sqlexec.RestrictedSQLExecutor
}

func (sr *statsReader) read(sql string) (rows []chunk.Row, fields []*ast.ResultField, err error) {
	if sr.history != nil {
		return sr.history.ExecRestrictedSQLWithSnapshot(sql)
	}
	rc, err := sr.ctx.(sqlexec.SQLExecutor).Execute(context.TODO(), sql)
	if len(rc) > 0 {
		defer terror.Call(rc[0].Close)
	}
	if err != nil {
		return nil, nil, err
	}
	for {
		req := rc[0].NewChunk()
		err := rc[0].Next(context.TODO(), req)
		if err != nil {
			return nil, nil, err
		}
		if req.NumRows() == 0 {
			break
		}
		for i := 0; i < req.NumRows(); i++ {
			rows = append(rows, req.GetRow(i))
		}
	}
	return rows, rc[0].Fields(), nil
}

func (sr *statsReader) isHistory() bool {
	return sr.history != nil
}

func (h *Handle) getStatsReader(history sqlexec.RestrictedSQLExecutor) (reader *statsReader, err error) {
	failpoint.Inject("mockGetStatsReaderFail", func(val failpoint.Value) {
		if val.(bool) {
			failpoint.Return(nil, errors.New("gofail genStatsReader error"))
		}
	})
	if history != nil {
		return &statsReader{history: history}, nil
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
	_, err = h.mu.ctx.(sqlexec.SQLExecutor).Execute(context.TODO(), "begin")
	if err != nil {
		return nil, err
	}
	return &statsReader{ctx: h.mu.ctx}, nil
}

func (h *Handle) releaseStatsReader(reader *statsReader) error {
	if reader.history != nil {
		return nil
	}
	_, err := h.mu.ctx.(sqlexec.SQLExecutor).Execute(context.TODO(), "commit")
	h.mu.Unlock()
	return err
}

const (
	// StatsStatusInited is the status for extended stats which are just registered but have not been analyzed yet.
	StatsStatusInited uint8 = iota
	// StatsStatusAnalyzed is the status for extended stats which have been collected in analyze.
	StatsStatusAnalyzed
	// StatsStatusDeleted is the status for extended stats which were dropped. These "deleted" records would be removed from storage by GCStats().
	StatsStatusDeleted
)

// InsertExtendedStats inserts a record into mysql.stats_extended and update version in mysql.stats_meta.
func (h *Handle) InsertExtendedStats(statsName, db string, colIDs []int64, tp int, tableID int64, ifNotExists bool) (err error) {
	bytes, err := json.Marshal(colIDs)
	if err != nil {
		return errors.Trace(err)
	}
	strColIDs := string(bytes)
	h.mu.Lock()
	defer h.mu.Unlock()
	ctx := context.TODO()
	exec := h.mu.ctx.(sqlexec.SQLExecutor)
	_, err = exec.Execute(ctx, "begin pessimistic")
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
	sql := fmt.Sprintf("INSERT INTO mysql.stats_extended(stats_name, db, type, table_id, column_ids, version, status) VALUES ('%s', '%s', %d, %d, '%s', %d, %d)", statsName, db, tp, tableID, strColIDs, version, StatsStatusInited)
	_, err = exec.Execute(ctx, sql)
	// Key exists, but `if not exists` is specified, so we ignore this error.
	if kv.ErrKeyExists.Equal(err) && ifNotExists {
		err = nil
	}
	return
}

// MarkExtendedStatsDeleted update the status of mysql.stats_extended to be `deleted` and the version of mysql.stats_meta.
func (h *Handle) MarkExtendedStatsDeleted(statsName, db string, tableID int64) (err error) {
	if tableID < 0 {
		sql := fmt.Sprintf("SELECT table_id FROM mysql.stats_extended WHERE stats_name = '%s' and db = '%s'", statsName, db)
		rows, _, err := h.restrictedExec.ExecRestrictedSQL(sql)
		if err != nil {
			return errors.Trace(err)
		}
		if len(rows) == 0 {
			return nil
		}
		tableID = rows[0].GetInt64(0)
	}
	h.mu.Lock()
	defer h.mu.Unlock()
	ctx := context.TODO()
	exec := h.mu.ctx.(sqlexec.SQLExecutor)
	_, err = exec.Execute(ctx, "begin pessimistic")
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
	sqls := make([]string, 2)
	sqls[0] = fmt.Sprintf("UPDATE mysql.stats_extended SET version = %d, status = %d WHERE stats_name = '%s' and db = '%s'", version, StatsStatusDeleted, statsName, db)
	sqls[1] = fmt.Sprintf("UPDATE mysql.stats_meta SET version = %d WHERE table_id = %d", version, tableID)
	return execSQLs(ctx, exec, sqls)
}

// ReloadExtendedStatistics drops the cache for extended statistics and reload data from mysql.stats_extended.
func (h *Handle) ReloadExtendedStatistics() error {
	reader, err := h.getStatsReader(nil)
	if err != nil {
		return err
	}
	allTables := h.statsCache.GetAll()
	tables := make([]*statistics.Table, 0, len(allTables))
	for _, tbl := range allTables {
		t, err := h.extendedStatsFromStorage(reader, tbl.Copy(), tbl.PhysicalID, true)

		if err != nil {
			return err
		}
		tables = append(tables, t)
	}
	err = h.releaseStatsReader(reader)
	if err != nil {
		return err
	}
	// Note that this update may fail when the statsCache.version has been modified by others.
	h.statsCache.Update(tables, nil, h.statsCache.GetVersion())
	return nil
}

// BuildExtendedStats build extended stats for column groups if needed based on the column samples.
func (h *Handle) BuildExtendedStats(tableID int64, cols []*model.ColumnInfo, collectors []*statistics.SampleCollector) (*statistics.ExtendedStatsColl, error) {
	sql := fmt.Sprintf("SELECT stats_name, db, type, column_ids FROM mysql.stats_extended WHERE table_id = %d and status in (%d, %d)", tableID, StatsStatusAnalyzed, StatsStatusInited)
	rows, _, err := h.restrictedExec.ExecRestrictedSQL(sql)
	if err != nil {
		return nil, errors.Trace(err)
	}
	if len(rows) == 0 {
		return nil, nil
	}
	statsColl := statistics.NewExtendedStatsColl()
	for _, row := range rows {
		key := statistics.ExtendedStatsKey{
			StatsName: row.GetString(0),
			DB:        row.GetString(1),
		}
		item := &statistics.ExtendedStatsItem{Tp: uint8(row.GetInt64(2))}
		colIDs := row.GetString(3)
		err := json.Unmarshal([]byte(colIDs), &item.ColIDs)
		if err != nil {
			logutil.BgLogger().Error("invalid column_ids in mysql.stats_extended, skip collecting extended stats for this row", zap.String("column_ids", colIDs), zap.Error(err))
			continue
		}
		item = h.fillExtendedStatsItemVals(item, cols, collectors)
		if item != nil {
			statsColl.Stats[key] = item
		}
	}
	if len(statsColl.Stats) == 0 {
		return nil, nil
	}
	return statsColl, nil
}

func (h *Handle) fillExtendedStatsItemVals(item *statistics.ExtendedStatsItem, cols []*model.ColumnInfo, collectors []*statistics.SampleCollector) *statistics.ExtendedStatsItem {
	switch item.Tp {
	case ast.StatsTypeCardinality, ast.StatsTypeDependency:
		return nil
	case ast.StatsTypeCorrelation:
		return h.fillExtStatsCorrVals(item, cols, collectors)
	}
	return nil
}

func (h *Handle) fillExtStatsCorrVals(item *statistics.ExtendedStatsItem, cols []*model.ColumnInfo, collectors []*statistics.SampleCollector) *statistics.ExtendedStatsItem {
	colOffsets := make([]int, 0, 2)
	for _, id := range item.ColIDs {
		for i, col := range cols {
			if col.ID == id {
				colOffsets = append(colOffsets, i)
				break
			}
		}
	}
	if len(colOffsets) != 2 {
		return nil
	}
	// samplesX and samplesY are in order of handle, i.e, their SampleItem.Ordinals are in order.
	samplesX := collectors[colOffsets[0]].Samples
	// We would modify Ordinal of samplesY, so we make a deep copy.
	samplesY := statistics.CopySampleItems(collectors[colOffsets[1]].Samples)
	sampleNum := len(samplesX)
	if sampleNum == 1 {
		item.ScalarVals = float64(1)
		return item
	}
	h.mu.Lock()
	sc := h.mu.ctx.GetSessionVars().StmtCtx
	h.mu.Unlock()
	var err error
	samplesX, err = statistics.SortSampleItems(sc, samplesX)
	if err != nil {
		return nil
	}
	samplesYInXOrder := make([]*statistics.SampleItem, sampleNum)
	for i, itemX := range samplesX {
		itemY := samplesY[itemX.Ordinal]
		itemY.Ordinal = i
		samplesYInXOrder[i] = itemY
	}
	samplesYInYOrder, err := statistics.SortSampleItems(sc, samplesYInXOrder)
	if err != nil {
		return nil
	}
	var corrXYSum float64
	for i := 1; i < sampleNum; i++ {
		corrXYSum += float64(i) * float64(samplesYInYOrder[i].Ordinal)
	}
	// X means the ordinal of the item in original sequence, Y means the oridnal of the item in the
	// sorted sequence, we know that X and Y value sets are both:
	// 0, 1, ..., sampleNum-1
	// we can simply compute sum(X) = sum(Y) =
	//    (sampleNum-1)*sampleNum / 2
	// and sum(X^2) = sum(Y^2) =
	//    (sampleNum-1)*sampleNum*(2*sampleNum-1) / 6
	// We use "Pearson correlation coefficient" to compute the order correlation of columns,
	// the formula is based on https://en.wikipedia.org/wiki/Pearson_correlation_coefficient.
	// Note that (itemsCount*corrX2Sum - corrXSum*corrXSum) would never be zero when sampleNum is larger than 1.
	itemsCount := float64(sampleNum)
	corrXSum := (itemsCount - 1) * itemsCount / 2.0
	corrX2Sum := (itemsCount - 1) * itemsCount * (2*itemsCount - 1) / 6.0
	item.ScalarVals = (itemsCount*corrXYSum - corrXSum*corrXSum) / (itemsCount*corrX2Sum - corrXSum*corrXSum)
	return item
}

// SaveExtendedStatsToStorage writes extended stats of a table into mysql.stats_extended.
func (h *Handle) SaveExtendedStatsToStorage(tableID int64, extStats *statistics.ExtendedStatsColl, isLoad bool) (err error) {
	if extStats == nil || len(extStats.Stats) == 0 {
		return nil
	}
	h.mu.Lock()
	defer h.mu.Unlock()
	ctx := context.TODO()
	exec := h.mu.ctx.(sqlexec.SQLExecutor)
	_, err = exec.Execute(ctx, "begin pessimistic")
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
	sqls := make([]string, 0, 1+len(extStats.Stats))
	for key, item := range extStats.Stats {
		bytes, err := json.Marshal(item.ColIDs)
		if err != nil {
			return errors.Trace(err)
		}
		strColIDs := string(bytes)
		switch item.Tp {
		case ast.StatsTypeCardinality, ast.StatsTypeCorrelation:
			// If isLoad is true, it's INSERT; otherwise, it's UPDATE.
			sqls = append(sqls, fmt.Sprintf("replace into mysql.stats_extended values ('%s', '%s', %d, %d, '%s', %f, null, %d, %d)", key.StatsName, key.DB, item.Tp, tableID, strColIDs, item.ScalarVals, version, StatsStatusAnalyzed))
		case ast.StatsTypeDependency:
			sqls = append(sqls, fmt.Sprintf("replace into mysql.stats_extended values ('%s', '%s', %d, %d, '%s', null, '%s', %d, %d)", key.StatsName, key.DB, item.Tp, tableID, strColIDs, item.StringVals, version, StatsStatusAnalyzed))
		}
	}
	if !isLoad {
		sqls = append(sqls, fmt.Sprintf("UPDATE mysql.stats_meta SET version = %d WHERE table_id = %d", version, tableID))
	}
	return execSQLs(ctx, exec, sqls)
}

// CurrentPruneMode indicates whether tbl support runtime prune for table and first partition id.
func (h *Handle) CurrentPruneMode() variable.PartitionPruneMode {
	h.mu.Lock()
	defer h.mu.Unlock()
	return variable.PartitionPruneMode(h.mu.ctx.GetSessionVars().PartitionPruneMode.Load())
}

// RefreshVars uses to pull PartitionPruneMethod vars from kv storage.
func (h *Handle) RefreshVars() error {
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.mu.ctx.RefreshVars(context.Background())
}
