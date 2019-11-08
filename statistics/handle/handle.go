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

	"github.com/pingcap/errors"
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

// StatsCache caches the tables in memory for Handle.
type StatsCache map[int64]*statistics.Table

// Handle can update stats info periodically.
type Handle struct {
	mu struct {
		sync.Mutex
		ctx sessionctx.Context
		// lastVersion is the latest update version before last lease.
		lastVersion uint64
		// rateMap contains the error rate delta from feedback.
		rateMap errorRateDeltaMap
		// pid2tid is the map from partition ID to table ID.
		pid2tid map[int64]int64
		// schemaVersion is the version of information schema when `pid2tid` is built.
		schemaVersion int64
	}

	restrictedExec sqlexec.RestrictedSQLExecutor

	StatsCache atomic.Value
	// ddlEventCh is a channel to notify a ddl operation has happened.
	// It is sent only by owner or the drop stats executor, and read by stats handle.
	ddlEventCh chan *util.Event
	// listHead contains all the stats collector required by session.
	listHead *SessionStatsCollector
	// globalMap contains all the delta map from collectors when we dump them to KV.
	globalMap tableDeltaMap
	// feedback is used to store query feedback info.
	feedback []*statistics.QueryFeedback

	lease atomic2.Duration
}

// Clear the StatsCache, only for test.
func (h *Handle) Clear() {
	h.mu.Lock()
	h.StatsCache.Store(StatsCache{})
	h.mu.lastVersion = 0
	for len(h.ddlEventCh) > 0 {
		<-h.ddlEventCh
	}
	h.feedback = h.feedback[:0]
	h.mu.ctx.GetSessionVars().InitChunkSize = 1
	h.mu.ctx.GetSessionVars().MaxChunkSize = 1
	h.mu.ctx.GetSessionVars().EnableChunkRPC = false
	h.mu.ctx.GetSessionVars().ProjectionConcurrency = 0
	h.listHead = &SessionStatsCollector{mapper: make(tableDeltaMap), rateMap: make(errorRateDeltaMap)}
	h.globalMap = make(tableDeltaMap)
	h.mu.rateMap = make(errorRateDeltaMap)
	h.mu.Unlock()
}

// MaxQueryFeedbackCount is the max number of feedback that cache in memory.
var MaxQueryFeedbackCount = atomic2.NewInt64(1 << 10)

// NewHandle creates a Handle for update stats.
func NewHandle(ctx sessionctx.Context, lease time.Duration) *Handle {
	handle := &Handle{
		ddlEventCh: make(chan *util.Event, 100),
		listHead:   &SessionStatsCollector{mapper: make(tableDeltaMap), rateMap: make(errorRateDeltaMap)},
		globalMap:  make(tableDeltaMap),
		feedback:   make([]*statistics.QueryFeedback, 0, MaxQueryFeedbackCount.Load()),
	}
	handle.lease.Store(lease)
	// It is safe to use it concurrently because the exec won't touch the ctx.
	if exec, ok := ctx.(sqlexec.RestrictedSQLExecutor); ok {
		handle.restrictedExec = exec
	}
	handle.mu.ctx = ctx
	handle.mu.rateMap = make(errorRateDeltaMap)
	handle.StatsCache.Store(StatsCache{})
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

// GetQueryFeedback gets the query feedback. It is only use in test.
func (h *Handle) GetQueryFeedback() []*statistics.QueryFeedback {
	defer func() {
		h.feedback = h.feedback[:0]
	}()
	return h.feedback
}

// DurationToTS converts duration to timestamp.
func DurationToTS(d time.Duration) uint64 {
	return oracle.ComposeTS(d.Nanoseconds()/int64(time.Millisecond), 0)
}

// Update reads stats meta from store and updates the stats map.
func (h *Handle) Update(is infoschema.InfoSchema) error {
	lastVersion := h.LastUpdateVersion()
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
			logutil.BgLogger().Debug("error occurred when read table stats", zap.String("table", tableInfo.Name.O), zap.Error(err))
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
	h.mu.Lock()
	h.mu.lastVersion = lastVersion
	h.UpdateTableStats(tables, deletedTableIDs)
	h.mu.Unlock()
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
	tbl, ok := h.StatsCache.Load().(StatsCache)[pid]
	if !ok {
		tbl = statistics.PseudoTable(tblInfo)
		tbl.PhysicalID = pid
		h.UpdateTableStats([]*statistics.Table{tbl}, nil)
		return tbl
	}
	return tbl
}

func (h *Handle) copyFromOldCache() StatsCache {
	newCache := StatsCache{}
	oldCache := h.StatsCache.Load().(StatsCache)
	for k, v := range oldCache {
		newCache[k] = v
	}
	return newCache
}

// UpdateTableStats updates the statistics table cache using copy on write.
func (h *Handle) UpdateTableStats(tables []*statistics.Table, deletedIDs []int64) {
	newCache := h.copyFromOldCache()
	for _, tbl := range tables {
		id := tbl.PhysicalID
		newCache[id] = tbl
	}
	for _, id := range deletedIDs {
		delete(newCache, id)
	}
	h.StatsCache.Store(newCache)
}

// LoadNeededHistograms will load histograms for those needed columns.
func (h *Handle) LoadNeededHistograms() error {
	cols := statistics.HistogramNeededColumns.AllCols()
	for _, col := range cols {
		tbl, ok := h.StatsCache.Load().(StatsCache)[col.TableID]
		if !ok {
			continue
		}
		tbl = tbl.Copy()
		c, ok := tbl.Columns[col.ColumnID]
		if !ok || c.Len() > 0 {
			statistics.HistogramNeededColumns.Delete(col)
			continue
		}
		hg, err := h.histogramFromStorage(col.TableID, c.ID, &c.Info.FieldType, c.NDV, 0, c.LastUpdateVersion, c.NullCount, c.TotColSize, c.Correlation, nil)
		if err != nil {
			return errors.Trace(err)
		}
		cms, err := h.cmSketchFromStorage(col.TableID, 0, col.ColumnID, nil)
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
		h.UpdateTableStats([]*statistics.Table{tbl}, nil)
		statistics.HistogramNeededColumns.Delete(col)
	}
	return nil
}

// LastUpdateVersion gets the last update version.
func (h *Handle) LastUpdateVersion() uint64 {
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.mu.lastVersion
}

// SetLastUpdateVersion sets the last update version.
func (h *Handle) SetLastUpdateVersion(version uint64) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.mu.lastVersion = version
}

// FlushStats flushes the cached stats update into store.
func (h *Handle) FlushStats() {
	for len(h.ddlEventCh) > 0 {
		e := <-h.ddlEventCh
		if err := h.HandleDDLEvent(e); err != nil {
			logutil.BgLogger().Debug("[stats] handle ddl event fail", zap.Error(err))
		}
	}
	if err := h.DumpStatsDeltaToKV(DumpAll); err != nil {
		logutil.BgLogger().Debug("[stats] dump stats delta fail", zap.Error(err))
	}
	if err := h.DumpStatsFeedbackToKV(); err != nil {
		logutil.BgLogger().Debug("[stats] dump stats feedback fail", zap.Error(err))
	}
}

func (h *Handle) cmSketchFromStorage(tblID int64, isIndex, histID int64, historyStatsExec sqlexec.RestrictedSQLExecutor) (_ *statistics.CMSketch, err error) {
	selSQL := fmt.Sprintf("select cm_sketch from mysql.stats_histograms where table_id = %d and is_index = %d and hist_id = %d", tblID, isIndex, histID)
	var rows []chunk.Row
	if historyStatsExec != nil {
		rows, _, err = historyStatsExec.ExecRestrictedSQLWithSnapshot(selSQL)
	} else {
		rows, _, err = h.restrictedExec.ExecRestrictedSQL(selSQL)
	}
	if err != nil || len(rows) == 0 {
		return nil, err
	}
	return statistics.LoadCMSketchWithTopN(h.restrictedExec, tblID, isIndex, histID, rows[0].GetBytes(0))
}

func (h *Handle) indexStatsFromStorage(row chunk.Row, table *statistics.Table, tableInfo *model.TableInfo, historyStatsExec sqlexec.RestrictedSQLExecutor) error {
	histID := row.GetInt64(2)
	distinct := row.GetInt64(3)
	histVer := row.GetUint64(4)
	nullCount := row.GetInt64(5)
	idx := table.Indices[histID]
	errorRate := statistics.ErrorRate{}
	flag := row.GetInt64(8)
	lastAnalyzePos := row.GetDatum(10, types.NewFieldType(mysql.TypeBlob))
	if statistics.IsAnalyzed(flag) {
		h.mu.Lock()
		h.mu.rateMap.clear(table.PhysicalID, histID, true)
		h.mu.Unlock()
	} else if idx != nil {
		errorRate = idx.ErrorRate
	}
	for _, idxInfo := range tableInfo.Indices {
		if histID != idxInfo.ID {
			continue
		}
		if idx == nil || idx.LastUpdateVersion < histVer {
			hg, err := h.histogramFromStorage(table.PhysicalID, histID, types.NewFieldType(mysql.TypeBlob), distinct, 1, histVer, nullCount, 0, 0, historyStatsExec)
			if err != nil {
				return errors.Trace(err)
			}
			cms, err := h.cmSketchFromStorage(table.PhysicalID, 1, idxInfo.ID, historyStatsExec)
			if err != nil {
				return errors.Trace(err)
			}
			idx = &statistics.Index{Histogram: *hg, CMSketch: cms, Info: idxInfo, ErrorRate: errorRate, StatsVer: row.GetInt64(7), Flag: flag, LastAnalyzePos: *lastAnalyzePos.Copy()}
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

func (h *Handle) columnStatsFromStorage(row chunk.Row, table *statistics.Table, tableInfo *model.TableInfo, loadAll bool, historyStatsExec sqlexec.RestrictedSQLExecutor) error {
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
	if statistics.IsAnalyzed(flag) {
		h.mu.Lock()
		h.mu.rateMap.clear(table.PhysicalID, histID, false)
		h.mu.Unlock()
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
			count, err := h.columnCountFromStorage(table.PhysicalID, histID)
			if err != nil {
				return errors.Trace(err)
			}
			col = &statistics.Column{
				PhysicalID:     table.PhysicalID,
				Histogram:      *statistics.NewHistogram(histID, distinct, nullCount, histVer, &colInfo.FieldType, 0, totColSize),
				Info:           colInfo,
				Count:          count + nullCount,
				ErrorRate:      errorRate,
				IsHandle:       tableInfo.PKIsHandle && mysql.HasPriKeyFlag(colInfo.Flag),
				Flag:           flag,
				LastAnalyzePos: *lastAnalyzePos.Copy(),
			}
			col.Histogram.Correlation = correlation
			break
		}
		if col == nil || col.LastUpdateVersion < histVer || loadAll {
			hg, err := h.histogramFromStorage(table.PhysicalID, histID, &colInfo.FieldType, distinct, 0, histVer, nullCount, totColSize, correlation, historyStatsExec)
			if err != nil {
				return errors.Trace(err)
			}
			cms, err := h.cmSketchFromStorage(table.PhysicalID, 0, colInfo.ID, historyStatsExec)
			if err != nil {
				return errors.Trace(err)
			}
			col = &statistics.Column{
				PhysicalID:     table.PhysicalID,
				Histogram:      *hg,
				Info:           colInfo,
				CMSketch:       cms,
				Count:          int64(hg.TotalRowCount()),
				ErrorRate:      errorRate,
				IsHandle:       tableInfo.PKIsHandle && mysql.HasPriKeyFlag(colInfo.Flag),
				Flag:           flag,
				LastAnalyzePos: *lastAnalyzePos.Copy(),
			}
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
	table, ok := h.StatsCache.Load().(StatsCache)[physicalID]
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
	var rows []chunk.Row
	if historyStatsExec != nil {
		rows, _, err = historyStatsExec.ExecRestrictedSQLWithSnapshot(selSQL)
	} else {
		rows, _, err = h.restrictedExec.ExecRestrictedSQL(selSQL)
	}
	// Check deleted table.
	if err != nil || len(rows) == 0 {
		return nil, nil
	}
	for _, row := range rows {
		if row.GetInt64(1) > 0 {
			err = h.indexStatsFromStorage(row, table, tableInfo, historyStatsExec)
		} else {
			err = h.columnStatsFromStorage(row, table, tableInfo, loadAll, historyStatsExec)
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

func (h *Handle) histogramFromStorage(tableID int64, colID int64, tp *types.FieldType, distinct int64, isIndex int, ver uint64, nullCount int64, totColSize int64, corr float64, historyStatsExec sqlexec.RestrictedSQLExecutor) (_ *statistics.Histogram, err error) {
	selSQL := fmt.Sprintf("select count, repeats, lower_bound, upper_bound from mysql.stats_buckets where table_id = %d and is_index = %d and hist_id = %d order by bucket_id", tableID, isIndex, colID)
	var (
		rows   []chunk.Row
		fields []*ast.ResultField
	)
	if historyStatsExec != nil {
		rows, fields, err = historyStatsExec.ExecRestrictedSQLWithSnapshot(selSQL)
	} else {
		rows, fields, err = h.restrictedExec.ExecRestrictedSQL(selSQL)
	}
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

func (h *Handle) columnCountFromStorage(tableID, colID int64) (int64, error) {
	selSQL := fmt.Sprintf("select sum(count) from mysql.stats_buckets where table_id = %d and is_index = %d and hist_id = %d", tableID, 0, colID)
	rows, _, err := h.restrictedExec.ExecRestrictedSQL(selSQL)
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
