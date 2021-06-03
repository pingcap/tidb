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
	"runtime/pprof"
	"sort"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/cznic/mathutil"
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
	"github.com/pingcap/tidb/sessionctx/variable"
	"github.com/pingcap/tidb/statistics"
	"github.com/pingcap/tidb/store/tikv/oracle"
	"github.com/pingcap/tidb/table"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util/chunk"
	"github.com/pingcap/tidb/util/logutil"
	"github.com/pingcap/tidb/util/memory"
	"github.com/pingcap/tidb/util/sqlexec"
	atomic2 "go.uber.org/atomic"
	"go.uber.org/zap"
)

const (
	// TiDBGlobalStats represents the global-stats for a partitioned table.
	TiDBGlobalStats = "global"
)

// statsCache caches the tables in memory for Handle.
type statsCache struct {
	tables map[int64]*statistics.Table
	// version is the latest version of cache. It is bumped when new records of `mysql.stats_meta` are loaded into cache.
	version uint64
	// minorVersion is to differentiate the cache when the version is unchanged while the cache contents are
	// modified indeed. This can happen when we load extra column histograms into cache, or when we modify the cache with
	// statistics feedbacks, etc. We cannot bump the version then because no new changes of `mysql.stats_meta` are loaded,
	// while the override of statsCache is in a copy-on-write way, to make sure the statsCache is unchanged by others during the
	// the interval of 'copy' and 'write', every 'write' should bump / check this minorVersion if the version keeps
	// unchanged.
	// This bump / check logic is encapsulated in `statsCache.update` and `updateStatsCache`, callers don't need to care
	// about this minorVersion actually.
	minorVersion uint64
	memUsage     int64
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
		memTracker *memory.Tracker
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

	// idxUsageListHead contains all the index usage collectors required by session.
	idxUsageListHead *SessionIndexUsageCollector
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
		if variable.TopSQLEnabled() {
			//  Restore the goroutine label by using the original ctx after execution is finished.
			defer pprof.SetGoroutineLabels(ctx)
		}
		stmt, err := exec.ParseWithParams(ctx, sql, params...)
		if err != nil {
			return nil, nil, errors.Trace(err)
		}
		return exec.ExecRestrictedStmt(ctx, stmt)
	})
}

func (h *Handle) execRestrictedSQLWithStatsVer(ctx context.Context, statsVer int, sql string, params ...interface{}) ([]chunk.Row, []*ast.ResultField, error) {
	return h.withRestrictedSQLExecutor(ctx, func(ctx context.Context, exec sqlexec.RestrictedSQLExecutor) ([]chunk.Row, []*ast.ResultField, error) {
		stmt, err := exec.ParseWithParams(ctx, sql, params...)
		// TODO: An ugly way to set @@tidb_partition_prune_mode. Need to be improved.
		if _, ok := stmt.(*ast.AnalyzeTableStmt); ok {
			pruneMode := h.CurrentPruneMode()
			if session, ok := exec.(sessionctx.Context); ok {
				session.GetSessionVars().PartitionPruneMode.Store(string(pruneMode))
			}
		}
		if err != nil {
			return nil, nil, errors.Trace(err)
		}
		return exec.ExecRestrictedStmt(ctx, stmt, execOptionForAnalyze[statsVer])
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
	h.statsCache.Lock()
	h.statsCache.Store(statsCache{tables: make(map[int64]*statistics.Table)})
	h.statsCache.memTracker = memory.NewTracker(memory.LabelForStatsCache, -1)
	h.statsCache.Unlock()
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

type sessionPool interface {
	Get() (pools.Resource, error)
	Put(pools.Resource)
}

// NewHandle creates a Handle for update stats.
func NewHandle(ctx sessionctx.Context, lease time.Duration, pool sessionPool) (*Handle, error) {
	handle := &Handle{
		ddlEventCh:       make(chan *util.Event, 100),
		listHead:         &SessionStatsCollector{mapper: make(tableDeltaMap), rateMap: make(errorRateDeltaMap)},
		globalMap:        make(tableDeltaMap),
		feedback:         statistics.NewQueryFeedbackMap(),
		idxUsageListHead: &SessionIndexUsageCollector{mapper: make(indexUsageMap)},
		pool:             pool,
	}
	handle.lease.Store(lease)
	handle.pool = pool
	handle.statsCache.memTracker = memory.NewTracker(memory.LabelForStatsCache, -1)
	handle.mu.ctx = ctx
	handle.mu.rateMap = make(errorRateDeltaMap)
	handle.statsCache.Store(statsCache{tables: make(map[int64]*statistics.Table)})
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

// UpdateSessionVar updates the necessary session variables for the stats reader.
func (h *Handle) UpdateSessionVar() error {
	h.mu.Lock()
	defer h.mu.Unlock()
	verInString, err := h.mu.ctx.GetSessionVars().GlobalVarsAccessor.GetGlobalSysVar(variable.TiDBAnalyzeVersion)
	if err != nil {
		return err
	}
	ver, err := strconv.ParseInt(verInString, 10, 64)
	if err != nil {
		return err
	}
	h.mu.ctx.GetSessionVars().AnalyzeVersion = int(ver)
	return err
}

// GlobalStats is used to store the statistics contained in the global-level stats
// which is generated by the merge of partition-level stats.
// It will both store the column stats and index stats.
// In the column statistics, the variable `num` is equal to the number of columns in the partition table.
// In the index statistics, the variable `num` is always equal to one.
type GlobalStats struct {
	Num   int
	Count int64
	Hg    []*statistics.Histogram
	Cms   []*statistics.CMSketch
	TopN  []*statistics.TopN
	Fms   []*statistics.FMSketch
}

// MergePartitionStats2GlobalStatsByTableID merge the partition-level stats to global-level stats based on the tableID.
func (h *Handle) MergePartitionStats2GlobalStatsByTableID(sc sessionctx.Context, opts map[ast.AnalyzeOptionType]uint64, is infoschema.InfoSchema, physicalID int64, isIndex int, idxID int64) (globalStats *GlobalStats, err error) {
	// get the partition table IDs
	h.mu.Lock()
	globalTable, ok := h.getTableByPhysicalID(is, physicalID)
	h.mu.Unlock()
	if !ok {
		err = errors.Errorf("unknown physical ID %d in stats meta table, maybe it has been dropped", physicalID)
		return
	}
	globalTableInfo := globalTable.Meta()
	return h.mergePartitionStats2GlobalStats(sc, opts, is, globalTableInfo, isIndex, idxID)
}

// MergePartitionStats2GlobalStatsByTableID merge the partition-level stats to global-level stats based on the tableInfo.
func (h *Handle) mergePartitionStats2GlobalStats(sc sessionctx.Context, opts map[ast.AnalyzeOptionType]uint64, is infoschema.InfoSchema, globalTableInfo *model.TableInfo, isIndex int, idxID int64) (globalStats *GlobalStats, err error) {
	partitionNum := len(globalTableInfo.Partition.Definitions)
	partitionIDs := make([]int64, 0, partitionNum)
	for i := 0; i < partitionNum; i++ {
		partitionIDs = append(partitionIDs, globalTableInfo.Partition.Definitions[i].ID)
	}

	// initialized the globalStats
	globalStats = new(GlobalStats)
	if isIndex == 0 {
		globalStats.Num = len(globalTableInfo.Columns)
	} else {
		globalStats.Num = 1
	}
	globalStats.Count = 0
	globalStats.Hg = make([]*statistics.Histogram, globalStats.Num)
	globalStats.Cms = make([]*statistics.CMSketch, globalStats.Num)
	globalStats.TopN = make([]*statistics.TopN, globalStats.Num)
	globalStats.Fms = make([]*statistics.FMSketch, globalStats.Num)

	// The first dimension of slice is means the number of column or index stats in the globalStats.
	// The second dimension of slice is means the number of partition tables.
	// Because all topN and histograms need to be collected before they can be merged.
	// So we should store all of the partition-level stats first, and merge them together.
	allHg := make([][]*statistics.Histogram, globalStats.Num)
	allCms := make([][]*statistics.CMSketch, globalStats.Num)
	allTopN := make([][]*statistics.TopN, globalStats.Num)
	allFms := make([][]*statistics.FMSketch, globalStats.Num)
	for i := 0; i < globalStats.Num; i++ {
		allHg[i] = make([]*statistics.Histogram, 0, partitionNum)
		allCms[i] = make([]*statistics.CMSketch, 0, partitionNum)
		allTopN[i] = make([]*statistics.TopN, 0, partitionNum)
		allFms[i] = make([]*statistics.FMSketch, 0, partitionNum)
	}

	for _, partitionID := range partitionIDs {
		h.mu.Lock()
		partitionTable, ok := h.getTableByPhysicalID(is, partitionID)
		h.mu.Unlock()
		if !ok {
			err = errors.Errorf("unknown physical ID %d in stats meta table, maybe it has been dropped", partitionID)
			return
		}
		tableInfo := partitionTable.Meta()
		var partitionStats *statistics.Table
		partitionStats, err = h.TableStatsFromStorage(tableInfo, partitionID, true, 0)
		if err != nil {
			return
		}
		// if the err == nil && partitionStats == nil, it means we lack the partition-level stats which the physicalID is equal to partitionID.
		if partitionStats == nil {
			var errMsg string
			if isIndex == 0 {
				errMsg = fmt.Sprintf("`%s`", tableInfo.Name.L)
			} else {
				indexName := ""
				for _, idx := range tableInfo.Indices {
					if idx.ID == idxID {
						indexName = idx.Name.L
					}
				}
				errMsg = fmt.Sprintf("`%s` index: `%s`", tableInfo.Name.L, indexName)
			}
			err = types.ErrPartitionStatsMissing.GenWithStackByArgs(errMsg)
			return
		}
		for i := 0; i < globalStats.Num; i++ {
			ID := tableInfo.Columns[i].ID
			if isIndex != 0 {
				// If the statistics is the index stats, we should use the index ID to replace the column ID.
				ID = idxID
			}
			count, hg, cms, topN, fms := partitionStats.GetStatsInfo(ID, isIndex == 1)
			if i == 0 {
				// In a partition, we will only update globalStats.Count once
				globalStats.Count += count
			}
			allHg[i] = append(allHg[i], hg)
			allCms[i] = append(allCms[i], cms)
			allTopN[i] = append(allTopN[i], topN)
			allFms[i] = append(allFms[i], fms)
		}
	}

	// After collect all of the statistics from the partition-level stats,
	// we should merge them together.
	for i := 0; i < globalStats.Num; i++ {
		// Merge CMSketch
		globalStats.Cms[i] = allCms[i][0].Copy()
		for j := 1; j < partitionNum; j++ {
			err = globalStats.Cms[i].MergeCMSketch(allCms[i][j])
			if err != nil {
				return
			}
		}

		// Merge topN. We need to merge TopN before merging the histogram.
		// Because after merging TopN, some numbers will be left.
		// These remaining topN numbers will be used as a separate bucket for later histogram merging.
		var popedTopN []statistics.TopNMeta
		globalStats.TopN[i], popedTopN, allHg[i], err = statistics.MergePartTopN2GlobalTopN(sc.GetSessionVars().StmtCtx, sc.GetSessionVars().AnalyzeVersion, allTopN[i], uint32(opts[ast.AnalyzeOptNumTopN]), allHg[i], isIndex == 1)
		if err != nil {
			return
		}

		// Merge histogram
		globalStats.Hg[i], err = statistics.MergePartitionHist2GlobalHist(sc.GetSessionVars().StmtCtx, allHg[i], popedTopN, int64(opts[ast.AnalyzeOptNumBuckets]), isIndex == 1)
		if err != nil {
			return
		}

		// NOTICE: after merging bucket NDVs have the trend to be underestimated, so for safe we don't use them.
		for j := range globalStats.Hg[i].Buckets {
			globalStats.Hg[i].Buckets[j].NDV = 0
		}

		// Update NDV of global-level stats
		globalStats.Fms[i] = allFms[i][0].Copy()
		for j := 1; j < partitionNum; j++ {
			globalStats.Fms[i].MergeFMSketch(allFms[i][j])
		}

		// update the NDV
		globalStatsNDV := globalStats.Fms[i].NDV()
		if globalStatsNDV > globalStats.Count {
			globalStatsNDV = globalStats.Count
		}
		globalStats.Hg[i].NDV = globalStatsNDV
	}
	return
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
	size = h.statsCache.memTracker.BytesConsumed()
	return
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

// updateStatsCache overrides the global statsCache with a new one, it may fail
// if the global statsCache has been modified by others already.
// Callers should add retry loop if necessary.
func (h *Handle) updateStatsCache(newCache statsCache) (updated bool) {
	h.statsCache.Lock()
	oldCache := h.statsCache.Load().(statsCache)
	if oldCache.version < newCache.version || (oldCache.version == newCache.version && oldCache.minorVersion < newCache.minorVersion) {
		h.statsCache.memTracker.Consume(newCache.memUsage - oldCache.memUsage)
		h.statsCache.Store(newCache)
		updated = true
	}
	h.statsCache.Unlock()
	return
}

func (sc statsCache) copy() statsCache {
	newCache := statsCache{tables: make(map[int64]*statistics.Table, len(sc.tables)),
		version:      sc.version,
		minorVersion: sc.minorVersion,
		memUsage:     sc.memUsage,
	}
	for k, v := range sc.tables {
		newCache.tables[k] = v
	}
	return newCache
}

//initMemoryUsage calc total memory usage of statsCache and set statsCache.memUsage
//should be called after the tables and their stats are initilazed
func (sc statsCache) initMemoryUsage() {
	sum := int64(0)
	for _, tb := range sc.tables {
		sum += tb.MemoryUsage()
	}
	sc.memUsage = sum
}

// update updates the statistics table cache using copy on write.
func (sc statsCache) update(tables []*statistics.Table, deletedIDs []int64, newVersion uint64) statsCache {
	newCache := sc.copy()
	if newVersion == newCache.version {
		newCache.minorVersion += uint64(1)
	} else {
		newCache.version = newVersion
		newCache.minorVersion = uint64(0)
	}
	for _, tbl := range tables {
		id := tbl.PhysicalID
		if ptbl, ok := newCache.tables[id]; ok {
			newCache.memUsage -= ptbl.MemoryUsage()
		}
		newCache.tables[id] = tbl
		newCache.memUsage += tbl.MemoryUsage()
	}
	for _, id := range deletedIDs {
		if ptbl, ok := newCache.tables[id]; ok {
			newCache.memUsage -= ptbl.MemoryUsage()
		}
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
		oldCache := h.statsCache.Load().(statsCache)
		tbl, ok := oldCache.tables[col.TableID]
		if !ok {
			continue
		}
		c, ok := tbl.Columns[col.ColumnID]
		if !ok || c.Len() > 0 {
			statistics.HistogramNeededColumns.Delete(col)
			continue
		}
		hg, err := h.histogramFromStorage(reader, col.TableID, c.ID, &c.Info.FieldType, c.Histogram.NDV, 0, c.LastUpdateVersion, c.NullCount, c.TotColSize, c.Correlation)
		if err != nil {
			return errors.Trace(err)
		}
		cms, topN, err := h.cmSketchAndTopNFromStorage(reader, col.TableID, 0, col.ColumnID)
		if err != nil {
			return errors.Trace(err)
		}
		fms, err := h.fmSketchFromStorage(reader, col.TableID, 0, col.ColumnID)
		if err != nil {
			return errors.Trace(err)
		}
		rows, _, err := reader.read("select stats_ver from mysql.stats_histograms where is_index = 0 and table_id = %? and hist_id = %?", col.TableID, col.ColumnID)
		if err != nil {
			return errors.Trace(err)
		}
		if len(rows) == 0 {
			logutil.BgLogger().Error("fail to get stats version for this histogram", zap.Int64("table_id", col.TableID), zap.Int64("hist_id", col.ColumnID))
		}
		colHist := &statistics.Column{
			PhysicalID: col.TableID,
			Histogram:  *hg,
			Info:       c.Info,
			CMSketch:   cms,
			TopN:       topN,
			FMSketch:   fms,
			Count:      int64(hg.TotalRowCount()),
			IsHandle:   c.IsHandle,
			StatsVer:   rows[0].GetInt64(0),
		}
		colHist.Count = int64(colHist.TotalRowCount())
		// Reload the latest stats cache, otherwise the `updateStatsCache` may fail with high probability, because functions
		// like `GetPartitionStats` called in `fmSketchFromStorage` would have modified the stats cache already.
		oldCache = h.statsCache.Load().(statsCache)
		tbl, ok = oldCache.tables[col.TableID]
		if !ok {
			continue
		}
		tbl = tbl.Copy()
		tbl.Columns[c.ID] = colHist
		if h.updateStatsCache(oldCache.update([]*statistics.Table{tbl}, nil, oldCache.version)) {
			statistics.HistogramNeededColumns.Delete(col)
		}
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

func (h *Handle) cmSketchAndTopNFromStorage(reader *statsReader, tblID int64, isIndex, histID int64) (_ *statistics.CMSketch, _ *statistics.TopN, err error) {
	topNRows, _, err := reader.read("select HIGH_PRIORITY value, count from mysql.stats_top_n where table_id = %? and is_index = %? and hist_id = %?", tblID, isIndex, histID)
	if err != nil {
		return nil, nil, err
	}
	rows, _, err := reader.read("select cm_sketch from mysql.stats_histograms where table_id = %? and is_index = %? and hist_id = %?", tblID, isIndex, histID)
	if err != nil {
		return nil, nil, err
	}
	if len(rows) == 0 {
		return statistics.DecodeCMSketchAndTopN(nil, topNRows)
	}
	return statistics.DecodeCMSketchAndTopN(rows[0].GetBytes(0), topNRows)
}

func (h *Handle) fmSketchFromStorage(reader *statsReader, tblID int64, isIndex, histID int64) (_ *statistics.FMSketch, err error) {
	rows, _, err := reader.read("select value from mysql.stats_fm_sketch where table_id = %? and is_index = %? and hist_id = %?", tblID, isIndex, histID)
	if err != nil || len(rows) == 0 {
		return nil, err
	}
	return statistics.DecodeFMSketch(rows[0].GetBytes(0))
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
			cms, topN, err := h.cmSketchAndTopNFromStorage(reader, table.PhysicalID, 1, idxInfo.ID)
			if err != nil {
				return errors.Trace(err)
			}
			fmSketch, err := h.fmSketchFromStorage(reader, table.PhysicalID, 1, histID)
			if err != nil {
				return errors.Trace(err)
			}
			idx = &statistics.Index{Histogram: *hg, CMSketch: cms, TopN: topN, FMSketch: fmSketch, Info: idxInfo, ErrorRate: errorRate, StatsVer: row.GetInt64(7), Flag: flag}
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
	statsVer := row.GetInt64(7)
	correlation := row.GetFloat64(9)
	lastAnalyzePos := row.GetDatum(10, types.NewFieldType(mysql.TypeBlob))
	fmSketch, err := h.fmSketchFromStorage(reader, table.PhysicalID, 0, histID)
	if err != nil {
		return errors.Trace(err)
	}
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
			count, err := h.columnCountFromStorage(reader, table.PhysicalID, histID, statsVer)
			if err != nil {
				return errors.Trace(err)
			}
			col = &statistics.Column{
				PhysicalID: table.PhysicalID,
				Histogram:  *statistics.NewHistogram(histID, distinct, nullCount, histVer, &colInfo.FieldType, 0, totColSize),
				FMSketch:   fmSketch,
				Info:       colInfo,
				Count:      count + nullCount,
				ErrorRate:  errorRate,
				IsHandle:   tableInfo.PKIsHandle && mysql.HasPriKeyFlag(colInfo.Flag),
				Flag:       flag,
				StatsVer:   statsVer,
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
			cms, topN, err := h.cmSketchAndTopNFromStorage(reader, table.PhysicalID, 0, colInfo.ID)
			if err != nil {
				return errors.Trace(err)
			}
			col = &statistics.Column{
				PhysicalID: table.PhysicalID,
				Histogram:  *hg,
				Info:       colInfo,
				CMSketch:   cms,
				TopN:       topN,
				FMSketch:   fmSketch,
				Count:      int64(hg.TotalRowCount()),
				ErrorRate:  errorRate,
				IsHandle:   tableInfo.PKIsHandle && mysql.HasPriKeyFlag(colInfo.Flag),
				Flag:       flag,
				StatsVer:   statsVer,
			}
			col.Count = int64(col.TotalRowCount())
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

	rows, _, err := reader.read("select modify_count, count from mysql.stats_meta where table_id = %?", physicalID)
	if err != nil || len(rows) == 0 {
		return nil, err
	}
	table.ModifyCount = rows[0].GetInt64(0)
	table.Count = rows[0].GetInt64(1)

	rows, _, err = reader.read("select table_id, is_index, hist_id, distinct_count, version, null_count, tot_col_size, stats_ver, flag, correlation, last_analyze_pos from mysql.stats_histograms where table_id = %?", physicalID)
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
	rows, _, err := reader.read("select name, status, type, column_ids, stats, version from mysql.stats_extended where table_id = %? and status in (%?, %?, %?) and version > %?", physicalID, StatsStatusInited, StatsStatusAnalyzed, StatsStatusDeleted, lastVersion)
	if err != nil || len(rows) == 0 {
		return table, nil
	}
	for _, row := range rows {
		lastVersion = mathutil.MaxUint64(lastVersion, row.GetUint64(5))
		name := row.GetString(0)
		status := uint8(row.GetInt64(1))
		if status == StatsStatusDeleted || status == StatsStatusInited {
			delete(table.ExtendedStats.Stats, name)
		} else {
			item := &statistics.ExtendedStatsItem{
				Tp: uint8(row.GetInt64(2)),
			}
			colIDs := row.GetString(3)
			err := json.Unmarshal([]byte(colIDs), &item.ColIDs)
			if err != nil {
				logutil.BgLogger().Error("[stats] decode column IDs failed", zap.String("column_ids", colIDs), zap.Error(err))
				return nil, err
			}
			statsStr := row.GetString(4)
			if item.Tp == ast.StatsTypeCardinality || item.Tp == ast.StatsTypeCorrelation {
				if statsStr != "" {
					item.ScalarVals, err = strconv.ParseFloat(statsStr, 64)
					if err != nil {
						logutil.BgLogger().Error("[stats] parse scalar stats failed", zap.String("stats", statsStr), zap.Error(err))
						return nil, err
					}
				}
			} else {
				item.StringVals = statsStr
			}
			table.ExtendedStats.Stats[name] = item
		}
	}
	table.ExtendedStats.LastUpdateVersion = lastVersion
	return table, nil
}

// SaveStatsToStorage saves the stats to storage.
func (h *Handle) SaveStatsToStorage(tableID int64, count int64, isIndex int, hg *statistics.Histogram, cms *statistics.CMSketch, topN *statistics.TopN, fms *statistics.FMSketch, statsVersion int, isAnalyzed int64, needDumpFMS bool) (err error) {
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
	cmSketch, err := statistics.EncodeCMSketchWithoutTopN(cms)
	if err != nil {
		return err
	}
	fmSketch, err := statistics.EncodeFMSketch(fms)
	if err != nil {
		return err
	}
	// Delete outdated data
	if _, err = exec.ExecuteInternal(ctx, "delete from mysql.stats_top_n where table_id = %? and is_index = %? and hist_id = %?", tableID, isIndex, hg.ID); err != nil {
		return err
	}
	if topN != nil {
		for _, meta := range topN.TopN {
			if _, err = exec.ExecuteInternal(ctx, "insert into mysql.stats_top_n (table_id, is_index, hist_id, value, count) values (%?, %?, %?, %?, %?)", tableID, isIndex, hg.ID, meta.Encoded, meta.Count); err != nil {
				return err
			}
		}
	}
	if _, err := exec.ExecuteInternal(ctx, "delete from mysql.stats_fm_sketch where table_id = %? and is_index = %? and hist_id = %?", tableID, isIndex, hg.ID); err != nil {
		return err
	}
	if fmSketch != nil && needDumpFMS {
		if _, err = exec.ExecuteInternal(ctx, "insert into mysql.stats_fm_sketch (table_id, is_index, hist_id, value) values (%?, %?, %?, %?)", tableID, isIndex, hg.ID, fmSketch); err != nil {
			return err
		}
	}
	flag := 0
	if isAnalyzed == 1 {
		flag = statistics.AnalyzeFlag
	}
	if _, err = exec.ExecuteInternal(ctx, "replace into mysql.stats_histograms (table_id, is_index, hist_id, distinct_count, version, null_count, cm_sketch, tot_col_size, stats_ver, flag, correlation) values (%?, %?, %?, %?, %?, %?, %?, %?, %?, %?, %?)",
		tableID, isIndex, hg.ID, hg.NDV, version, hg.NullCount, cmSketch, hg.TotColSize, statsVersion, flag, hg.Correlation); err != nil {
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
		if _, err = exec.ExecuteInternal(ctx, "insert into mysql.stats_buckets(table_id, is_index, hist_id, bucket_id, count, repeats, lower_bound, upper_bound, ndv) values(%?, %?, %?, %?, %?, %?, %?, %?, %?)", tableID, isIndex, hg.ID, i, count, hg.Buckets[i].Repeat, lowerBound.GetBytes(), upperBound.GetBytes(), hg.Buckets[i].NDV); err != nil {
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
	rows, fields, err := reader.read("select count, repeats, lower_bound, upper_bound, ndv from mysql.stats_buckets where table_id = %? and is_index = %? and hist_id = %? order by bucket_id", tableID, isIndex, colID)
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
		hg.AppendBucketWithNDV(&lowerBound, &upperBound, totalCount, repeats, rows[i].GetInt64(4))
	}
	hg.PreCalculateScalar()
	return hg, nil
}

func (h *Handle) columnCountFromStorage(reader *statsReader, tableID, colID, statsVer int64) (int64, error) {
	rows, _, err := reader.read("select sum(count) from mysql.stats_buckets where table_id = %? and is_index = 0 and hist_id = %?", tableID, colID)
	if err != nil {
		return 0, errors.Trace(err)
	}
	if rows[0].IsNull(0) {
		return 0, nil
	}
	count, err := rows[0].GetMyDecimal(0).ToInt()
	if err != nil {
		return 0, errors.Trace(err)
	}
	if statsVer >= statistics.Version2 {
		// Before stats ver 2, histogram represents all data in this column.
		// In stats ver 2, histogram + TopN represent all data in this column.
		// So we need to add TopN total count here.
		rows, _, err = reader.read("select sum(count) from mysql.stats_top_n where table_id = %? and is_index = 0 and hist_id = %?", tableID, colID)
		if err != nil {
			return 0, errors.Trace(err)
		}
		if !rows[0].IsNull(0) {
			topNCount, err := rows[0].GetMyDecimal(0).ToInt()
			if err != nil {
				return 0, errors.Trace(err)
			}
			count += topNCount
		}
	}
	return count, err
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

const (
	// StatsStatusInited is the status for extended stats which are just registered but have not been analyzed yet.
	StatsStatusInited uint8 = iota
	// StatsStatusAnalyzed is the status for extended stats which have been collected in analyze.
	StatsStatusAnalyzed
	// StatsStatusDeleted is the status for extended stats which were dropped. These "deleted" records would be removed from storage by GCStats().
	StatsStatusDeleted
)

// InsertExtendedStats inserts a record into mysql.stats_extended and update version in mysql.stats_meta.
func (h *Handle) InsertExtendedStats(statsName string, colIDs []int64, tp int, tableID int64, ifNotExists bool) (err error) {
	sort.Slice(colIDs, func(i, j int) bool { return colIDs[i] < colIDs[j] })
	bytes, err := json.Marshal(colIDs)
	if err != nil {
		return errors.Trace(err)
	}
	strColIDs := string(bytes)
	h.mu.Lock()
	defer h.mu.Unlock()
	ctx := context.Background()
	exec := h.mu.ctx.(sqlexec.SQLExecutor)
	_, err = exec.ExecuteInternal(ctx, "begin pessimistic")
	if err != nil {
		return errors.Trace(err)
	}
	defer func() {
		err = finishTransaction(ctx, exec, err)
	}()
	// No need to use `exec.ExecuteInternal` since we have acquired the lock.
	rows, _, err := h.execRestrictedSQL(ctx, "SELECT name, type, column_ids FROM mysql.stats_extended WHERE table_id = %? and status in (%?, %?)", tableID, StatsStatusInited, StatsStatusAnalyzed)
	if err != nil {
		return errors.Trace(err)
	}
	for _, row := range rows {
		currStatsName := row.GetString(0)
		currTp := row.GetInt64(1)
		currStrColIDs := row.GetString(2)
		if currStatsName == statsName {
			if ifNotExists {
				return nil
			}
			return errors.Errorf("extended statistics '%s' for the specified table already exists", statsName)
		}
		if tp == int(currTp) && currStrColIDs == strColIDs {
			return errors.Errorf("extended statistics '%s' with same type on same columns already exists", statsName)
		}
	}
	// Remove the existing 'deleted' records.
	if _, err = exec.ExecuteInternal(ctx, "DELETE FROM mysql.stats_extended WHERE name = %? and table_id = %?", statsName, tableID); err != nil {
		return err
	}
	// Remove the cache item, which is necessary for cases like a cluster with 3 tidb instances, e.g, a, b and c.
	// If tidb-a executes `alter table drop stats_extended` to mark the record as 'deleted', and before this operation
	// is synchronized to other tidb instances, tidb-b executes `alter table add stats_extended`, which would delete
	// the record from the table, tidb-b should delete the cached item synchronously. While for tidb-c, it has to wait for
	// next `Update()` to remove the cached item then.
	h.removeExtendedStatsItem(tableID, statsName)
	txn, err := h.mu.ctx.Txn(true)
	if err != nil {
		return errors.Trace(err)
	}
	version := txn.StartTS()
	const sql = "INSERT INTO mysql.stats_extended(name, type, table_id, column_ids, version, status) VALUES (%?, %?, %?, %?, %?, %?)"
	if _, err = exec.ExecuteInternal(ctx, sql, statsName, tp, tableID, strColIDs, version, StatsStatusInited); err != nil {
		return err
	}
	// Bump version in `mysql.stats_meta` to trigger stats cache refresh.
	_, err = exec.ExecuteInternal(ctx, "UPDATE mysql.stats_meta SET version = %? WHERE table_id = %?", version, tableID)
	return
}

// MarkExtendedStatsDeleted update the status of mysql.stats_extended to be `deleted` and the version of mysql.stats_meta.
func (h *Handle) MarkExtendedStatsDeleted(statsName string, tableID int64, ifExists bool) (err error) {
	ctx := context.Background()
	rows, _, err := h.execRestrictedSQL(ctx, "SELECT name FROM mysql.stats_extended WHERE name = %? and table_id = %? and status in (%?, %?)", statsName, tableID, StatsStatusInited, StatsStatusAnalyzed)
	if err != nil {
		return errors.Trace(err)
	}
	if len(rows) == 0 {
		if ifExists {
			return nil
		}
		return errors.New(fmt.Sprintf("extended statistics '%s' for the specified table does not exist", statsName))
	}
	if len(rows) > 1 {
		logutil.BgLogger().Warn("unexpected duplicate extended stats records found", zap.String("name", statsName), zap.Int64("table_id", tableID))
	}

	h.mu.Lock()
	defer h.mu.Unlock()
	exec := h.mu.ctx.(sqlexec.SQLExecutor)
	_, err = exec.ExecuteInternal(ctx, "begin pessimistic")
	if err != nil {
		return errors.Trace(err)
	}
	defer func() {
		err1 := finishTransaction(ctx, exec, err)
		if err == nil && err1 == nil {
			h.removeExtendedStatsItem(tableID, statsName)
		}
		err = err1
	}()
	txn, err := h.mu.ctx.Txn(true)
	if err != nil {
		return errors.Trace(err)
	}
	version := txn.StartTS()
	if _, err = exec.ExecuteInternal(ctx, "UPDATE mysql.stats_extended SET version = %?, status = %? WHERE name = %? and table_id = %?", version, StatsStatusDeleted, statsName, tableID); err != nil {
		return err
	}
	if _, err = exec.ExecuteInternal(ctx, "UPDATE mysql.stats_meta SET version = %? WHERE table_id = %?", version, tableID); err != nil {
		return err
	}
	return nil
}

const updateStatsCacheRetryCnt = 5

func (h *Handle) removeExtendedStatsItem(tableID int64, statsName string) {
	for retry := updateStatsCacheRetryCnt; retry > 0; retry-- {
		oldCache := h.statsCache.Load().(statsCache)
		tbl, ok := oldCache.tables[tableID]
		if !ok || tbl.ExtendedStats == nil || len(tbl.ExtendedStats.Stats) == 0 {
			return
		}
		newTbl := tbl.Copy()
		delete(newTbl.ExtendedStats.Stats, statsName)
		if h.updateStatsCache(oldCache.update([]*statistics.Table{newTbl}, nil, oldCache.version)) {
			return
		}
		if retry == 1 {
			logutil.BgLogger().Info("remove extended stats cache failed", zap.String("stats_name", statsName), zap.Int64("table_id", tableID))
		} else {
			logutil.BgLogger().Info("remove extended stats cache failed, retrying", zap.String("stats_name", statsName), zap.Int64("table_id", tableID))
		}
	}
}

// ReloadExtendedStatistics drops the cache for extended statistics and reload data from mysql.stats_extended.
func (h *Handle) ReloadExtendedStatistics() error {
	for retry := updateStatsCacheRetryCnt; retry > 0; retry-- {
		reader, err := h.getStatsReader(0)
		if err != nil {
			return err
		}
		oldCache := h.statsCache.Load().(statsCache)
		tables := make([]*statistics.Table, 0, len(oldCache.tables))
		for physicalID, tbl := range oldCache.tables {
			t, err := h.extendedStatsFromStorage(reader, tbl.Copy(), physicalID, true)
			if err != nil {
				return err
			}
			tables = append(tables, t)
		}
		err = h.releaseStatsReader(reader)
		if err != nil {
			return err
		}
		if h.updateStatsCache(oldCache.update(tables, nil, oldCache.version)) {
			return nil
		}
	}
	return errors.New(fmt.Sprintf("update stats cache failed for %d attempts", updateStatsCacheRetryCnt))
}

// BuildExtendedStats build extended stats for column groups if needed based on the column samples.
func (h *Handle) BuildExtendedStats(tableID int64, cols []*model.ColumnInfo, collectors []*statistics.SampleCollector) (*statistics.ExtendedStatsColl, error) {
	ctx := context.Background()
	const sql = "SELECT name, type, column_ids FROM mysql.stats_extended WHERE table_id = %? and status in (%?, %?)"
	rows, _, err := h.execRestrictedSQL(ctx, sql, tableID, StatsStatusAnalyzed, StatsStatusInited)
	if err != nil {
		return nil, errors.Trace(err)
	}
	if len(rows) == 0 {
		return nil, nil
	}
	statsColl := statistics.NewExtendedStatsColl()
	for _, row := range rows {
		name := row.GetString(0)
		item := &statistics.ExtendedStatsItem{Tp: uint8(row.GetInt64(1))}
		colIDs := row.GetString(2)
		err := json.Unmarshal([]byte(colIDs), &item.ColIDs)
		if err != nil {
			logutil.BgLogger().Error("invalid column_ids in mysql.stats_extended, skip collecting extended stats for this row", zap.String("column_ids", colIDs), zap.Error(err))
			continue
		}
		item = h.fillExtendedStatsItemVals(item, cols, collectors)
		if item != nil {
			statsColl.Stats[name] = item
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
	sampleNum := mathutil.Min(len(samplesX), len(samplesY))
	if sampleNum == 1 {
		item.ScalarVals = 1
		return item
	}
	if sampleNum <= 0 {
		item.ScalarVals = 0
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
	samplesYInXOrder := make([]*statistics.SampleItem, 0, sampleNum)
	for i, itemX := range samplesX {
		if itemX.Ordinal >= len(samplesY) {
			continue
		}
		itemY := samplesY[itemX.Ordinal]
		itemY.Ordinal = i
		samplesYInXOrder = append(samplesYInXOrder, itemY)
	}
	samplesYInYOrder, err := statistics.SortSampleItems(sc, samplesYInXOrder)
	if err != nil {
		return nil
	}
	var corrXYSum float64
	for i := 1; i < len(samplesYInYOrder); i++ {
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
	_, err = exec.ExecuteInternal(ctx, "begin pessimistic")
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
	for name, item := range extStats.Stats {
		bytes, err := json.Marshal(item.ColIDs)
		if err != nil {
			return errors.Trace(err)
		}
		strColIDs := string(bytes)
		var statsStr string
		switch item.Tp {
		case ast.StatsTypeCardinality, ast.StatsTypeCorrelation:
			statsStr = fmt.Sprintf("%f", item.ScalarVals)
		case ast.StatsTypeDependency:
			statsStr = item.StringVals
		}
		// If isLoad is true, it's INSERT; otherwise, it's UPDATE.
		if _, err := exec.ExecuteInternal(ctx, "replace into mysql.stats_extended values (%?, %?, %?, %?, %?, %?, %?)", name, item.Tp, tableID, strColIDs, statsStr, version, StatsStatusAnalyzed); err != nil {
			return err
		}
	}
	if !isLoad {
		if _, err := exec.ExecuteInternal(ctx, "UPDATE mysql.stats_meta SET version = %? WHERE table_id = %?", version, tableID); err != nil {
			return err
		}
	}
	return nil
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

// CheckAnalyzeVersion checks whether all the statistics versions of this table's columns and indexes are the same.
func (h *Handle) CheckAnalyzeVersion(tblInfo *model.TableInfo, physicalIDs []int64, version *int) bool {
	// We simply choose one physical id to get its stats.
	var tbl *statistics.Table
	for _, pid := range physicalIDs {
		tbl = h.GetPartitionStats(tblInfo, pid)
		if !tbl.Pseudo {
			break
		}
	}
	if tbl == nil || tbl.Pseudo {
		return true
	}
	return statistics.CheckAnalyzeVerOnTable(tbl, version)
}
