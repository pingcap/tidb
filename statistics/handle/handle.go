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
	"encoding/json"
	"fmt"
	"math"
	"strconv"
	"sync"
	"time"

	"github.com/ngaut/pools"
	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/config"
	ddlUtil "github.com/pingcap/tidb/ddl/util"
	"github.com/pingcap/tidb/infoschema"
	"github.com/pingcap/tidb/parser/ast"
	"github.com/pingcap/tidb/parser/model"
	"github.com/pingcap/tidb/parser/mysql"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/sessionctx/stmtctx"
	"github.com/pingcap/tidb/sessionctx/variable"
	"github.com/pingcap/tidb/statistics"
	"github.com/pingcap/tidb/statistics/handle/autoanalyze"
	"github.com/pingcap/tidb/statistics/handle/cache"
	"github.com/pingcap/tidb/statistics/handle/extstats"
	"github.com/pingcap/tidb/statistics/handle/globalstats"
	"github.com/pingcap/tidb/statistics/handle/history"
	handle_metrics "github.com/pingcap/tidb/statistics/handle/metrics"
	"github.com/pingcap/tidb/statistics/handle/storage"
	"github.com/pingcap/tidb/statistics/handle/usage"
	"github.com/pingcap/tidb/statistics/handle/util"
	"github.com/pingcap/tidb/table"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util/chunk"
	"github.com/pingcap/tidb/util/logutil"
	"github.com/tiancaiamao/gp"
	"github.com/tikv/client-go/v2/oracle"
	atomic2 "go.uber.org/atomic"
	"go.uber.org/zap"
)

// Handle can update stats info periodically.
type Handle struct {
	// This gpool is used to reuse goroutine in the mergeGlobalStatsTopN.
	gpool *gp.Pool
	pool  sessionPool

	// initStatsCtx is the ctx only used for initStats
	initStatsCtx sessionctx.Context

	// sysProcTracker is used to track sys process like analyze
	sysProcTracker sessionctx.SysProcTracker

	// autoAnalyzeProcIDGetter is used to generate auto analyze ID.
	autoAnalyzeProcIDGetter func() uint64

	InitStatsDone chan struct{}

	// ddlEventCh is a channel to notify a ddl operation has happened.
	// It is sent only by owner or the drop stats executor, and read by stats handle.
	ddlEventCh chan *ddlUtil.Event

	// idxUsageListHead contains all the index usage collectors required by session.
	idxUsageListHead *usage.SessionIndexUsageCollector

	// listHead contains all the stats collector required by session.
	listHead *SessionStatsCollector

	// It can be read by multiple readers at the same time without acquiring lock, but it can be
	// written only after acquiring the lock.
	statsCache *cache.StatsCachePointer

	// tableDelta contains all the delta map from collectors when we dump them to KV.
	tableDelta *usage.TableDelta

	// statsUsage contains all the column stats usage information from collectors when we dump them to KV.
	statsUsage *usage.StatsUsage

	// StatsLoad is used to load stats concurrently
	StatsLoad StatsLoad

	schemaMu struct {
		// pid2tid is the map from partition ID to table ID.
		pid2tid map[int64]int64
		// schemaVersion is the version of information schema when `pid2tid` is built.
		schemaVersion int64
		sync.RWMutex
	}

	lease atomic2.Duration
}

func (h *Handle) execRows(sql string, args ...interface{}) (rows []chunk.Row, fields []*ast.ResultField, rerr error) {
	_ = h.callWithSCtx(func(sctx sessionctx.Context) error {
		rows, fields, rerr = util.ExecRows(sctx, sql, args...)
		return nil
	})
	return
}

// Clear the statsCache, only for test.
func (h *Handle) Clear() {
	cache, err := cache.NewStatsCache()
	if err != nil {
		logutil.BgLogger().Warn("create stats cache failed", zap.Error(err))
		return
	}
	h.statsCache.Replace(cache)
	for len(h.ddlEventCh) > 0 {
		<-h.ddlEventCh
	}
	h.listHead.ClearForTest()
	h.tableDelta.Reset()
	h.statsUsage.Reset()
}

type sessionPool interface {
	Get() (pools.Resource, error)
	Put(pools.Resource)
}

// NewHandle creates a Handle for update stats.
func NewHandle(_, initStatsCtx sessionctx.Context, lease time.Duration, pool sessionPool, tracker sessionctx.SysProcTracker, autoAnalyzeProcIDGetter func() uint64) (*Handle, error) {
	cfg := config.GetGlobalConfig()

	handle := &Handle{
		gpool:                   gp.New(math.MaxInt16, time.Minute),
		ddlEventCh:              make(chan *ddlUtil.Event, 1000),
		listHead:                NewSessionStatsCollector(),
		idxUsageListHead:        usage.NewSessionIndexUsageCollector(nil),
		pool:                    pool,
		sysProcTracker:          tracker,
		autoAnalyzeProcIDGetter: autoAnalyzeProcIDGetter,
		InitStatsDone:           make(chan struct{}),
	}
	handle.initStatsCtx = initStatsCtx
	handle.lease.Store(lease)
	statsCache, err := cache.NewStatsCachePointer()
	if err != nil {
		return nil, err
	}
	handle.statsCache = statsCache
	handle.tableDelta = usage.NewTableDelta()
	handle.statsUsage = usage.NewStatsUsage()
	handle.StatsLoad.SubCtxs = make([]sessionctx.Context, cfg.Performance.StatsLoadConcurrency)
	handle.StatsLoad.NeededItemsCh = make(chan *NeededItemTask, cfg.Performance.StatsLoadQueueSize)
	handle.StatsLoad.TimeoutItemsCh = make(chan *NeededItemTask, cfg.Performance.StatsLoadQueueSize)
	handle.StatsLoad.WorkingColMap = map[model.TableItemID][]chan stmtctx.StatsLoadResult{}
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

// DurationToTS converts duration to timestamp.
func DurationToTS(d time.Duration) uint64 {
	return oracle.ComposeTS(d.Nanoseconds()/int64(time.Millisecond), 0)
}

// UpdateStatsHealthyMetrics updates stats healthy distribution metrics according to stats cache.
func (h *Handle) UpdateStatsHealthyMetrics() {
	v := h.statsCache.Load()
	if v == nil {
		return
	}

	distribution := make([]int64, 5)
	for _, tbl := range v.Values() {
		healthy, ok := tbl.GetStatsHealthy()
		if !ok {
			continue
		}
		if healthy < 50 {
			distribution[0]++
		} else if healthy < 80 {
			distribution[1]++
		} else if healthy < 100 {
			distribution[2]++
		} else {
			distribution[3]++
		}
		distribution[4]++
	}
	for i, val := range distribution {
		handle_metrics.StatsHealthyGauges[i].Set(float64(val))
	}
}

// Update reads stats meta from store and updates the stats map.
func (h *Handle) Update(is infoschema.InfoSchema) error {
	oldCache := h.statsCache.Load()
	lastVersion := oldCache.Version()
	// We need this because for two tables, the smaller version may write later than the one with larger version.
	// Consider the case that there are two tables A and B, their version and commit time is (A0, A1) and (B0, B1),
	// and A0 < B0 < B1 < A1. We will first read the stats of B, and update the lastVersion to B0, but we cannot read
	// the table stats of A0 if we read stats that greater than lastVersion which is B0.
	// We can read the stats if the diff between commit time and version is less than three lease.
	offset := DurationToTS(3 * h.Lease())
	if oldCache.Version() >= offset {
		lastVersion = lastVersion - offset
	} else {
		lastVersion = 0
	}
	rows, _, err := h.execRows("SELECT version, table_id, modify_count, count from mysql.stats_meta where version > %? order by version", lastVersion)
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
		table, ok := h.getTableByPhysicalID(is, physicalID)
		if !ok {
			logutil.BgLogger().Debug("unknown physical ID in stats meta table, maybe it has been dropped", zap.Int64("ID", physicalID))
			deletedTableIDs = append(deletedTableIDs, physicalID)
			continue
		}
		tableInfo := table.Meta()
		if oldTbl, ok := oldCache.Get(physicalID); ok && oldTbl.Version >= version && tableInfo.UpdateTS == oldTbl.TblInfoUpdateTS {
			continue
		}
		tbl, err := h.TableStatsFromStorage(tableInfo, physicalID, false, 0)
		// Error is not nil may mean that there are some ddl changes on this table, we will not update it.
		if err != nil {
			logutil.BgLogger().Error("error occurred when read table stats", zap.String("category", "stats"), zap.String("table", tableInfo.Name.O), zap.Error(err))
			continue
		}
		if tbl == nil {
			deletedTableIDs = append(deletedTableIDs, physicalID)
			continue
		}
		tbl.Version = version
		tbl.RealtimeCount = count
		tbl.ModifyCount = modifyCount
		tbl.Name = getFullTableName(is, tableInfo)
		tbl.TblInfoUpdateTS = tableInfo.UpdateTS
		tables = append(tables, tbl)
	}
	h.updateStatsCache(oldCache, tables, deletedTableIDs)
	return nil
}

// UpdateSCtxVarsForStats updates all necessary variables that may affect the behavior of statistics.
func UpdateSCtxVarsForStats(sctx sessionctx.Context) error {
	// analyzer version
	verInString, err := sctx.GetSessionVars().GlobalVarsAccessor.GetGlobalSysVar(variable.TiDBAnalyzeVersion)
	if err != nil {
		return err
	}
	ver, err := strconv.ParseInt(verInString, 10, 64)
	if err != nil {
		return err
	}
	sctx.GetSessionVars().AnalyzeVersion = int(ver)

	// enable historical stats
	val, err := sctx.GetSessionVars().GlobalVarsAccessor.GetGlobalSysVar(variable.TiDBEnableHistoricalStats)
	if err != nil {
		return err
	}
	sctx.GetSessionVars().EnableHistoricalStats = variable.TiDBOptOn(val)

	// partition mode
	pruneMode, err := sctx.GetSessionVars().GlobalVarsAccessor.GetGlobalSysVar(variable.TiDBPartitionPruneMode)
	if err != nil {
		return err
	}
	sctx.GetSessionVars().PartitionPruneMode.Store(pruneMode)

	// enable analyze snapshot
	analyzeSnapshot, err := sctx.GetSessionVars().GlobalVarsAccessor.GetGlobalSysVar(variable.TiDBEnableAnalyzeSnapshot)
	if err != nil {
		return err
	}
	sctx.GetSessionVars().EnableAnalyzeSnapshot = variable.TiDBOptOn(analyzeSnapshot)

	// enable skip column types
	val, err = sctx.GetSessionVars().GlobalVarsAccessor.GetGlobalSysVar(variable.TiDBAnalyzeSkipColumnTypes)
	if err != nil {
		return err
	}
	sctx.GetSessionVars().AnalyzeSkipColumnTypes = variable.ParseAnalyzeSkipColumnTypes(val)

	// skip missing partition stats
	val, err = sctx.GetSessionVars().GlobalVarsAccessor.GetGlobalSysVar(variable.TiDBSkipMissingPartitionStats)
	if err != nil {
		return err
	}
	sctx.GetSessionVars().SkipMissingPartitionStats = variable.TiDBOptOn(val)
	return nil
}

// MergePartitionStats2GlobalStatsByTableID merge the partition-level stats to global-level stats based on the tableID.
func (h *Handle) MergePartitionStats2GlobalStatsByTableID(
	sc sessionctx.Context,
	opts map[ast.AnalyzeOptionType]uint64, is infoschema.InfoSchema,
	physicalID int64,
	isIndex bool,
	histIDs []int64,
	allPartitionStats map[int64]*statistics.Table,
) (globalStats *globalstats.GlobalStats, err error) {
	return globalstats.MergePartitionStats2GlobalStatsByTableID(sc, h.gpool, opts, is, physicalID, isIndex, histIDs, allPartitionStats, h.getTableByPhysicalID, h.loadTablePartitionStats)
}

func (h *Handle) loadTablePartitionStats(tableInfo *model.TableInfo, partitionDef *model.PartitionDefinition) (*statistics.Table, error) {
	var partitionStats *statistics.Table
	partitionStats, err := h.TableStatsFromStorage(tableInfo, partitionDef.ID, true, 0)
	if err != nil {
		return nil, err
	}
	// if the err == nil && partitionStats == nil, it means we lack the partition-level stats which the physicalID is equal to partitionID.
	if partitionStats == nil {
		errMsg := fmt.Sprintf("table `%s` partition `%s`", tableInfo.Name.L, partitionDef.Name.L)
		err = types.ErrPartitionStatsMissing.GenWithStackByArgs(errMsg)
		return nil, err
	}
	return partitionStats, nil
}

// MergePartitionStats2GlobalStatsByTableID merge the partition-level stats to global-level stats based on the tableInfo.
func (h *Handle) mergePartitionStats2GlobalStats(
	opts map[ast.AnalyzeOptionType]uint64,
	is infoschema.InfoSchema,
	globalTableInfo *model.TableInfo,
	isIndex bool,
	histIDs []int64,
	allPartitionStats map[int64]*statistics.Table,
) (gstats *globalstats.GlobalStats, err error) {
	err = h.callWithSCtx(func(sctx sessionctx.Context) error {
		gstats, err = globalstats.MergePartitionStats2GlobalStats(sctx, h.gpool, opts, is, globalTableInfo, isIndex, histIDs, allPartitionStats, h.getTableByPhysicalID, h.loadTablePartitionStats)
		return err
	})
	return
}

func (h *Handle) getTableByPhysicalID(is infoschema.InfoSchema, physicalID int64) (table.Table, bool) {
	h.schemaMu.Lock()
	defer h.schemaMu.Unlock()
	if is.SchemaMetaVersion() != h.schemaMu.schemaVersion {
		h.schemaMu.schemaVersion = is.SchemaMetaVersion()
		h.schemaMu.pid2tid = buildPartitionID2TableID(is)
	}
	if id, ok := h.schemaMu.pid2tid[physicalID]; ok {
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
	size = h.statsCache.Load().Cost()
	return
}

// GetTableStats retrieves the statistics table from cache, and the cache will be updated by a goroutine.
func (h *Handle) GetTableStats(tblInfo *model.TableInfo) *statistics.Table {
	return h.GetPartitionStats(tblInfo, tblInfo.ID)
}

// GetPartitionStats retrieves the partition stats from cache.
func (h *Handle) GetPartitionStats(tblInfo *model.TableInfo, pid int64) *statistics.Table {
	var tbl *statistics.Table
	if h == nil {
		tbl = statistics.PseudoTable(tblInfo, false)
		tbl.PhysicalID = pid
		return tbl
	}
	statsCache := h.statsCache.Load()
	tbl, ok := statsCache.Get(pid)
	if !ok {
		tbl = statistics.PseudoTable(tblInfo, false)
		tbl.PhysicalID = pid
		if tblInfo.GetPartitionInfo() == nil || h.statsCacheLen() < 64 {
			h.updateStatsCache(statsCache, []*statistics.Table{tbl}, nil)
		}
		return tbl
	}
	return tbl
}

func (h *Handle) statsCacheLen() int {
	return h.statsCache.Load().Len()
}

func (h *Handle) initStatsCache(newCache *cache.StatsCache) {
	h.statsCache.Replace(newCache)
}

// updateStatsCache will update statsCache into non COW mode.
// If it is in the COW mode. it overrides the global statsCache with a new one, it may fail
// if the global statsCache has been modified by others already.
// Callers should add retry loop if necessary.
func (h *Handle) updateStatsCache(newCache *cache.StatsCache, tables []*statistics.Table, deletedIDs []int64) (updated bool) {
	h.statsCache.UpdateStatsCache(newCache, tables, deletedIDs)
	return true
}

// LoadNeededHistograms will load histograms for those needed columns/indices.
func (h *Handle) LoadNeededHistograms() (err error) {
	err = h.callWithSCtx(func(sctx sessionctx.Context) error {
		loadFMSketch := config.GetGlobalConfig().Performance.EnableLoadFMSketch
		return storage.LoadNeededHistograms(sctx, h.statsCache, loadFMSketch)
	}, flagWrapTxn)
	return err
}

// LastUpdateVersion gets the last update version.
func (h *Handle) LastUpdateVersion() uint64 {
	return h.statsCache.Load().Version()
}

// FlushStats flushes the cached stats update into store.
func (h *Handle) FlushStats() {
	for len(h.ddlEventCh) > 0 {
		e := <-h.ddlEventCh
		if err := h.HandleDDLEvent(e); err != nil {
			logutil.BgLogger().Error("handle ddl event fail", zap.String("category", "stats"), zap.Error(err))
		}
	}
	if err := h.DumpStatsDeltaToKV(DumpAll); err != nil {
		logutil.BgLogger().Error("dump stats delta fail", zap.String("category", "stats"), zap.Error(err))
	}
}

// TableStatsFromStorage loads table stats info from storage.
func (h *Handle) TableStatsFromStorage(tableInfo *model.TableInfo, physicalID int64, loadAll bool, snapshot uint64) (statsTbl *statistics.Table, err error) {
	err = h.callWithSCtx(func(sctx sessionctx.Context) error {
		var ok bool
		statsTbl, ok = h.statsCache.Load().Get(physicalID)
		if !ok {
			statsTbl = nil
		}
		statsTbl, err = storage.TableStatsFromStorage(sctx, snapshot, tableInfo, physicalID, loadAll, h.Lease(), statsTbl)
		return err
	}, flagWrapTxn)
	return
}

// StatsMetaCountAndModifyCount reads count and modify_count for the given table from mysql.stats_meta.
func (h *Handle) StatsMetaCountAndModifyCount(tableID int64) (count, modifyCount int64, err error) {
	err = h.callWithSCtx(func(sctx sessionctx.Context) error {
		rows, _, err := util.ExecRows(sctx, "select count, modify_count from mysql.stats_meta where table_id = %?", tableID)
		if err != nil {
			return err
		}
		if len(rows) == 0 {
			return nil
		}
		count = int64(rows[0].GetUint64(0))
		modifyCount = rows[0].GetInt64(1)
		return nil
	}, flagWrapTxn)
	return
}

// SaveTableStatsToStorage saves the stats of a table to storage.
func (h *Handle) SaveTableStatsToStorage(results *statistics.AnalyzeResults, analyzeSnapshot bool, source string) (err error) {
	return h.callWithSCtx(func(sctx sessionctx.Context) error {
		return SaveTableStatsToStorage(sctx, results, analyzeSnapshot, source)
	})
}

// SaveTableStatsToStorage saves the stats of a table to storage.
func SaveTableStatsToStorage(sctx sessionctx.Context, results *statistics.AnalyzeResults, analyzeSnapshot bool, source string) error {
	statsVer, err := storage.SaveTableStatsToStorage(sctx, results, analyzeSnapshot)
	if err == nil && statsVer != 0 {
		tableID := results.TableID.GetStatisticsID()
		if err1 := history.RecordHistoricalStatsMeta(sctx, tableID, statsVer, source); err1 != nil {
			logutil.BgLogger().Error("record historical stats meta failed",
				zap.Int64("table-id", tableID),
				zap.Uint64("version", statsVer),
				zap.String("source", source),
				zap.Error(err1))
		}
	}
	return err
}

// SaveStatsToStorage saves the stats to storage.
// If count is negative, both count and modify count would not be used and not be written to the table. Unless, corresponding
// fields in the stats_meta table will be updated.
// TODO: refactor to reduce the number of parameters
func (h *Handle) SaveStatsToStorage(tableID int64, count, modifyCount int64, isIndex int, hg *statistics.Histogram,
	cms *statistics.CMSketch, topN *statistics.TopN, statsVersion int, isAnalyzed int64, updateAnalyzeTime bool, source string) (err error) {
	var statsVer uint64
	err = h.callWithSCtx(func(sctx sessionctx.Context) error {
		statsVer, err = storage.SaveStatsToStorage(sctx, tableID,
			count, modifyCount, isIndex, hg, cms, topN, statsVersion, isAnalyzed, updateAnalyzeTime)
		return err
	})
	if err == nil && statsVer != 0 {
		h.recordHistoricalStatsMeta(tableID, statsVer, source)
	}
	return
}

// SaveMetaToStorage will save stats_meta to storage.
func (h *Handle) SaveMetaToStorage(tableID, count, modifyCount int64, source string) (err error) {
	var statsVer uint64
	err = h.callWithSCtx(func(sctx sessionctx.Context) error {
		statsVer, err = storage.SaveMetaToStorage(sctx, tableID, count, modifyCount)
		return err
	})
	if err == nil && statsVer != 0 {
		h.recordHistoricalStatsMeta(tableID, statsVer, source)
	}
	return
}

// InsertExtendedStats inserts a record into mysql.stats_extended and update version in mysql.stats_meta.
func (h *Handle) InsertExtendedStats(statsName string, colIDs []int64, tp int, tableID int64, ifNotExists bool) (err error) {
	var statsVer uint64
	err = h.callWithSCtx(func(sctx sessionctx.Context) error {
		statsVer, err = extstats.InsertExtendedStats(sctx, h.updateStatsCache, h.statsCache.Load(), statsName, colIDs, tp, tableID, ifNotExists)
		return err
	})
	if err == nil && statsVer != 0 {
		h.recordHistoricalStatsMeta(tableID, statsVer, StatsMetaHistorySourceExtendedStats)
	}
	return
}

// MarkExtendedStatsDeleted update the status of mysql.stats_extended to be `deleted` and the version of mysql.stats_meta.
func (h *Handle) MarkExtendedStatsDeleted(statsName string, tableID int64, ifExists bool) (err error) {
	var statsVer uint64
	err = h.callWithSCtx(func(sctx sessionctx.Context) error {
		statsVer, err = extstats.MarkExtendedStatsDeleted(sctx, h.updateStatsCache, h.statsCache.Load(), statsName, tableID, ifExists)
		return err
	})
	if err == nil && statsVer != 0 {
		h.recordHistoricalStatsMeta(tableID, statsVer, StatsMetaHistorySourceExtendedStats)
	}
	return
}

const updateStatsCacheRetryCnt = 5

// ReloadExtendedStatistics drops the cache for extended statistics and reload data from mysql.stats_extended.
// TODO: move this method to the `extstats` package.
func (h *Handle) ReloadExtendedStatistics() error {
	return h.callWithSCtx(func(sctx sessionctx.Context) error {
		for retry := updateStatsCacheRetryCnt; retry > 0; retry-- {
			oldCache := h.statsCache.Load()
			tables := make([]*statistics.Table, 0, oldCache.Len())
			for _, tbl := range oldCache.Values() {
				t, err := storage.ExtendedStatsFromStorage(sctx, tbl.Copy(), tbl.PhysicalID, true)
				if err != nil {
					return err
				}
				tables = append(tables, t)
			}
			if h.updateStatsCache(oldCache, tables, nil) {
				return nil
			}
		}
		return fmt.Errorf("update stats cache failed for %d attempts", updateStatsCacheRetryCnt)
	}, flagWrapTxn)
}

// BuildExtendedStats build extended stats for column groups if needed based on the column samples.
func (h *Handle) BuildExtendedStats(tableID int64, cols []*model.ColumnInfo, collectors []*statistics.SampleCollector) (es *statistics.ExtendedStatsColl, err error) {
	err = h.callWithSCtx(func(sctx sessionctx.Context) error {
		es, err = extstats.BuildExtendedStats(sctx, tableID, cols, collectors)
		return err
	})
	return es, err
}

// SaveExtendedStatsToStorage writes extended stats of a table into mysql.stats_extended.
func (h *Handle) SaveExtendedStatsToStorage(tableID int64, extStats *statistics.ExtendedStatsColl, isLoad bool) (err error) {
	var statsVer uint64
	err = h.callWithSCtx(func(sctx sessionctx.Context) error {
		statsVer, err = extstats.SaveExtendedStatsToStorage(sctx, tableID, extStats, isLoad)
		return err
	})
	if err == nil && statsVer != 0 {
		h.recordHistoricalStatsMeta(tableID, statsVer, StatsMetaHistorySourceExtendedStats)
	}
	return
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

type colStatsTimeInfo struct {
	LastUsedAt     *types.Time
	LastAnalyzedAt *types.Time
}

// getDisableColumnTrackingTime reads the value of tidb_disable_column_tracking_time from mysql.tidb if it exists.
func (h *Handle) getDisableColumnTrackingTime() (*time.Time, error) {
	rows, fields, err := h.execRows("SELECT variable_value FROM %n.%n WHERE variable_name = %?", mysql.SystemDB, mysql.TiDBTable, variable.TiDBDisableColumnTrackingTime)
	if err != nil {
		return nil, err
	}
	if len(rows) == 0 {
		return nil, nil
	}
	d := rows[0].GetDatum(0, &fields[0].Column.FieldType)
	// The string represents the UTC time when tidb_enable_column_tracking is set to 0.
	value, err := d.ToString()
	if err != nil {
		return nil, err
	}
	t, err := time.Parse(types.UTCTimeFormat, value)
	if err != nil {
		return nil, err
	}
	return &t, nil
}

// LoadColumnStatsUsage loads column stats usage information from disk.
func (h *Handle) LoadColumnStatsUsage(loc *time.Location) (map[model.TableItemID]colStatsTimeInfo, error) {
	disableTime, err := h.getDisableColumnTrackingTime()
	if err != nil {
		return nil, errors.Trace(err)
	}
	// Since we use another session from session pool to read mysql.column_stats_usage, which may have different @@time_zone, so we do time zone conversion here.
	rows, _, err := h.execRows("SELECT table_id, column_id, CONVERT_TZ(last_used_at, @@TIME_ZONE, '+00:00'), CONVERT_TZ(last_analyzed_at, @@TIME_ZONE, '+00:00') FROM mysql.column_stats_usage")
	if err != nil {
		return nil, errors.Trace(err)
	}
	colStatsMap := make(map[model.TableItemID]colStatsTimeInfo, len(rows))
	for _, row := range rows {
		if row.IsNull(0) || row.IsNull(1) {
			continue
		}
		tblColID := model.TableItemID{TableID: row.GetInt64(0), ID: row.GetInt64(1), IsIndex: false}
		var statsUsage colStatsTimeInfo
		if !row.IsNull(2) {
			gt, err := row.GetTime(2).GoTime(time.UTC)
			if err != nil {
				return nil, errors.Trace(err)
			}
			// If `last_used_at` is before the time when `set global enable_column_tracking = 0`, we should ignore it because
			// `set global enable_column_tracking = 0` indicates all the predicate columns collected before.
			if disableTime == nil || gt.After(*disableTime) {
				t := types.NewTime(types.FromGoTime(gt.In(loc)), mysql.TypeTimestamp, types.DefaultFsp)
				statsUsage.LastUsedAt = &t
			}
		}
		if !row.IsNull(3) {
			gt, err := row.GetTime(3).GoTime(time.UTC)
			if err != nil {
				return nil, errors.Trace(err)
			}
			t := types.NewTime(types.FromGoTime(gt.In(loc)), mysql.TypeTimestamp, types.DefaultFsp)
			statsUsage.LastAnalyzedAt = &t
		}
		colStatsMap[tblColID] = statsUsage
	}
	return colStatsMap, nil
}

// CollectColumnsInExtendedStats returns IDs of the columns involved in extended stats.
func (h *Handle) CollectColumnsInExtendedStats(tableID int64) ([]int64, error) {
	const sql = "SELECT name, type, column_ids FROM mysql.stats_extended WHERE table_id = %? and status in (%?, %?)"
	rows, _, err := h.execRows(sql, tableID, statistics.ExtendedStatsAnalyzed, statistics.ExtendedStatsInited)
	if err != nil {
		return nil, errors.Trace(err)
	}
	if len(rows) == 0 {
		return nil, nil
	}
	columnIDs := make([]int64, 0, len(rows)*2)
	for _, row := range rows {
		twoIDs := make([]int64, 0, 2)
		data := row.GetString(2)
		err := json.Unmarshal([]byte(data), &twoIDs)
		if err != nil {
			logutil.BgLogger().Error("invalid column_ids in mysql.stats_extended, skip collecting extended stats for this row", zap.String("column_ids", data), zap.Error(err))
			continue
		}
		columnIDs = append(columnIDs, twoIDs...)
	}
	return columnIDs, nil
}

// GetPredicateColumns returns IDs of predicate columns, which are the columns whose stats are used(needed) when generating query plans.
func (h *Handle) GetPredicateColumns(tableID int64) ([]int64, error) {
	disableTime, err := h.getDisableColumnTrackingTime()
	if err != nil {
		return nil, errors.Trace(err)
	}
	rows, _, err := h.execRows("SELECT column_id, CONVERT_TZ(last_used_at, @@TIME_ZONE, '+00:00') FROM mysql.column_stats_usage WHERE table_id = %? AND last_used_at IS NOT NULL", tableID)
	if err != nil {
		return nil, errors.Trace(err)
	}
	columnIDs := make([]int64, 0, len(rows))
	for _, row := range rows {
		if row.IsNull(0) || row.IsNull(1) {
			continue
		}
		colID := row.GetInt64(0)
		gt, err := row.GetTime(1).GoTime(time.UTC)
		if err != nil {
			return nil, errors.Trace(err)
		}
		// If `last_used_at` is before the time when `set global enable_column_tracking = 0`, we don't regard the column as predicate column because
		// `set global enable_column_tracking = 0` indicates all the predicate columns collected before.
		if disableTime == nil || gt.After(*disableTime) {
			columnIDs = append(columnIDs, colID)
		}
	}
	return columnIDs, nil
}

// RecordHistoricalStatsToStorage records the given table's stats data to mysql.stats_history
func (h *Handle) RecordHistoricalStatsToStorage(dbName string, tableInfo *model.TableInfo, physicalID int64, isPartition bool) (uint64, error) {
	var js *storage.JSONTable
	var err error
	if isPartition {
		js, err = h.tableStatsToJSON(dbName, tableInfo, physicalID, 0)
	} else {
		js, err = h.DumpStatsToJSON(dbName, tableInfo, nil, true)
	}
	if err != nil {
		return 0, errors.Trace(err)
	}

	var version uint64
	err = h.callWithSCtx(func(sctx sessionctx.Context) error {
		version, err = history.RecordHistoricalStatsToStorage(sctx, physicalID, js)
		return err
	}, flagWrapTxn)
	return version, err
}

// CheckHistoricalStatsEnable is used to check whether TiDBEnableHistoricalStats is enabled.
func (h *Handle) CheckHistoricalStatsEnable() (enable bool, err error) {
	err = h.callWithSCtx(func(sctx sessionctx.Context) error {
		enable = sctx.GetSessionVars().EnableHistoricalStats
		return nil
	})
	return
}

// InsertAnalyzeJob inserts analyze job into mysql.analyze_jobs and gets job ID for further updating job.
func (h *Handle) InsertAnalyzeJob(job *statistics.AnalyzeJob, instance string, procID uint64) error {
	return h.callWithSCtx(func(sctx sessionctx.Context) error {
		return autoanalyze.InsertAnalyzeJob(sctx, job, instance, procID)
	})
}

// DeleteAnalyzeJobs deletes the analyze jobs whose update time is earlier than updateTime.
func (h *Handle) DeleteAnalyzeJobs(updateTime time.Time) error {
	_, _, err := h.execRows("DELETE FROM mysql.analyze_jobs WHERE update_time < CONVERT_TZ(%?, '+00:00', @@TIME_ZONE)", updateTime.UTC().Format(types.TimeFormat))
	return err
}

// SetStatsCacheCapacity sets capacity
func (h *Handle) SetStatsCacheCapacity(c int64) {
	if h == nil {
		return
	}
	v := h.statsCache.Load()
	if v == nil {
		return
	}
	sc := v
	sc.SetCapacity(c)
	logutil.BgLogger().Info("update stats cache capacity successfully", zap.Int64("capacity", c))
}

// Close stops the background
func (h *Handle) Close() {
	h.gpool.Close()
	h.statsCache.Load().Close()
}

const (
	// StatsMetaHistorySourceAnalyze indicates stats history meta source from analyze
	StatsMetaHistorySourceAnalyze = "analyze"
	// StatsMetaHistorySourceLoadStats indicates stats history meta source from load stats
	StatsMetaHistorySourceLoadStats = "load stats"
	// StatsMetaHistorySourceFlushStats indicates stats history meta source from flush stats
	StatsMetaHistorySourceFlushStats = "flush stats"
	// StatsMetaHistorySourceSchemaChange indicates stats history meta source from schema change
	StatsMetaHistorySourceSchemaChange = "schema change"
	// StatsMetaHistorySourceExtendedStats indicates stats history meta source from extended stats
	StatsMetaHistorySourceExtendedStats = "extended stats"
)

func (h *Handle) recordHistoricalStatsMeta(tableID int64, version uint64, source string) {
	v := h.statsCache.Load()
	if v == nil {
		return
	}
	sc := v
	tbl, ok := sc.Get(tableID)
	if !ok {
		return
	}
	if !tbl.IsInitialized() {
		return
	}
	err := h.callWithSCtx(func(sctx sessionctx.Context) error {
		return history.RecordHistoricalStatsMeta(sctx, tableID, version, source)
	})
	if err != nil {
		logutil.BgLogger().Error("record historical stats meta failed",
			zap.Int64("table-id", tableID),
			zap.Uint64("version", version),
			zap.String("source", source),
			zap.Error(err))
		return
	}
}
