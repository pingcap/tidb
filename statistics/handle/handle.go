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
	"fmt"
	"math"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/config"
	ddlUtil "github.com/pingcap/tidb/ddl/util"
	"github.com/pingcap/tidb/infoschema"
	"github.com/pingcap/tidb/parser/ast"
	"github.com/pingcap/tidb/parser/model"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/sessionctx/stmtctx"
	"github.com/pingcap/tidb/statistics"
	"github.com/pingcap/tidb/statistics/handle/autoanalyze"
	"github.com/pingcap/tidb/statistics/handle/cache"
	"github.com/pingcap/tidb/statistics/handle/extstats"
	"github.com/pingcap/tidb/statistics/handle/globalstats"
	"github.com/pingcap/tidb/statistics/handle/history"
	"github.com/pingcap/tidb/statistics/handle/lockstats"
	handle_metrics "github.com/pingcap/tidb/statistics/handle/metrics"
	"github.com/pingcap/tidb/statistics/handle/storage"
	"github.com/pingcap/tidb/statistics/handle/usage"
	"github.com/pingcap/tidb/statistics/handle/util"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util/chunk"
	"github.com/pingcap/tidb/util/logutil"
	"github.com/tiancaiamao/gp"
	atomic2 "go.uber.org/atomic"
	"go.uber.org/zap"
)

// Handle can update stats info periodically.
type Handle struct {
	pool util.SessionPool

	// initStatsCtx is the ctx only used for initStats
	initStatsCtx sessionctx.Context

	// sysProcTracker is used to track sys process like analyze
	sysProcTracker sessionctx.SysProcTracker

	// TableInfoGetter is used to fetch table meta info.
	util.TableInfoGetter

	// StatsGC is used to GC stats.
	util.StatsGC

	// StatsUsage is used to track the usage of column / index statistics.
	util.StatsUsage

	// StatsHistory is used to manage historical stats.
	util.StatsHistory

	// StatsAnalyze is used to handle auto-analyze and manage analyze jobs.
	util.StatsAnalyze

	// StatsLock is used to manage locked stats.
	util.StatsLock

	// This gpool is used to reuse goroutine in the mergeGlobalStatsTopN.
	gpool *gp.Pool

	// autoAnalyzeProcIDGetter is used to generate auto analyze ID.
	autoAnalyzeProcIDGetter func() uint64

	InitStatsDone chan struct{}

	// ddlEventCh is a channel to notify a ddl operation has happened.
	// It is sent only by owner or the drop stats executor, and read by stats handle.
	ddlEventCh chan *ddlUtil.Event

	// StatsCache ...
	util.StatsCache

	// StatsLoad is used to load stats concurrently
	StatsLoad StatsLoad

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
	h.StatsCache.Clear()
	for len(h.ddlEventCh) > 0 {
		<-h.ddlEventCh
	}
	h.ResetSessionStatsList()
}

// NewHandle creates a Handle for update stats.
func NewHandle(_, initStatsCtx sessionctx.Context, lease time.Duration, pool util.SessionPool, tracker sessionctx.SysProcTracker, autoAnalyzeProcIDGetter func() uint64) (*Handle, error) {
	cfg := config.GetGlobalConfig()
	handle := &Handle{
		gpool:                   gp.New(math.MaxInt16, time.Minute),
		ddlEventCh:              make(chan *ddlUtil.Event, 1000),
		pool:                    pool,
		sysProcTracker:          tracker,
		autoAnalyzeProcIDGetter: autoAnalyzeProcIDGetter,
		InitStatsDone:           make(chan struct{}),
		TableInfoGetter:         util.NewTableInfoGetter(),
		StatsAnalyze:            autoanalyze.NewStatsAnalyze(pool),
		StatsLock:               lockstats.NewStatsLock(pool),
	}
	handle.StatsGC = storage.NewStatsGC(pool, lease, handle.TableInfoGetter, handle.MarkExtendedStatsDeleted)

	handle.initStatsCtx = initStatsCtx
	handle.lease.Store(lease)
	statsCache, err := cache.NewStatsCacheImpl()
	if err != nil {
		return nil, err
	}
	handle.StatsCache = statsCache
	handle.StatsHistory = history.NewStatsHistory(pool, handle.StatsCache)
	handle.StatsUsage = usage.NewStatsUsageImpl(pool, handle.TableInfoGetter, handle.StatsCache,
		handle.StatsHistory, handle.GetLockedTables, handle.GetPartitionStats)
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

// UpdateStatsHealthyMetrics updates stats healthy distribution metrics according to stats cache.
func (h *Handle) UpdateStatsHealthyMetrics() {
	distribution := make([]int64, 5)
	for _, tbl := range h.Values() {
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
	lastVersion := h.MaxTableStatsVersion()
	// We need this because for two tables, the smaller version may write later than the one with larger version.
	// Consider the case that there are two tables A and B, their version and commit time is (A0, A1) and (B0, B1),
	// and A0 < B0 < B1 < A1. We will first read the stats of B, and update the lastVersion to B0, but we cannot read
	// the table stats of A0 if we read stats that greater than lastVersion which is B0.
	// We can read the stats if the diff between commit time and version is less than three lease.
	offset := util.DurationToTS(3 * h.Lease())
	if h.MaxTableStatsVersion() >= offset {
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
		table, ok := h.TableInfoByID(is, physicalID)
		if !ok {
			logutil.BgLogger().Debug("unknown physical ID in stats meta table, maybe it has been dropped", zap.Int64("ID", physicalID))
			deletedTableIDs = append(deletedTableIDs, physicalID)
			continue
		}
		tableInfo := table.Meta()
		if oldTbl, ok := h.Get(physicalID); ok && oldTbl.Version >= version && tableInfo.UpdateTS == oldTbl.TblInfoUpdateTS {
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
	h.UpdateStatsCache(tables, deletedTableIDs)
	return nil
}

// MergePartitionStats2GlobalStatsByTableID merge the partition-level stats to global-level stats based on the tableID.
func (h *Handle) MergePartitionStats2GlobalStatsByTableID(sc sessionctx.Context,
	opts map[ast.AnalyzeOptionType]uint64, is infoschema.InfoSchema,
	physicalID int64,
	isIndex bool,
	histIDs []int64,
	_ map[int64]*statistics.Table,
) (globalStats *globalstats.GlobalStats, err error) {
	return globalstats.MergePartitionStats2GlobalStatsByTableID(sc, h.gpool, opts, is, physicalID, isIndex, histIDs, h.TableInfoByID, h.callWithSCtx)
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
	_ map[int64]*statistics.Table,
) (gstats *globalstats.GlobalStats, err error) {
	err = h.callWithSCtx(func(sctx sessionctx.Context) error {
		gstats, err = globalstats.MergePartitionStats2GlobalStats(sctx, h.gpool, opts, is, globalTableInfo, isIndex,
			histIDs, h.TableInfoByID, h.callWithSCtx)
		return err
	})
	return
}

// GetTableStats retrieves the statistics table from cache, and the cache will be updated by a goroutine.
// TODO: remove GetTableStats later on.
func (h *Handle) GetTableStats(tblInfo *model.TableInfo) *statistics.Table {
	return h.GetPartitionStats(tblInfo, tblInfo.ID)
}

// GetPartitionStats retrieves the partition stats from cache.
// TODO: remove GetPartitionStats later on.
func (h *Handle) GetPartitionStats(tblInfo *model.TableInfo, pid int64) *statistics.Table {
	var tbl *statistics.Table
	if h == nil {
		tbl = statistics.PseudoTable(tblInfo, false)
		tbl.PhysicalID = pid
		return tbl
	}
	tbl, ok := h.Get(pid)
	if !ok {
		tbl = statistics.PseudoTable(tblInfo, false)
		tbl.PhysicalID = pid
		if tblInfo.GetPartitionInfo() == nil || h.Len() < 64 {
			h.UpdateStatsCache([]*statistics.Table{tbl}, nil)
		}
		return tbl
	}
	return tbl
}

// LoadNeededHistograms will load histograms for those needed columns/indices.
func (h *Handle) LoadNeededHistograms() (err error) {
	err = h.callWithSCtx(func(sctx sessionctx.Context) error {
		loadFMSketch := config.GetGlobalConfig().Performance.EnableLoadFMSketch
		return storage.LoadNeededHistograms(sctx, h.StatsCache, loadFMSketch)
	}, util.FlagWrapTxn)
	return err
}

// FlushStats flushes the cached stats update into store.
func (h *Handle) FlushStats() {
	for len(h.ddlEventCh) > 0 {
		e := <-h.ddlEventCh
		if err := h.HandleDDLEvent(e); err != nil {
			logutil.BgLogger().Error("handle ddl event fail", zap.String("category", "stats"), zap.Error(err))
		}
	}
	if err := h.DumpStatsDeltaToKV(true); err != nil {
		logutil.BgLogger().Error("dump stats delta fail", zap.String("category", "stats"), zap.Error(err))
	}
}

// TableStatsFromStorage loads table stats info from storage.
func (h *Handle) TableStatsFromStorage(tableInfo *model.TableInfo, physicalID int64, loadAll bool, snapshot uint64) (statsTbl *statistics.Table, err error) {
	err = h.callWithSCtx(func(sctx sessionctx.Context) error {
		var ok bool
		statsTbl, ok = h.Get(physicalID)
		if !ok {
			statsTbl = nil
		}
		statsTbl, err = storage.TableStatsFromStorage(sctx, snapshot, tableInfo, physicalID, loadAll, h.Lease(), statsTbl)
		return err
	}, util.FlagWrapTxn)
	return
}

// StatsMetaCountAndModifyCount reads count and modify_count for the given table from mysql.stats_meta.
func (h *Handle) StatsMetaCountAndModifyCount(tableID int64) (count, modifyCount int64, err error) {
	err = h.callWithSCtx(func(sctx sessionctx.Context) error {
		count, modifyCount, _, err = storage.StatsMetaCountAndModifyCount(sctx, tableID)
		return err
	}, util.FlagWrapTxn)
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
		h.RecordHistoricalStatsMeta(tableID, statsVer, source)
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
		h.RecordHistoricalStatsMeta(tableID, statsVer, source)
	}
	return
}

// InsertExtendedStats inserts a record into mysql.stats_extended and update version in mysql.stats_meta.
func (h *Handle) InsertExtendedStats(statsName string, colIDs []int64, tp int, tableID int64, ifNotExists bool) (err error) {
	var statsVer uint64
	err = h.callWithSCtx(func(sctx sessionctx.Context) error {
		statsVer, err = storage.InsertExtendedStats(sctx, h.StatsCache, statsName, colIDs, tp, tableID, ifNotExists)
		return err
	})
	if err == nil && statsVer != 0 {
		h.RecordHistoricalStatsMeta(tableID, statsVer, StatsMetaHistorySourceExtendedStats)
	}
	return
}

// MarkExtendedStatsDeleted update the status of mysql.stats_extended to be `deleted` and the version of mysql.stats_meta.
func (h *Handle) MarkExtendedStatsDeleted(statsName string, tableID int64, ifExists bool) (err error) {
	var statsVer uint64
	err = h.callWithSCtx(func(sctx sessionctx.Context) error {
		statsVer, err = storage.MarkExtendedStatsDeleted(sctx, h.StatsCache, statsName, tableID, ifExists)
		return err
	})
	if err == nil && statsVer != 0 {
		h.RecordHistoricalStatsMeta(tableID, statsVer, StatsMetaHistorySourceExtendedStats)
	}
	return
}

const updateStatsCacheRetryCnt = 5

// ReloadExtendedStatistics drops the cache for extended statistics and reload data from mysql.stats_extended.
// TODO: move this method to the `extstats` package.
func (h *Handle) ReloadExtendedStatistics() error {
	return h.callWithSCtx(func(sctx sessionctx.Context) error {
		tables := make([]*statistics.Table, 0, h.Len())
		for _, tbl := range h.Values() {
			t, err := storage.ExtendedStatsFromStorage(sctx, tbl.Copy(), tbl.PhysicalID, true)
			if err != nil {
				return err
			}
			tables = append(tables, t)
		}
		h.UpdateStatsCache(tables, nil)
		return nil
	}, util.FlagWrapTxn)
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
		statsVer, err = storage.SaveExtendedStatsToStorage(sctx, tableID, extStats, isLoad)
		return err
	})
	if err == nil && statsVer != 0 {
		h.RecordHistoricalStatsMeta(tableID, statsVer, StatsMetaHistorySourceExtendedStats)
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
	}, util.FlagWrapTxn)
	return version, err
}

// Close stops the background
func (h *Handle) Close() {
	h.gpool.Close()
	h.StatsCache.Close()
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
