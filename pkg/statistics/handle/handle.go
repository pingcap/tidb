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
	"math"
	"time"

	"github.com/pingcap/tidb/pkg/config"
	ddlUtil "github.com/pingcap/tidb/pkg/ddl/util"
	"github.com/pingcap/tidb/pkg/infoschema"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/parser/model"
	"github.com/pingcap/tidb/pkg/sessionctx"
	"github.com/pingcap/tidb/pkg/sessionctx/stmtctx"
	"github.com/pingcap/tidb/pkg/statistics"
	"github.com/pingcap/tidb/pkg/statistics/handle/autoanalyze"
	"github.com/pingcap/tidb/pkg/statistics/handle/cache"
	"github.com/pingcap/tidb/pkg/statistics/handle/extstats"
	"github.com/pingcap/tidb/pkg/statistics/handle/globalstats"
	"github.com/pingcap/tidb/pkg/statistics/handle/history"
	"github.com/pingcap/tidb/pkg/statistics/handle/lockstats"
	"github.com/pingcap/tidb/pkg/statistics/handle/storage"
	"github.com/pingcap/tidb/pkg/statistics/handle/usage"
	"github.com/pingcap/tidb/pkg/statistics/handle/util"
	"github.com/pingcap/tidb/pkg/util/logutil"
	"github.com/tiancaiamao/gp"
	atomic2 "go.uber.org/atomic"
	"go.uber.org/zap"
)

const (
	// StatsOwnerKey is the stats owner path that is saved to etcd.
	StatsOwnerKey = "/tidb/stats/owner"
	// StatsPrompt is the prompt for stats owner manager.
	StatsPrompt = "stats"
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

	// StatsReadWriter is used to read/write stats from/to storage.
	util.StatsReadWriter

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
		StatsLock:               lockstats.NewStatsLock(pool),
	}
	handle.StatsGC = storage.NewStatsGC(handle)
	handle.StatsReadWriter = storage.NewStatsReadWriter(handle)

	handle.initStatsCtx = initStatsCtx
	handle.lease.Store(lease)
	statsCache, err := cache.NewStatsCacheImpl(handle)
	if err != nil {
		return nil, err
	}
	handle.StatsCache = statsCache
	handle.StatsHistory = history.NewStatsHistory(handle, handle.tableStatsToJSON, handle.DumpStatsToJSON)
	handle.StatsUsage = usage.NewStatsUsageImpl(handle)
	handle.StatsAnalyze = autoanalyze.NewStatsAnalyze(handle)
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

// BuildExtendedStats build extended stats for column groups if needed based on the column samples.
func (h *Handle) BuildExtendedStats(tableID int64, cols []*model.ColumnInfo, collectors []*statistics.SampleCollector) (es *statistics.ExtendedStatsColl, err error) {
	err = h.callWithSCtx(func(sctx sessionctx.Context) error {
		es, err = extstats.BuildExtendedStats(sctx, tableID, cols, collectors)
		return err
	})
	return es, err
}

// Close stops the background
func (h *Handle) Close() {
	h.gpool.Close()
	h.StatsCache.Close()
}

func (h *Handle) callWithSCtx(f func(sctx sessionctx.Context) error, flags ...int) (err error) {
	return util.CallWithSCtx(h.pool, f, flags...)
}

// GetCurrentPruneMode returns the current latest partitioning table prune mode.
func (h *Handle) GetCurrentPruneMode() (mode string, err error) {
	err = h.callWithSCtx(func(sctx sessionctx.Context) error {
		mode = sctx.GetSessionVars().PartitionPruneMode.Load()
		return nil
	})
	return
}

// GPool returns the goroutine pool of handle.
func (h *Handle) GPool() *gp.Pool {
	return h.gpool
}

// SPool returns the session pool.
func (h *Handle) SPool() util.SessionPool {
	return h.pool
}

// SysProcTracker is used to track sys process like analyze
func (h *Handle) SysProcTracker() sessionctx.SysProcTracker {
	return h.sysProcTracker
}

// AutoAnalyzeProcID generates an analyze ID.
func (h *Handle) AutoAnalyzeProcID() uint64 {
	return h.autoAnalyzeProcIDGetter()
}
