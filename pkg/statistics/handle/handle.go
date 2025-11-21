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
<<<<<<< HEAD
	"math"
	"time"

	"github.com/pingcap/tidb/pkg/config"
	ddlUtil "github.com/pingcap/tidb/pkg/ddl/util"
	"github.com/pingcap/tidb/pkg/parser/model"
	"github.com/pingcap/tidb/pkg/sessionctx"
=======
	"context"
	"sync"
	"time"

	"github.com/pingcap/failpoint"
	"github.com/pingcap/tidb/pkg/ddl/notifier"
	"github.com/pingcap/tidb/pkg/meta/autoid"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/session/syssession"
	"github.com/pingcap/tidb/pkg/sessionctx"
	"github.com/pingcap/tidb/pkg/sessionctx/sysproctrack"
>>>>>>> 9f3ae48f30b (statistics: ignore system tables in stats cache (#64097))
	"github.com/pingcap/tidb/pkg/statistics"
	"github.com/pingcap/tidb/pkg/statistics/handle/autoanalyze"
	"github.com/pingcap/tidb/pkg/statistics/handle/cache"
	"github.com/pingcap/tidb/pkg/statistics/handle/globalstats"
	"github.com/pingcap/tidb/pkg/statistics/handle/history"
	"github.com/pingcap/tidb/pkg/statistics/handle/lockstats"
	statslogutil "github.com/pingcap/tidb/pkg/statistics/handle/logutil"
	"github.com/pingcap/tidb/pkg/statistics/handle/storage"
	"github.com/pingcap/tidb/pkg/statistics/handle/usage"
	"github.com/pingcap/tidb/pkg/statistics/handle/util"
<<<<<<< HEAD
	"github.com/tiancaiamao/gp"
	atomic2 "go.uber.org/atomic"
=======
	"github.com/pingcap/tidb/pkg/util/filter"
	"github.com/pingcap/tidb/pkg/util/intest"
	"github.com/pingcap/tidb/pkg/util/sqlexec"
>>>>>>> 9f3ae48f30b (statistics: ignore system tables in stats cache (#64097))
	"go.uber.org/zap"
)

const (
	// StatsOwnerKey is the stats owner path that is saved to etcd.
	StatsOwnerKey = "/tidb/stats/owner"
	// StatsPrompt is the prompt for stats owner manager.
	StatsPrompt = "stats"
)

// Handle can update stats info periodically.
//
//nolint:fieldalignment
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

	// StatsGlobal is used to manage global stats.
	util.StatsGlobal

	// This gpool is used to reuse goroutine in the mergeGlobalStatsTopN.
	gpool *gp.Pool

	// autoAnalyzeProcIDGetter is used to generate auto analyze ID.
	autoAnalyzeProcIDGetter func() uint64

<<<<<<< HEAD
	InitStatsDone chan struct{}

	// ddlEventCh is a channel to notify a ddl operation has happened.
	// It is sent only by owner or the drop stats executor, and read by stats handle.
	ddlEventCh chan *ddlUtil.Event

	// StatsCache ...
	util.StatsCache

	// StatsLoad is used to load stats concurrently
	StatsLoad StatsLoad

	lease atomic2.Duration
=======
	// StatsCache ...
	types.StatsCache

	// systemDBIDCache caches the database IDs that are confirmed as system schemas to avoid repeated session usage.
	systemDBIDCache sync.Map

	InitStatsDone chan struct{}
>>>>>>> 9f3ae48f30b (statistics: ignore system tables in stats cache (#64097))
}

// Clear the statsCache, only for test.
func (h *Handle) Clear() {
	h.StatsCache.Clear()
	for len(h.ddlEventCh) > 0 {
		<-h.ddlEventCh
	}
	h.ResetSessionStatsList()
	h.resetSystemDBIDCache()
}

func (h *Handle) resetSystemDBIDCache() {
	h.systemDBIDCache.Clear()
}

// GetSystemDBIDCacheLenForTest gets the length of systemDBIDCache, only for test.
func (h *Handle) GetSystemDBIDCacheLenForTest() int {
	length := 0
	h.systemDBIDCache.Range(func(_, _ any) bool {
		length++
		return true
	})
	return length
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
	handle.StatsHistory = history.NewStatsHistory(handle)
	handle.StatsUsage = usage.NewStatsUsageImpl(handle)
	handle.StatsAnalyze = autoanalyze.NewStatsAnalyze(handle)
	handle.StatsGlobal = globalstats.NewStatsGlobal(handle)
	handle.StatsLoad.SubCtxs = make([]sessionctx.Context, cfg.Performance.StatsLoadConcurrency)
	handle.StatsLoad.NeededItemsCh = make(chan *NeededItemTask, cfg.Performance.StatsLoadQueueSize)
	handle.StatsLoad.TimeoutItemsCh = make(chan *NeededItemTask, cfg.Performance.StatsLoadQueueSize)
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
<<<<<<< HEAD
	tbl, ok := h.Get(pid)
	if !ok {
		tbl = statistics.PseudoTable(tblInfo, false)
		tbl.PhysicalID = pid
		if tblInfo.GetPartitionInfo() == nil || h.Len() < 64 {
			h.UpdateStatsCache(util.CacheUpdate{
=======

	tbl, ok := h.Get(physicalTableID)
	if ok {
		return tbl, true
	}
	if tblInfo == nil {
		return nil, false
	}

	tbl = statistics.PseudoTable(tblInfo, false, true)
	tbl.PhysicalID = physicalTableID

	// TODO: Determine whether we really need to cache pseudo table stats for non-partitioned tables.
	// If the memory overhead is manageable, we can remove this optimization.
	shouldCachePseudo := tblInfo.GetPartitionInfo() == nil || h.Len() < 64
	if !shouldCachePseudo {
		return tbl, true
	}

	// NOTE: Sessions borrowed from the pool cannot fetch schema metadata for local temporary tables,
	// so skip caching their statistics.
	// Also skip global temporary tables for consistency.
	isTempTable := tblInfo.TempTableType != model.TempTableNone
	if isTempTable {
		return tbl, true
	}

	// In some test cases, we may need to skip the system table check.
	if intest.InTest {
		// The failpoint to skip system table check, for testing only.
		skipSystemTableCheck := false
		failpoint.Inject("SkipSystemTableCheck", func(val failpoint.Value) {
			skip, ok := val.(bool)
			if ok && skip {
				skipSystemTableCheck = true
			}
		})

		// In some test environments, the session pool may be nil.
		// In such cases, we cannot determine if it's a system table, so we skip the check.
		if se, ok := h.SPool().(*syssession.AdvancedSessionPool); ok && se == nil {
			skipSystemTableCheck = true
		}
		if skipSystemTableCheck {
			h.UpdateStatsCache(types.CacheUpdate{
>>>>>>> 9f3ae48f30b (statistics: ignore system tables in stats cache (#64097))
				Updated: []*statistics.Table{tbl},
			})
			return tbl, true
		}
<<<<<<< HEAD
		return tbl
	}
	return tbl
=======
	}

	isSystemTable, err := h.isSystemTable(physicalTableID, tblInfo)
	if err != nil {
		dbID := tblInfo.DBID
		statslogutil.StatsErrVerboseSampleLogger().Warn("Check system table failed", zap.Int64("tableID", physicalTableID), zap.Int64("dbID", dbID), zap.Error(err))
		return tbl, true
	}

	if isSystemTable {
		return tbl, true
	}

	h.UpdateStatsCache(types.CacheUpdate{
		Updated: []*statistics.Table{tbl},
	})
	return tbl, true
}

// isSystemTable determines whether the table should be treated as a system table.
// NOTE: You might worry that this slows down Get. It runs only once per non-partitioned table, or once per partition when the cache holds fewer than 64 entries, so the impact is negligible.
// Stats healthy metrics almost never show pseudo tables, because once a DDL event is processed or the table is updated, real statistics are loaded into the cache.
// We also cache the database IDs of system schemas to avoid repeated session usage.
func (h *Handle) isSystemTable(physicalTableID int64, tblInfo *model.TableInfo) (bool, error) {
	intest.Assert(tblInfo != nil, "tblInfo should not be nil for tableID %d", physicalTableID)
	dbID := tblInfo.DBID
	intest.Assert(dbID > 0, "invalid dbID %d for tableID %d", dbID, physicalTableID)
	if autoid.IsMemSchemaID(dbID) {
		return true, nil
	}

	if _, ok := h.systemDBIDCache.Load(dbID); ok {
		return true, nil
	}

	isSystemTable := false
	err := h.SPool().WithSession(func(session *syssession.Session) error {
		return session.WithSessionContext(func(sctx sessionctx.Context) error {
			is := sctx.GetLatestInfoSchema()
			db, ok := is.SchemaByID(dbID)
			// 1 is used for some unit tests where the database is not created but directly injected.
			intest.Assert(ok || dbID == 1, "cannot find db for table %d, dbID %d", physicalTableID, dbID)
			if ok && filter.IsSystemSchema(db.Name.L) {
				isSystemTable = true
			}
			return nil
		})
	})
	if err != nil {
		intest.Assert(err == nil, "unexpected error: %v, tableID %d, dbID %d", err, physicalTableID, dbID)
		return false, err
	}
	if isSystemTable {
		h.systemDBIDCache.Store(dbID, struct{}{})
	}

	return isSystemTable, nil
>>>>>>> 9f3ae48f30b (statistics: ignore system tables in stats cache (#64097))
}

// FlushStats flushes the cached stats update into store.
func (h *Handle) FlushStats() {
	for len(h.ddlEventCh) > 0 {
		e := <-h.ddlEventCh
		if err := h.HandleDDLEvent(e); err != nil {
			statslogutil.StatsLogger().Error("handle ddl event fail", zap.Error(err))
		}
	}
	if err := h.DumpStatsDeltaToKV(true); err != nil {
		statslogutil.StatsLogger().Error("dump stats delta fail", zap.Error(err))
	}
}

// Close stops the background
func (h *Handle) Close() {
	h.gpool.Close()
	h.StatsCache.Close()
<<<<<<< HEAD
}

// GetCurrentPruneMode returns the current latest partitioning table prune mode.
func (h *Handle) GetCurrentPruneMode() (mode string, err error) {
	err = util.CallWithSCtx(h.pool, func(sctx sessionctx.Context) error {
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
=======
	h.StatsUsage.Close()
	h.StatsAnalyze.Close()
	h.resetSystemDBIDCache()
>>>>>>> 9f3ae48f30b (statistics: ignore system tables in stats cache (#64097))
}
