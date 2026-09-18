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
	"context"
	"sync"
	"time"

	"github.com/pingcap/failpoint"
	"github.com/pingcap/tidb/pkg/ddl/notifier"
	"github.com/pingcap/tidb/pkg/infoschema"
	"github.com/pingcap/tidb/pkg/meta/autoid"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/sessionctx"
	"github.com/pingcap/tidb/pkg/sessionctx/sysproctrack"
	"github.com/pingcap/tidb/pkg/statistics"
	"github.com/pingcap/tidb/pkg/statistics/handle/autoanalyze"
	"github.com/pingcap/tidb/pkg/statistics/handle/cache"
	"github.com/pingcap/tidb/pkg/statistics/handle/ddl"
	"github.com/pingcap/tidb/pkg/statistics/handle/globalstats"
	"github.com/pingcap/tidb/pkg/statistics/handle/history"
	"github.com/pingcap/tidb/pkg/statistics/handle/lockstats"
	statslogutil "github.com/pingcap/tidb/pkg/statistics/handle/logutil"
	"github.com/pingcap/tidb/pkg/statistics/handle/storage"
	"github.com/pingcap/tidb/pkg/statistics/handle/syncload"
	"github.com/pingcap/tidb/pkg/statistics/handle/types"
	"github.com/pingcap/tidb/pkg/statistics/handle/usage"
	"github.com/pingcap/tidb/pkg/statistics/handle/util"
	pkgutil "github.com/pingcap/tidb/pkg/util"
	"github.com/pingcap/tidb/pkg/util/filter"
	"github.com/pingcap/tidb/pkg/util/intest"
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
	// Pool is used to get a session or a goroutine to execute stats updating.
	util.Pool

	// AutoAnalyzeProcIDGenerator is used to generate auto analyze proc ID.
	util.AutoAnalyzeProcIDGenerator

	// LeaseGetter is used to get stats lease.
	util.LeaseGetter

	// initStatsCtx is a context specifically used for initStats.
	// It's not designed for concurrent use, so avoid using it in such scenarios.
	// Currently, it's only utilized within initStats, which is exclusively used during bootstrap.
	// Since bootstrap is a one-time operation, using this context remains safe.
	initStatsCtx sessionctx.Context

	// TableInfoGetter is used to fetch table meta info.
	util.TableInfoGetter

	// StatsGC is used to GC stats.
	types.StatsGC

	// StatsUsage is used to track the usage of column / index statistics.
	types.StatsUsage

	// StatsHistory is used to manage historical stats.
	types.StatsHistory

	// StatsAnalyze is used to handle auto-analyze and manage analyze jobs.
	types.StatsAnalyze

	// StatsSyncLoad is used to load stats syncly.
	types.StatsSyncLoad

	// StatsReadWriter is used to read/write stats from/to storage.
	types.StatsReadWriter

	// StatsLock is used to manage locked stats.
	types.StatsLock

	// StatsGlobal is used to manage global stats.
	types.StatsGlobal

	// DDL is used to handle ddl events.
	types.DDL

	// StatsCache ...
	types.StatsCache

	// systemDBIDCache caches the database IDs that are confirmed as system schemas to avoid repeated session usage.
	systemDBIDCache sync.Map

	InitStatsDone chan struct{}
}

// Clear the statsCache, only for test.
func (h *Handle) Clear() {
	h.StatsCache.Clear()
	for len(h.DDLEventCh()) > 0 {
		<-h.DDLEventCh()
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
func NewHandle(
	ctx context.Context,
	initStatsCtx sessionctx.Context,
	lease time.Duration,
	is infoschema.InfoSchema,
	pool pkgutil.DestroyableSessionPool,
	tracker sysproctrack.Tracker,
	ddlNotifier *notifier.DDLNotifier,
	autoAnalyzeProcIDGetter func() uint64,
	releaseAutoAnalyzeProcID func(uint64),
) (*Handle, error) {
	handle := &Handle{
		InitStatsDone:   make(chan struct{}),
		TableInfoGetter: util.NewTableInfoGetter(),
		StatsLock:       lockstats.NewStatsLock(pool),
	}
	handle.StatsGC = storage.NewStatsGC(handle)
	handle.StatsReadWriter = storage.NewStatsReadWriter(handle)

	handle.initStatsCtx = initStatsCtx
	statsCache, err := cache.NewStatsCacheImpl(handle)
	if err != nil {
		return nil, err
	}
	handle.Pool = util.NewPool(pool)
	handle.AutoAnalyzeProcIDGenerator = util.NewGenerator(autoAnalyzeProcIDGetter, releaseAutoAnalyzeProcID)
	handle.LeaseGetter = util.NewLeaseGetter(lease)
	handle.StatsCache = statsCache
	handle.StatsHistory = history.NewStatsHistory(handle)
	handle.StatsUsage = usage.NewStatsUsageImpl(handle)
	handle.StatsAnalyze = autoanalyze.NewStatsAnalyze(ctx, handle, tracker, ddlNotifier)
	handle.StatsSyncLoad = syncload.NewStatsSyncLoad(is, handle)
	handle.StatsGlobal = globalstats.NewStatsGlobal(handle)
	handle.DDL = ddl.NewDDLHandler(
		handle.StatsReadWriter,
		handle,
	)
	if ddlNotifier != nil {
		// In test environments, we use a channel-based approach to handle DDL events.
		// This maintains compatibility with existing test cases that expect events to be delivered through channels.
		// In production, DDL events are handled by the notifier system instead.
		if !intest.InTest {
			ddlNotifier.RegisterHandler(notifier.StatsMetaHandlerID, handle.DDL.HandleDDLEvent)
		}
	}
	return handle, nil
}

// GetPhysicalTableStats retrieves the statistics for a physical table from cache or creates a pseudo statistics table.
// physicalTableID can be a table ID or partition ID.
func (h *Handle) GetPhysicalTableStats(physicalTableID int64, tblInfo *model.TableInfo) *statistics.Table {
	tblStats, found := h.getStatsByPhysicalID(physicalTableID, tblInfo)
	intest.Assert(tblStats != nil, "stats shoud not be nil")
	intest.Assert(found, "stats shoud not be nil")
	return tblStats
}

// GetNonPseudoPhysicalTableStats retrieves the statistics for a physical table from cache, but it will not return pseudo.
// physicalTableID can be a table ID or partition ID.
// Note: this function may return nil if the table is not found in the cache.
func (h *Handle) GetNonPseudoPhysicalTableStats(physicalTableID int64) (*statistics.Table, bool) {
	return h.getStatsByPhysicalID(physicalTableID, nil)
}

func (h *Handle) getStatsByPhysicalID(physicalTableID int64, tblInfo *model.TableInfo) (*statistics.Table, bool) {
	if h == nil {
		if tblInfo != nil {
			tbl := statistics.PseudoTable(tblInfo, false, false)
			tbl.PhysicalID = physicalTableID
			return tbl, true
		}
		return nil, false
	}

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
		if h.SPool() == nil {
			skipSystemTableCheck = true
		}
		if skipSystemTableCheck {
			h.UpdateStatsCache(types.CacheUpdate{
				Updated: []*statistics.Table{tbl},
			})
			return tbl, true
		}
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
	se, err := h.SPool().Get()
	if err != nil {
		return false, err
	}
	defer func() {
		if err == nil { // only recycle when no error
			h.SPool().Put(se)
		} else {
			// Note: Otherwise, the session will be leaked.
			h.SPool().Destroy(se)
		}
	}()

	sctx := se.(sessionctx.Context)
	is := sctx.GetDomainInfoSchema().(infoschema.InfoSchema)
	db, ok := is.SchemaByID(dbID)
	// 1 is used for some unit tests where the database is not created but directly injected.
	intest.Assert(ok || dbID == 1, "cannot find db for table %d, dbID %d", physicalTableID, dbID)
	if ok && filter.IsSystemSchema(db.Name.L) {
		isSystemTable = true
	}
	if err != nil {
		intest.Assert(err == nil, "unexpected error: %v, tableID %d, dbID %d", err, physicalTableID, dbID)
		return false, err
	}
	if isSystemTable {
		h.systemDBIDCache.Store(dbID, struct{}{})
	}

	return isSystemTable, nil
}

// FlushStats flushes the cached stats update into store.
func (h *Handle) FlushStats() {
	if err := h.DumpStatsDeltaToKV(true); err != nil {
		statslogutil.StatsLogger().Error("dump stats delta fail", zap.Error(err))
	}
}

// StartWorker starts the background collector worker inside
func (h *Handle) StartWorker() {
	h.StatsUsage.StartWorker()
}

// Close stops the background
func (h *Handle) Close() {
	h.Pool.Close()
	h.StatsCache.Close()
	h.StatsUsage.Close()
	h.StatsAnalyze.Close()
	h.resetSystemDBIDCache()
}
