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
	"time"

	"github.com/pingcap/tidb/pkg/ddl/notifier"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/session/syssession"
	"github.com/pingcap/tidb/pkg/sessionctx"
	"github.com/pingcap/tidb/pkg/sessionctx/sysproctrack"
	"github.com/pingcap/tidb/pkg/statistics"
	"github.com/pingcap/tidb/pkg/statistics/handle/autoanalyze"
	"github.com/pingcap/tidb/pkg/statistics/handle/cache"
	"github.com/pingcap/tidb/pkg/statistics/handle/ddl"
	"github.com/pingcap/tidb/pkg/statistics/handle/globalstats"
	"github.com/pingcap/tidb/pkg/statistics/handle/lockstats"
	statslogutil "github.com/pingcap/tidb/pkg/statistics/handle/logutil"
	"github.com/pingcap/tidb/pkg/statistics/handle/storage"
	"github.com/pingcap/tidb/pkg/statistics/handle/syncload"
	"github.com/pingcap/tidb/pkg/statistics/handle/types"
	"github.com/pingcap/tidb/pkg/statistics/handle/usage"
	"github.com/pingcap/tidb/pkg/statistics/handle/util"
	"github.com/pingcap/tidb/pkg/util/intest"
	"github.com/pingcap/tidb/pkg/util/sqlexec"
	"go.uber.org/zap"
)

const (
	// StatsOwnerKey is the stats owner path that is saved to etcd.
	StatsOwnerKey = "/tidb/stats/owner"
	// StatsPrompt is the prompt for stats owner manager.
	StatsPrompt = "stats"
)

// AttachStatsCollector attaches the stats collector for the session.
// this function is registered in BootstrapSession in pkg/session/session.go
var AttachStatsCollector = func(s sqlexec.SQLExecutor) sqlexec.SQLExecutor {
	return s
}

// DetachStatsCollector removes the stats collector for the session
// this function is registered in BootstrapSession in pkg/session/session.go
var DetachStatsCollector = func(s sqlexec.SQLExecutor) sqlexec.SQLExecutor {
	return s
}

// Handle can update stats info periodically.
//
//nolint:fieldalignment
type Handle struct {
	pool    util.Pool
	procIDs util.AutoAnalyzeProcIDGenerator
	lease   *util.LeaseHolder
	tables  util.TableInfoGetter

	locker        *lockstats.Locker
	gc            *storage.GC
	ddlHandler    *ddl.Handler
	syncLoad      *syncload.Loader
	autoAnalyze   *autoanalyze.Analyzer
	statsUsage    *usage.Tracker
	readWriter    *storage.ReadWriter
	cache         *cache.Cache
	analyzeHandle *AnalyzeHandle

	// tableStats gets the stats of tables. The getters of Handle wrap it, because a nil Handle
	// must still give pseudo stats.
	tableStats *tableStatsGetter

	InitStatsDone chan struct{}
}

// AnalyzeHandle is the part of the stats handle that an ANALYZE statement uses. It is a set of
// stats components, so a session can use another set, for example one without a stats cache.
type AnalyzeHandle struct {
	util.GoroutinePool
	types.PhysicalTableStatsGetter
	types.StatsCacheLoader
	types.StatsMetaReader
	types.AnalyzeResultSaver
	types.LockedTablesGetter
	types.PredicateColumnsGetter
	types.AnalyzeJobRecorder
	types.GlobalStatsMerger
}

// Pool returns the pools whose sessions and goroutines the stats components use.
func (h *Handle) Pool() util.Pool {
	return h.pool
}

// ProcIDs returns the generator of the process IDs of auto-analyze statements.
func (h *Handle) ProcIDs() util.AutoAnalyzeProcIDGenerator {
	return h.procIDs
}

// Tables returns the getter of the table infos that the stats components use.
func (h *Handle) Tables() util.TableInfoGetter {
	return h.tables
}

// Lease returns the stats lease.
func (h *Handle) Lease() time.Duration {
	return h.lease.Lease()
}

// SetLeaseForTest sets the stats lease.
func (h *Handle) SetLeaseForTest(lease time.Duration) {
	h.lease.SetLeaseForTest(lease)
}

// AutoAnalyze returns the component that analyzes the outdated tables in the background.
func (h *Handle) AutoAnalyze() *autoanalyze.Analyzer {
	return h.autoAnalyze
}

// Cache returns the component that keeps the stats of tables in memory.
func (h *Handle) Cache() *cache.Cache {
	return h.cache
}

// DDL returns the component that updates the stats for DDL events.
func (h *Handle) DDL() *ddl.Handler {
	return h.ddlHandler
}

// GC returns the component that removes the stats that are not needed any more.
func (h *Handle) GC() *storage.GC {
	return h.gc
}

// Locker returns the component that locks the stats of tables.
func (h *Handle) Locker() *lockstats.Locker {
	return h.locker
}

// Storage returns the component that reads and writes the stats in the storage.
func (h *Handle) Storage() *storage.ReadWriter {
	return h.readWriter
}

// SyncLoad returns the component that loads the stats that queries need.
func (h *Handle) SyncLoad() *syncload.Loader {
	return h.syncLoad
}

// Usage returns the component that tracks the table deltas and the usage of stats.
func (h *Handle) Usage() *usage.Tracker {
	return h.statsUsage
}

// AnalyzeHandle returns the stats components that ANALYZE uses.
func (h *Handle) AnalyzeHandle() *AnalyzeHandle {
	return h.analyzeHandle
}

// ClearForTest clears the stats cache, the DDL events and the session stats of the handle.
func (h *Handle) ClearForTest() {
	h.cache.Clear()
	for len(h.ddlHandler.DDLEventCh()) > 0 {
		<-h.ddlHandler.DDLEventCh()
	}
	h.statsUsage.ResetSessionStatsList()
	h.tableStats.resetSystemDBIDCache()
}

// GetSystemDBIDCacheLenForTest gets the length of systemDBIDCache, only for test.
func (h *Handle) GetSystemDBIDCacheLenForTest() int {
	return h.tableStats.systemDBIDCacheLen()
}

// NewHandle creates a Handle for update stats.
func NewHandle(
	ctx context.Context,
	lease time.Duration,
	pool syssession.Pool,
	tracker sysproctrack.Tracker,
	ddlNotifier *notifier.DDLNotifier,
	autoAnalyzeProcIDGetter func() uint64,
	releaseAutoAnalyzeProcID func(uint64),
) (*Handle, error) {
	statsPool := util.NewPool(pool)
	procIDs := util.NewGenerator(autoAnalyzeProcIDGetter, releaseAutoAnalyzeProcID)
	leaseHolder := util.NewLeaseHolder(lease)
	tables := util.NewTableInfoGetter()
	locker := lockstats.NewLocker(statsPool)
	statsCache, err := cache.New(statsPool, leaseHolder, tables)
	if err != nil {
		return nil, err
	}
	tableStats := newTableStatsGetter(statsPool, statsCache)
	readWriter := storage.NewReadWriter(statsPool, leaseHolder, tables, statsCache)
	autoAnalyze := autoanalyze.NewAnalyzer(ctx, statsPool, procIDs, tables, statsCache, tableStats, tracker, ddlNotifier)
	globalStats := globalstats.NewMerger(statsPool, tables, readWriter)
	statsUsage := usage.NewTracker(statsPool, leaseHolder, locker, tableStats)
	handle := &Handle{
		InitStatsDone: make(chan struct{}),
		pool:          statsPool,
		procIDs:       procIDs,
		lease:         leaseHolder,
		tables:        tables,
		cache:         statsCache,
		tableStats:    tableStats,
		locker:        locker,
		gc:            storage.NewGC(statsPool, leaseHolder, tables),
		readWriter:    readWriter,
		statsUsage:    statsUsage,
		syncLoad:      syncload.NewLoader(statsPool, leaseHolder, tables, statsCache),
		autoAnalyze:   autoAnalyze,
		ddlHandler:    ddl.NewHandler(),
		analyzeHandle: &AnalyzeHandle{
			GoroutinePool:            statsPool,
			PhysicalTableStatsGetter: tableStats,
			StatsCacheLoader:         statsCache,
			StatsMetaReader:          readWriter,
			AnalyzeResultSaver:       readWriter,
			LockedTablesGetter:       locker,
			PredicateColumnsGetter:   statsUsage,
			AnalyzeJobRecorder:       autoAnalyze,
			GlobalStatsMerger:        globalStats,
		},
	}
	if ddlNotifier != nil {
		// In test environments, we use a channel-based approach to handle DDL events.
		// This maintains compatibility with existing test cases that expect events to be delivered through channels.
		// In production, DDL events are handled by the notifier system instead.
		if !intest.InTest {
			ddlNotifier.RegisterHandler(notifier.StatsMetaHandlerID, handle.ddlHandler.HandleDDLEvent)
		}
	}
	return handle, nil
}

// GetPhysicalTableStats retrieves the statistics for a physical table from cache or creates a pseudo statistics table.
// physicalTableID can be a table ID or partition ID.
func (h *Handle) GetPhysicalTableStats(physicalTableID int64, tblInfo *model.TableInfo) *statistics.Table {
	tblStats, found := h.getStatsByPhysicalID(physicalTableID, tblInfo)
	intest.Assert(tblStats != nil, "stats should not be nil")
	intest.Assert(found, "stats should not be nil")
	return tblStats
}

// ReadColumnDistributionStats reads one column's metadata, TopN, and Histogram
// from one MVCC snapshot. Any read or decoding failure aborts the whole read.
// It does not update the statistics cache.
func (*Handle) ReadColumnDistributionStats(
	ctx context.Context,
	sctx sessionctx.Context,
	physicalTableID int64,
	colInfo *model.ColumnInfo,
) (*statistics.Column, error) {
	return storage.ReadColumnDistributionStats(
		ctx, sctx, physicalTableID, colInfo)
}

// GetNonPseudoPhysicalTableStats retrieves the statistics for a physical table from cache. Unlike
// GetPhysicalTableStats, it does not create pseudo stats, but it can return pseudo stats that are already cached.
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

	return h.tableStats.getStatsByPhysicalID(physicalTableID, tblInfo)
}

// FlushStats flushes the cached stats update into store.
func (h *Handle) FlushStats() {
	if err := h.statsUsage.DumpStatsDeltaToKV(true); err != nil {
		statslogutil.StatsLogger().Warn("dump stats delta fail", zap.Error(err))
	}
}

// StartWorker starts the background collector worker inside
func (h *Handle) StartWorker() {
	h.statsUsage.StartWorker()
}

// Close stops the background
func (h *Handle) Close() {
	h.pool.Close()
	h.cache.Close()
	h.statsUsage.Close()
	h.autoAnalyze.Close()
	h.tableStats.resetSystemDBIDCache()
}
