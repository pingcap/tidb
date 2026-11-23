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
	// Pool is used to get a session or a goroutine to execute stats updating.
	util.Pool

	// AutoAnalyzeProcIDGenerator is used to generate auto analyze proc ID.
	util.AutoAnalyzeProcIDGenerator

	// LeaseHolder is used to get stats lease.
	*util.LeaseHolder

	// TableInfoGetter is used to fetch table meta info.
	util.TableInfoGetter

	// StatsGC is used to GC stats.
	types.StatsGC

	// StatsUsage is used to track the usage of column / index statistics.
	types.StatsUsage

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
	types.StatsGlobal
}

// AnalyzeHandle returns the stats components that ANALYZE uses.
func (h *Handle) AnalyzeHandle() *AnalyzeHandle {
	return h.analyzeHandle
}

// Clear the statsCache, only for test.
func (h *Handle) Clear() {
	h.StatsCache.Clear()
	for len(h.DDLEventCh()) > 0 {
		<-h.DDLEventCh()
	}
	h.ResetSessionStatsList()
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
	statsLock := lockstats.NewStatsLock(pool)
	statsCache, err := cache.NewStatsCacheImpl(statsPool, leaseHolder, tables)
	if err != nil {
		return nil, err
	}
	tableStats := newTableStatsGetter(statsPool, statsCache)
	readWriter := storage.NewStatsReadWriter(statsPool, leaseHolder, tables, statsCache)
	statsAnalyze := autoanalyze.NewStatsAnalyze(ctx, statsPool, procIDs, tables, statsCache, tableStats, tracker, ddlNotifier)
	statsGlobal := globalstats.NewStatsGlobal(statsPool, tables, readWriter)
	statsUsage := usage.NewStatsUsageImpl(statsPool, leaseHolder, statsLock, tableStats)
	handle := &Handle{
		InitStatsDone:              make(chan struct{}),
		Pool:                       statsPool,
		AutoAnalyzeProcIDGenerator: procIDs,
		LeaseHolder:                leaseHolder,
		TableInfoGetter:            tables,
		StatsCache:                 statsCache,
		tableStats:                 tableStats,
		StatsLock:                  statsLock,
		StatsGC:                    storage.NewStatsGC(statsPool, leaseHolder, tables),
		StatsReadWriter:            readWriter,
		StatsGlobal:                statsGlobal,
		StatsUsage:                 statsUsage,
		StatsSyncLoad:              syncload.NewStatsSyncLoad(statsPool, leaseHolder, tables, statsCache),
		StatsAnalyze:               statsAnalyze,
		DDL:                        ddl.NewDDLHandler(),
		analyzeHandle: &AnalyzeHandle{
			GoroutinePool:            statsPool,
			PhysicalTableStatsGetter: tableStats,
			StatsCacheLoader:         statsCache,
			StatsMetaReader:          readWriter,
			AnalyzeResultSaver:       readWriter,
			LockedTablesGetter:       statsLock,
			PredicateColumnsGetter:   statsUsage,
			AnalyzeJobRecorder:       statsAnalyze,
			StatsGlobal:              statsGlobal,
		},
	}
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

	return h.tableStats.getStatsByPhysicalID(physicalTableID, tblInfo)
}

// FlushStats flushes the cached stats update into store.
func (h *Handle) FlushStats() {
	if err := h.DumpStatsDeltaToKV(true); err != nil {
		statslogutil.StatsLogger().Warn("dump stats delta fail", zap.Error(err))
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
	h.tableStats.resetSystemDBIDCache()
}
