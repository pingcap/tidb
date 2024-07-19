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
	"time"

	"github.com/pingcap/tidb/pkg/parser/model"
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

	InitStatsDone chan struct{}

	// StatsCache ...
	types.StatsCache
}

// Clear the statsCache, only for test.
func (h *Handle) Clear() {
	h.StatsCache.Clear()
	for len(h.DDLEventCh()) > 0 {
		<-h.DDLEventCh()
	}
	h.ResetSessionStatsList()
}

// NewHandle creates a Handle for update stats.
func NewHandle(
	_, /* ctx, keep it for feature usage */
	initStatsCtx sessionctx.Context,
	lease time.Duration,
	pool util.SessionPool,
	tracker sysproctrack.Tracker,
	autoAnalyzeProcIDGetter func() uint64,
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
	handle.AutoAnalyzeProcIDGenerator = util.NewGenerator(autoAnalyzeProcIDGetter)
	handle.LeaseGetter = util.NewLeaseGetter(lease)
	handle.StatsCache = statsCache
	handle.StatsHistory = history.NewStatsHistory(handle)
	handle.StatsUsage = usage.NewStatsUsageImpl(handle)
	handle.StatsAnalyze = autoanalyze.NewStatsAnalyze(handle, tracker)
	handle.StatsSyncLoad = syncload.NewStatsSyncLoad(handle)
	handle.StatsGlobal = globalstats.NewStatsGlobal(handle)
	handle.DDL = ddl.NewDDLHandler(
		handle.StatsReadWriter,
		handle,
		handle.StatsGlobal,
	)
	return handle, nil
}

// GetTableStats retrieves the statistics table from cache, and the cache will be updated by a goroutine.
// TODO: remove GetTableStats later on.
func (h *Handle) GetTableStats(tblInfo *model.TableInfo) *statistics.Table {
	return h.GetPartitionStats(tblInfo, tblInfo.ID)
}

// GetTableStatsForAutoAnalyze is to get table stats but it will not return pseudo stats.
func (h *Handle) GetTableStatsForAutoAnalyze(tblInfo *model.TableInfo) *statistics.Table {
	return h.getPartitionStats(tblInfo, tblInfo.ID, false)
}

// GetPartitionStats retrieves the partition stats from cache.
// TODO: remove GetTableStats later on.
func (h *Handle) GetPartitionStats(tblInfo *model.TableInfo, pid int64) *statistics.Table {
	return h.getPartitionStats(tblInfo, pid, true)
}

// GetPartitionStatsForAutoAnalyze is to get partition stats but it will not return pseudo stats.
func (h *Handle) GetPartitionStatsForAutoAnalyze(tblInfo *model.TableInfo, pid int64) *statistics.Table {
	return h.getPartitionStats(tblInfo, pid, false)
}

func (h *Handle) getPartitionStats(tblInfo *model.TableInfo, pid int64, returnPseudo bool) *statistics.Table {
	var tbl *statistics.Table
	if h == nil {
		tbl = statistics.PseudoTable(tblInfo, false, false)
		tbl.PhysicalID = pid
		return tbl
	}
	tbl, ok := h.Get(pid)
	if !ok {
		if returnPseudo {
			tbl = statistics.PseudoTable(tblInfo, false, true)
			tbl.PhysicalID = pid
			if tblInfo.GetPartitionInfo() == nil || h.Len() < 64 {
				h.UpdateStatsCache([]*statistics.Table{tbl}, nil)
			}
			return tbl
		}
		return nil
	}
	return tbl
}

// FlushStats flushes the cached stats update into store.
func (h *Handle) FlushStats() {
	for len(h.DDLEventCh()) > 0 {
		e := <-h.DDLEventCh()
		if err := h.HandleDDLEvent(e); err != nil {
			statslogutil.StatsLogger().Error("handle ddl event fail", zap.Error(err))
		}
	}
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
}
