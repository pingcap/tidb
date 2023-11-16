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

	"github.com/pingcap/tidb/pkg/sessionctx"
	"github.com/pingcap/tidb/pkg/statistics/handle/autoanalyze"
	"github.com/pingcap/tidb/pkg/statistics/handle/cache"
	"github.com/pingcap/tidb/pkg/statistics/handle/ddl"
	"github.com/pingcap/tidb/pkg/statistics/handle/globalstats"
	"github.com/pingcap/tidb/pkg/statistics/handle/history"
	"github.com/pingcap/tidb/pkg/statistics/handle/lockstats"
	statslogutil "github.com/pingcap/tidb/pkg/statistics/handle/logutil"
	"github.com/pingcap/tidb/pkg/statistics/handle/storage"
	"github.com/pingcap/tidb/pkg/statistics/handle/syncload"
	"github.com/pingcap/tidb/pkg/statistics/handle/usage"
	"github.com/pingcap/tidb/pkg/statistics/handle/util"
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
	util.Pool
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

	// StatsSyncLoad is used to load stats syncly.
	util.StatsSyncLoad

	// StatsReadWriter is used to read/write stats from/to storage.
	util.StatsReadWriter

	// StatsLock is used to manage locked stats.
	util.StatsLock

	// StatsGlobal is used to manage global stats.
	util.StatsGlobal

	// DDL is used to handle ddl events.
	util.DDL

	// autoAnalyzeProcIDGetter is used to generate auto analyze ID.
	autoAnalyzeProcIDGetter func() uint64

	InitStatsDone chan struct{}

	// StatsCache ...
	util.StatsCache

	lease atomic2.Duration
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
	_,
	initStatsCtx sessionctx.Context,
	lease time.Duration,
	pool util.SessionPool,
	tracker sessionctx.SysProcTracker,
	autoAnalyzeProcIDGetter func() uint64,
) (*Handle, error) {
	handle := &Handle{
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
	handle.Pool = util.NewPool(pool)
	handle.StatsCache = statsCache
	handle.StatsHistory = history.NewStatsHistory(handle)
	handle.StatsUsage = usage.NewStatsUsageImpl(handle)
	handle.StatsAnalyze = autoanalyze.NewStatsAnalyze(handle)
	handle.StatsSyncLoad = syncload.NewStatsSyncLoad(handle)
	handle.StatsGlobal = globalstats.NewStatsGlobal(handle)
	handle.DDL = ddl.NewDDLHandler(handle.StatsReadWriter, handle, handle.StatsGlobal)
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

// FlushStats flushes the cached stats update into store.
func (h *Handle) FlushStats() {
	for len(h.DDLEventCh()) > 0 {
		e := <-h.DDLEventCh()
		if err := h.HandleDDLEvent(e); err != nil {
			statslogutil.StatsLogger.Error("handle ddl event fail", zap.Error(err))
		}
	}
	if err := h.DumpStatsDeltaToKV(true); err != nil {
		statslogutil.StatsLogger.Error("dump stats delta fail", zap.Error(err))
	}
}

// Close stops the background
func (h *Handle) Close() {
	h.Pool.Close()
	h.StatsCache.Close()
}

// GetCurrentPruneMode returns the current latest partitioning table prune mode.
func (h *Handle) GetCurrentPruneMode() (mode string, err error) {
	err = util.CallWithSCtx(h.SPool(), func(sctx sessionctx.Context) error {
		mode = sctx.GetSessionVars().PartitionPruneMode.Load()
		return nil
	})
	return
}

// SysProcTracker is used to track sys process like analyze
func (h *Handle) SysProcTracker() sessionctx.SysProcTracker {
	return h.sysProcTracker
}

// AutoAnalyzeProcID generates an analyze ID.
func (h *Handle) AutoAnalyzeProcID() uint64 {
	return h.autoAnalyzeProcIDGetter()
}
