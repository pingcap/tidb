// Copyright 2026 PingCAP, Inc.
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

	"github.com/pingcap/tidb/pkg/infoschema"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/session/syssession"
	"github.com/pingcap/tidb/pkg/sessionctx"
	"github.com/pingcap/tidb/pkg/statistics"
	"github.com/pingcap/tidb/pkg/statistics/handle/autoanalyze"
	"github.com/pingcap/tidb/pkg/statistics/handle/globalstats"
	"github.com/pingcap/tidb/pkg/statistics/handle/lockstats"
	statslogutil "github.com/pingcap/tidb/pkg/statistics/handle/logutil"
	"github.com/pingcap/tidb/pkg/statistics/handle/storage"
	"github.com/pingcap/tidb/pkg/statistics/handle/types"
	"github.com/pingcap/tidb/pkg/statistics/handle/usage"
	"github.com/pingcap/tidb/pkg/statistics/handle/util"
	"github.com/tiancaiamao/gp"
	"go.uber.org/zap"
)

var (
	_ types.AnalyzeStatsHandle = (*Handle)(nil)
	_ types.AnalyzeStatsHandle = (*AnalyzeHandle)(nil)
)

// AnalyzeHandle is the stats handle of one ANALYZE statement in a session without a domain.
// It has no cache and no background worker, and works on the storage through its session pool.
type AnalyzeHandle struct {
	util.TableInfoGetter
	readWriter types.StatsReadWriter
	lock       types.StatsLock
	global     types.StatsGlobal
	pool       syssession.Pool
	gpool      *gp.Pool
	lease      time.Duration
}

// NewAnalyzeHandle creates the stats handle of one statement, which works on the sessions of pool.
func NewAnalyzeHandle(pool syssession.Pool, gpool *gp.Pool, lease time.Duration) *AnalyzeHandle {
	h := &AnalyzeHandle{
		TableInfoGetter: util.NewTableInfoGetter(),
		lock:            lockstats.NewStatsLock(pool),
		pool:            pool,
		gpool:           gpool,
		lease:           lease,
	}
	h.readWriter = storage.NewStatsReadWriter(h)
	h.global = globalstats.NewStatsGlobal(h)
	return h
}

// SPool implements util.Pool.
func (h *AnalyzeHandle) SPool() syssession.Pool {
	return h.pool
}

// GPool implements util.Pool.
func (h *AnalyzeHandle) GPool() *gp.Pool {
	return h.gpool
}

// Lease implements types.ReadWriterHandle.
func (h *AnalyzeHandle) Lease() time.Duration {
	return h.lease
}

// Get implements types.ReadWriterHandle. Without a cache, statistics are always read from the storage.
func (*AnalyzeHandle) Get(int64) (*statistics.Table, bool) {
	return nil, false
}

// UpdateStatsCache implements types.ReadWriterHandle.
func (*AnalyzeHandle) UpdateStatsCache(types.CacheUpdate) {}

// Update implements types.AnalyzeStatsHandle. The node that dispatched the statement loads the new statistics.
func (*AnalyzeHandle) Update(context.Context, infoschema.InfoSchema, ...int64) error {
	return nil
}

// GetPhysicalTableStats implements types.AnalyzeStatsHandle. It reads the statistics from
// the storage, and returns pseudo statistics like Handle when there are none.
func (h *AnalyzeHandle) GetPhysicalTableStats(physicalTableID int64, tblInfo *model.TableInfo) *statistics.Table {
	tbl, err := h.readWriter.TableStatsFromStorage(tblInfo, physicalTableID, false, 0)
	if err != nil {
		statslogutil.StatsLogger().Warn("failed to read table stats", zap.Int64("physicalID", physicalTableID), zap.Error(err))
	}
	if tbl == nil {
		tbl = statistics.PseudoTable(tblInfo, false, true)
		tbl.PhysicalID = physicalTableID
	}
	return tbl
}

// StatsMetaCountAndModifyCount implements types.AnalyzeStatsHandle.
func (h *AnalyzeHandle) StatsMetaCountAndModifyCount(tableID int64) (count, modifyCount int64, err error) {
	return h.readWriter.StatsMetaCountAndModifyCount(tableID)
}

// SaveAnalyzeResultToStorage implements types.AnalyzeStatsHandle.
func (h *AnalyzeHandle) SaveAnalyzeResultToStorage(results *statistics.AnalyzeResults, analyzeSnapshot bool) error {
	return h.readWriter.SaveAnalyzeResultToStorage(results, analyzeSnapshot)
}

// SaveColOrIdxStatsToStorage implements types.GlobalStatsHandle.
func (h *AnalyzeHandle) SaveColOrIdxStatsToStorage(tableID int64, count, modifyCount int64, isIndex int, hg *statistics.Histogram,
	cms *statistics.CMSketch, topN *statistics.TopN, fms *statistics.FMSketch, statsVersion int, updateAnalyzeTime bool) error {
	return h.readWriter.SaveColOrIdxStatsToStorage(tableID, count, modifyCount, isIndex, hg, cms, topN, fms, statsVersion, updateAnalyzeTime)
}

// GetLockedTables implements types.AnalyzeStatsHandle.
func (h *AnalyzeHandle) GetLockedTables(tableIDs ...int64) (map[int64]struct{}, error) {
	return h.lock.GetLockedTables(tableIDs...)
}

// GetPredicateColumns implements types.AnalyzeStatsHandle.
func (h *AnalyzeHandle) GetPredicateColumns(tableID int64) (columnIDs []int64, err error) {
	return usage.GetPredicateColumns(h.pool, tableID)
}

// MergePartitionStats2GlobalStatsByTableID implements types.AnalyzeStatsHandle.
func (h *AnalyzeHandle) MergePartitionStats2GlobalStatsByTableID(sc sessionctx.Context,
	opts map[ast.AnalyzeOptionType]uint64, is infoschema.InfoSchema, info *types.GlobalStatsInfo, physicalID int64) error {
	return h.global.MergePartitionStats2GlobalStatsByTableID(sc, opts, is, info, physicalID)
}

// InsertAnalyzeJob implements types.AnalyzeStatsHandle.
func (h *AnalyzeHandle) InsertAnalyzeJob(job *statistics.AnalyzeJob, instance string, procID uint64) error {
	return autoanalyze.InsertAnalyzeJob(h.pool, job, instance, procID)
}

// StartAnalyzeJob implements types.AnalyzeStatsHandle.
func (h *AnalyzeHandle) StartAnalyzeJob(job *statistics.AnalyzeJob) {
	autoanalyze.StartAnalyzeJob(h.pool, job)
}

// UpdateAnalyzeJobProgress implements types.AnalyzeStatsHandle.
func (h *AnalyzeHandle) UpdateAnalyzeJobProgress(job *statistics.AnalyzeJob, rowCount int64) {
	autoanalyze.UpdateAnalyzeJobProgress(h.pool, job, rowCount)
}

// FinishAnalyzeJob implements types.AnalyzeStatsHandle.
func (h *AnalyzeHandle) FinishAnalyzeJob(job *statistics.AnalyzeJob, failReason error, analyzeType statistics.JobType) {
	autoanalyze.FinishAnalyzeJob(h.pool, job, failReason, analyzeType)
}
