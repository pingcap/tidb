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

	"github.com/pingcap/tidb/pkg/infoschema"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/sessionctx"
	"github.com/pingcap/tidb/pkg/statistics"
	"github.com/pingcap/tidb/pkg/statistics/handle/autoanalyze"
	"github.com/pingcap/tidb/pkg/statistics/handle/globalstats"
	"github.com/pingcap/tidb/pkg/statistics/handle/lockstats"
	statslogutil "github.com/pingcap/tidb/pkg/statistics/handle/logutil"
	"github.com/pingcap/tidb/pkg/statistics/handle/storage"
	"github.com/pingcap/tidb/pkg/statistics/handle/usage"
	"github.com/pingcap/tidb/pkg/statistics/handle/util"
	"go.uber.org/zap"
)

// NewAnalyzeHandleWithoutCache creates the stats handle of ANALYZE in a session that has no stats
// cache, such as a session without a domain. Its components work on the storage in the sessions of
// pool, and ANALYZE reuses the goroutines of goroutines.
func NewAnalyzeHandleWithoutCache(pool util.SessionPool, goroutines util.GoroutinePool, lease util.LeaseGetter) *AnalyzeHandle {
	return &AnalyzeHandle{
		GoroutinePool:            goroutines,
		PhysicalTableStatsGetter: &storageTableStatsGetter{pool: pool, lease: lease},
		StatsCacheLoader:         noStatsCache{},
		StatsMetaReader:          storage.NewStatsMetaReader(pool),
		AnalyzeResultSaver:       storage.NewAnalyzeResultSaver(pool, lease),
		LockedTablesGetter:       lockstats.NewLocker(pool),
		PredicateColumnsGetter:   usage.NewPredicateColumnsGetter(pool),
		AnalyzeJobRecorder:       autoanalyze.NewAnalyzeJobRecorder(pool),
		GlobalStatsMerger: globalstats.NewMerger(
			pool, util.NewTableInfoGetter(), storage.NewColOrIdxStatsSaver(pool, lease)),
	}
}

// storageTableStatsGetter reads the stats of tables from the storage in the sessions of pool.
type storageTableStatsGetter struct {
	pool  util.SessionPool
	lease util.LeaseGetter
}

// GetPhysicalTableStats implements types.PhysicalTableStatsGetter. Like Handle, it returns pseudo
// stats when the storage has none.
func (g *storageTableStatsGetter) GetPhysicalTableStats(physicalTableID int64, tblInfo *model.TableInfo) *statistics.Table {
	var tbl *statistics.Table
	err := util.CallWithSCtx(g.pool.SPool(), func(sctx sessionctx.Context) error {
		var err error
		tbl, err = storage.TableStatsFromStorage(sctx, 0, tblInfo, physicalTableID, false, g.lease.Lease(), nil)
		return err
	}, util.FlagWrapTxn)
	if err != nil {
		statslogutil.StatsLogger().Warn("failed to read table stats", zap.Int64("physicalID", physicalTableID), zap.Error(err))
	}
	if tbl == nil {
		tbl = statistics.PseudoTable(tblInfo, false, true)
		tbl.PhysicalID = physicalTableID
	}
	return tbl
}

// noStatsCache is the stats cache loader of a handle without a stats cache. The node that
// dispatched the statement loads the new stats into its own cache.
type noStatsCache struct{}

// Update implements types.StatsCacheLoader.
func (noStatsCache) Update(context.Context, infoschema.InfoSchema, ...int64) error {
	return nil
}
