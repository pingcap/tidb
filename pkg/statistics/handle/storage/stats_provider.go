// Copyright 2026 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package storage

import (
	"context"
	"sync"

	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/meta/metadef"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/sessionctx"
	"github.com/pingcap/tidb/pkg/statistics"
	statsutil "github.com/pingcap/tidb/pkg/statistics/handle/util"
	"github.com/pingcap/tidb/pkg/util"
	"github.com/pingcap/tidb/pkg/util/logutil"
	"github.com/pingcap/tidb/pkg/util/sqlkiller"
	"go.uber.org/zap"
)

// OnDemandStatsProvider loads physical table statistics on demand from
// mysql.stats_* system tables and memoizes them for the lifetime of a single
// SQL compilation or single-shot query execution.
//
// Missing statistics and storage load errors are permanently memoized as
// PseudoTable entries without invalidation or retry, so do not reuse one
// provider across long-lived or multi-query background loops that expect to
// observe newly analyzed statistics or recover from transient storage errors.
type OnDemandStatsProvider struct {
	ctx      context.Context
	sessPool util.DestroyableSessionPool

	mu    sync.Mutex
	cache map[int64]*statistics.Table
}

// NewOnDemandStatsProvider creates a scoped on-demand stats provider backed by
// sessPool and bound to ctx for cancellation propagation.
func NewOnDemandStatsProvider(
	ctx context.Context,
	sessPool util.DestroyableSessionPool,
) *OnDemandStatsProvider {
	if ctx == nil {
		ctx = context.Background()
	}
	return &OnDemandStatsProvider{
		ctx:      ctx,
		sessPool: sessPool,
		cache:    make(map[int64]*statistics.Table),
	}
}

// GetPhysicalTableStats returns cached statistics for physicalTableID, loading
// them on demand from mysql.stats_* on a miss and falling back to pseudo stats
// when unavailable, canceled, or on error. Missing and failed loads (except
// caller context cancellation) are memoized for the provider's lifetime.
func (p *OnDemandStatsProvider) GetPhysicalTableStats(physicalTableID int64, tblInfo *model.TableInfo) *statistics.Table {
	// Never query mysql.stats_* for reserved/system tables to avoid recursive
	// stats lookups when loading mysql.stats_* itself.
	if metadef.IsReservedID(physicalTableID) || p.ctx.Err() != nil {
		return newPseudoPhysicalTable(physicalTableID, tblInfo)
	}

	p.mu.Lock()
	defer p.mu.Unlock()

	if tbl, ok := p.cache[physicalTableID]; ok {
		return tbl
	}
	if p.ctx.Err() != nil {
		return newPseudoPhysicalTable(physicalTableID, tblInfo)
	}

	tbl, err := p.loadFromStorage(physicalTableID, tblInfo)
	if err != nil {
		if p.ctx.Err() != nil {
			return newPseudoPhysicalTable(physicalTableID, tblInfo)
		}
		logutil.BgLogger().Warn("failed to load table stats on demand, fallback to pseudo",
			zap.Int64("physicalTableID", physicalTableID),
			zap.String("table", tblInfo.Name.O),
			zap.Error(err))
	}
	if tbl == nil {
		tbl = newPseudoPhysicalTable(physicalTableID, tblInfo)
	}
	p.cache[physicalTableID] = tbl
	return tbl
}

func newPseudoPhysicalTable(physicalTableID int64, tblInfo *model.TableInfo) *statistics.Table {
	tbl := statistics.PseudoTable(tblInfo, false, true)
	tbl.PhysicalID = physicalTableID
	return tbl
}

func (p *OnDemandStatsProvider) loadFromStorage(
	physicalTableID int64,
	tblInfo *model.TableInfo,
) (_ *statistics.Table, err error) {
	if err = p.ctx.Err(); err != nil {
		return nil, err
	}
	res, err := p.sessPool.Get()
	if err != nil {
		return nil, err
	}
	auxSCtx := res.(sessionctx.Context)
	execCtx := kv.WithInternalSourceType(p.ctx, kv.InternalTxnStatsForegroundPriority)
	stop := context.AfterFunc(p.ctx, func() {
		auxSCtx.GetSessionVars().SQLKiller.SendKillSignal(sqlkiller.QueryInterrupted)
	})
	completed := false
	defer func() {
		stop()
		auxSCtx.RollbackTxn(statsutil.StatsCtx)
		if !completed || err != nil || p.ctx.Err() != nil {
			p.sessPool.Destroy(res)
			return
		}
		p.sessPool.Put(res)
	}()

	if err = p.ctx.Err(); err != nil {
		return nil, err
	}
	if _, _, err = statsutil.ExecRowsWithCtx(execCtx, auxSCtx, "BEGIN"); err != nil {
		return nil, err
	}

	// Read version and snapshot inside the same snapshot transaction so tables
	// analyzed under PREDICATE column mode without column/index histograms still
	// report IsAnalyzed() == true (matching StatsCache.Update).
	metaRows, _, err := statsutil.ExecRowsWithCtx(
		execCtx, auxSCtx,
		"select version, snapshot from mysql.stats_meta where table_id = %?",
		physicalTableID,
	)
	if err != nil {
		return nil, err
	}
	if err = p.ctx.Err(); err != nil {
		return nil, err
	}
	if len(metaRows) == 0 {
		completed = true
		return nil, nil
	}
	version := metaRows[0].GetUint64(0)
	snapshot := metaRows[0].GetUint64(1)

	// loadAll=false with lease=0 loads full column/index histograms, TopN, and
	// CMSketch in one shot while skipping mysql.stats_fm_sketch (which is only
	// needed when merging partition stats during ANALYZE).
	tbl, err := TableStatsFromStorage(auxSCtx, 0, tblInfo, physicalTableID, false, 0, nil)
	if err != nil {
		return nil, err
	}
	if err = p.ctx.Err(); err != nil {
		return nil, err
	}
	if tbl != nil {
		tbl.Version = version
		if tbl.LastAnalyzeVersion == 0 && snapshot != 0 {
			tbl.LastAnalyzeVersion = snapshot
		}
	}
	completed = true
	return tbl, nil
}
