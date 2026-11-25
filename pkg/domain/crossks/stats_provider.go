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

package crossks

import (
	"context"
	"sync"

	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/meta/metadef"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/sessionctx"
	"github.com/pingcap/tidb/pkg/statistics"
	statsstorage "github.com/pingcap/tidb/pkg/statistics/handle/storage"
	"github.com/pingcap/tidb/pkg/util"
	"github.com/pingcap/tidb/pkg/util/logutil"
	"go.uber.org/zap"
)

// OnDemandStatsProvider loads physical table statistics on demand from the
// target keyspace's mysql.stats_* system tables and caches them only for the
// lifetime of this provider (e.g. a single cross-keyspace SQL compilation or
// background task). It avoids running a resident StatsHandle per keyspace.
type OnDemandStatsProvider struct {
	sessPool util.DestroyableSessionPool

	mu    sync.Mutex
	cache map[int64]*statistics.Table
}

// NewOnDemandStatsProvider creates a scoped on-demand stats provider backed by
// the target keyspace's session pool.
func NewOnDemandStatsProvider(sessPool util.DestroyableSessionPool) *OnDemandStatsProvider {
	return &OnDemandStatsProvider{
		sessPool: sessPool,
		cache:    make(map[int64]*statistics.Table),
	}
}

// GetPhysicalTableStats returns cached statistics for physicalTableID, loading
// them on demand from mysql.stats_* on a miss and falling back to pseudo stats
// when unavailable or on error.
func (p *OnDemandStatsProvider) GetPhysicalTableStats(physicalTableID int64, tblInfo *model.TableInfo) *statistics.Table {
	if tblInfo == nil {
		return nil
	}
	// Never query mysql.stats_* for reserved/system tables to avoid recursive
	// stats lookups when loading mysql.stats_* itself.
	if metadef.IsReservedID(physicalTableID) {
		tbl := statistics.PseudoTable(tblInfo, false, true)
		tbl.PhysicalID = physicalTableID
		return tbl
	}

	p.mu.Lock()
	defer p.mu.Unlock()

	if tbl, ok := p.cache[physicalTableID]; ok {
		return tbl
	}

	tbl, err := p.loadFromStorage(physicalTableID, tblInfo)
	if err != nil {
		logutil.BgLogger().Warn("failed to load cross-keyspace table stats on demand, fallback to pseudo",
			zap.Int64("physicalTableID", physicalTableID),
			zap.String("table", tblInfo.Name.O),
			zap.Error(err))
	}
	if tbl == nil {
		tbl = statistics.PseudoTable(tblInfo, false, true)
		tbl.PhysicalID = physicalTableID
	}
	p.cache[physicalTableID] = tbl
	return tbl
}

func (p *OnDemandStatsProvider) loadFromStorage(physicalTableID int64, tblInfo *model.TableInfo) (*statistics.Table, error) {
	if p.sessPool == nil {
		return nil, nil
	}

	res, err := p.sessPool.Get()
	if err != nil {
		return nil, err
	}
	auxSCtx, ok := res.(sessionctx.Context)
	if !ok {
		p.sessPool.Put(res)
		return nil, nil
	}

	ctx := kv.WithInternalSourceType(context.Background(), kv.InternalTxnStats)
	defer func() {
		auxSCtx.RollbackTxn(ctx)
		p.sessPool.Put(res)
	}()

	if _, err := auxSCtx.GetSQLExecutor().ExecuteInternal(ctx, "BEGIN"); err != nil {
		return nil, err
	}
	// loadAll=true loads full histograms/TopN in one shot so the planner does
	// not rely on Domain's background StatsSyncLoad workers.
	return statsstorage.TableStatsFromStorage(auxSCtx, 0, tblInfo, physicalTableID, true, 0, nil)
}
