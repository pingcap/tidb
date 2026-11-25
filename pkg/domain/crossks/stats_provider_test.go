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

package crossks_test

import (
	"context"
	"sync/atomic"
	"testing"

	"github.com/ngaut/pools"
	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/pkg/domain/crossks"
	"github.com/pingcap/tidb/pkg/meta/metadef"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/parser"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/planner"
	"github.com/pingcap/tidb/pkg/planner/core/resolve"
	plannerstats "github.com/pingcap/tidb/pkg/planner/core/stats"
	"github.com/pingcap/tidb/pkg/sessionctx/variable"
	"github.com/pingcap/tidb/pkg/testkit"
	"github.com/pingcap/tidb/pkg/util"
	"github.com/pingcap/tidb/pkg/util/mock"
	"github.com/stretchr/testify/require"
)

var _ plannerstats.PhysicalTableStatsProvider = (*crossks.OnDemandStatsProvider)(nil)

type countingSessionPool struct {
	util.DestroyableSessionPool
	getCount atomic.Int64
	putCount atomic.Int64
	getErr   error
}

func (p *countingSessionPool) Get() (pools.Resource, error) {
	p.getCount.Add(1)
	if p.getErr != nil {
		return nil, p.getErr
	}
	return p.DestroyableSessionPool.Get()
}

func (p *countingSessionPool) Put(r pools.Resource) {
	p.putCount.Add(1)
	p.DestroyableSessionPool.Put(r)
}

func TestOnDemandStatsProviderLoadsAndCachesStats(t *testing.T) {
	store, dom := testkit.CreateMockStoreAndDomain(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("create table t (id int primary key, a int, b int, key idx_a(a))")
	tk.MustExec("insert into t values (1, 10, 100), (2, 10, 200), (3, 20, 300), (4, 30, 400)")
	tk.MustExec("analyze table t")

	tk.MustExec(`create table tp (id int, a int, key idx_a(a))
		partition by range (id) (
			partition p0 values less than (10),
			partition p1 values less than (20)
		)`)
	tk.MustExec("insert into tp values (1, 1), (2, 2), (11, 11), (12, 12)")
	tk.MustExec("set @@tidb_partition_prune_mode = 'dynamic'")
	tk.MustExec("analyze table tp")

	is := dom.InfoSchema()
	tblT, err := is.TableByName(context.Background(), ast.NewCIStr("test"), ast.NewCIStr("t"))
	require.NoError(t, err)
	tblTP, err := is.TableByName(context.Background(), ast.NewCIStr("test"), ast.NewCIStr("tp"))
	require.NoError(t, err)

	spyPool := &countingSessionPool{DestroyableSessionPool: dom.SysSessionPool()}
	provider := crossks.NewOnDemandStatsProvider(spyPool)

	// First lookup hits storage once and returns real analyzed stats.
	gotT := provider.GetPhysicalTableStats(tblT.Meta().ID, tblT.Meta())
	require.NotNil(t, gotT)
	require.False(t, gotT.Pseudo)
	require.True(t, gotT.IsAnalyzed())
	require.Equal(t, int64(4), gotT.RealtimeCount)
	require.Equal(t, int64(1), spyPool.getCount.Load())
	require.Equal(t, int64(1), spyPool.putCount.Load())

	// Second lookup hits the provider cache without borrowing another session.
	gotTCached := provider.GetPhysicalTableStats(tblT.Meta().ID, tblT.Meta())
	require.Same(t, gotT, gotTCached)
	require.Equal(t, int64(1), spyPool.getCount.Load())

	// Reserved/system table IDs return pseudo stats without querying storage.
	sysTblInfo := &model.TableInfo{ID: metadef.TiDBImportJobsTableID, Name: ast.NewCIStr("tidb_import_jobs")}
	gotSys := provider.GetPhysicalTableStats(sysTblInfo.ID, sysTblInfo)
	require.True(t, gotSys.Pseudo)
	require.Equal(t, sysTblInfo.ID, gotSys.PhysicalID)
	require.Equal(t, int64(1), spyPool.getCount.Load())

	// Missing table stats gracefully fall back to pseudo stats.
	missingTblInfo := &model.TableInfo{ID: 99999999, Name: ast.NewCIStr("missing")}
	gotMissing := provider.GetPhysicalTableStats(missingTblInfo.ID, missingTblInfo)
	require.NotNil(t, gotMissing)
	require.True(t, gotMissing.Pseudo)
	require.Equal(t, missingTblInfo.ID, gotMissing.PhysicalID)

	// Failing pool falls back cleanly to pseudo stats.
	errPoolProvider := crossks.NewOnDemandStatsProvider(&countingSessionPool{getErr: errors.New("pool exhausted")})
	require.True(t, errPoolProvider.GetPhysicalTableStats(tblT.Meta().ID, tblT.Meta()).Pseudo)

	// Verify planning against a partitioned table without a Domain (simulating cross-KS session).
	p := parser.New()
	stmt, err := p.ParseOneStmt("select * from test.tp where id = 5", "", "")
	require.NoError(t, err)

	// 1. Without a bound StatsProvider, planning must not panic and falls back to static pruning
	// when missing global stats are not skipped.
	ctxWithoutStats := mock.NewContext()
	ctxWithoutStats.Store = store
	ctxWithoutStats.GetSessionVars().InRestrictedSQL = true
	ctxWithoutStats.GetSessionVars().SkipMissingPartitionStats = false
	ctxWithoutStats.GetSessionVars().PartitionPruneMode.Store(string(variable.Dynamic))
	ctxWithoutStats.GetSessionVars().StmtCtx.UseDynamicPruneMode = true
	ctxWithoutStats.SetInfoSchema(is)
	_, _, err = planner.Optimize(context.Background(), ctxWithoutStats, resolve.NewNodeW(stmt), is)
	require.NoError(t, err)
	require.False(t, ctxWithoutStats.GetSessionVars().StmtCtx.UseDynamicPruneMode)
	require.True(t, plannerstats.GetStatsTable(ctxWithoutStats, tblTP.Meta(), tblTP.Meta().ID).Pseudo)

	// 2. With OnDemandStatsProvider bound, global stats are loaded and dynamic pruning stays enabled.
	ctxWithStats := mock.NewContext()
	ctxWithStats.Store = store
	ctxWithStats.GetSessionVars().InRestrictedSQL = true
	ctxWithStats.GetSessionVars().SkipMissingPartitionStats = false
	ctxWithStats.GetSessionVars().PartitionPruneMode.Store(string(variable.Dynamic))
	ctxWithStats.GetSessionVars().StmtCtx.UseDynamicPruneMode = true
	ctxWithStats.SetInfoSchema(is)
	ctxWithStats.GetSessionVars().StatsProvider = provider
	defer func() {
		ctxWithStats.GetSessionVars().StatsProvider = nil
	}()

	_, _, err = planner.Optimize(context.Background(), ctxWithStats, resolve.NewNodeW(stmt), is)
	require.NoError(t, err)
	require.True(t, ctxWithStats.GetSessionVars().StmtCtx.UseDynamicPruneMode)
	gotTP := plannerstats.GetStatsTable(ctxWithStats, tblTP.Meta(), tblTP.Meta().ID)
	require.False(t, gotTP.Pseudo)
	require.Equal(t, int64(4), gotTP.RealtimeCount)
}
