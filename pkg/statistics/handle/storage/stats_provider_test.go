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

package storage_test

import (
	"context"
	"strings"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/ngaut/pools"
	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/pkg/meta/metadef"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/parser"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/planner"
	"github.com/pingcap/tidb/pkg/planner/core/resolve"
	plannerstats "github.com/pingcap/tidb/pkg/planner/core/stats"
	"github.com/pingcap/tidb/pkg/sessionctx"
	"github.com/pingcap/tidb/pkg/sessionctx/variable"
	statsstorage "github.com/pingcap/tidb/pkg/statistics/handle/storage"
	"github.com/pingcap/tidb/pkg/testkit"
	"github.com/pingcap/tidb/pkg/util"
	"github.com/pingcap/tidb/pkg/util/chunk"
	"github.com/pingcap/tidb/pkg/util/mock"
	"github.com/pingcap/tidb/pkg/util/sqlexec"
	"github.com/stretchr/testify/require"
)

var _ plannerstats.PhysicalTableStatsProvider = (*statsstorage.OnDemandStatsProvider)(nil)

type spyRestrictedSQLExecutor struct {
	sqlexec.RestrictedSQLExecutor
	onExec func(sql string)
}

func (e *spyRestrictedSQLExecutor) ExecRestrictedSQL(
	ctx context.Context,
	opts []sqlexec.OptionFuncAlias,
	sql string,
	args ...any,
) ([]chunk.Row, []*resolve.ResultField, error) {
	if e.onExec != nil {
		e.onExec(sql)
	}
	return e.RestrictedSQLExecutor.ExecRestrictedSQL(ctx, opts, sql, args...)
}

type spySessionContext struct {
	sessionctx.Context
	raw    pools.Resource
	onExec func(sql string)
}

func (s *spySessionContext) Close() {
	s.raw.Close()
}

func (s *spySessionContext) GetRestrictedSQLExecutor() sqlexec.RestrictedSQLExecutor {
	return &spyRestrictedSQLExecutor{
		RestrictedSQLExecutor: s.Context.GetRestrictedSQLExecutor(),
		onExec:                s.onExec,
	}
}

type countingSessionPool struct {
	util.DestroyableSessionPool
	getCount     atomic.Int64
	putCount     atomic.Int64
	destroyCount atomic.Int64
	getErr       error

	mu      sync.Mutex
	sqls    []string
	onQuery func(sql string)
}

func (p *countingSessionPool) Get() (pools.Resource, error) {
	p.getCount.Add(1)
	if p.getErr != nil {
		return nil, p.getErr
	}
	res, err := p.DestroyableSessionPool.Get()
	if err != nil {
		return nil, err
	}
	return &spySessionContext{
		Context: res.(sessionctx.Context),
		raw:     res,
		onExec: func(sql string) {
			p.mu.Lock()
			p.sqls = append(p.sqls, sql)
			cb := p.onQuery
			p.mu.Unlock()
			if cb != nil {
				cb(sql)
			}
		},
	}, nil
}

func (p *countingSessionPool) unwrap(r pools.Resource) pools.Resource {
	if wrapped, ok := r.(*spySessionContext); ok {
		return wrapped.raw
	}
	return r
}

func (p *countingSessionPool) Put(r pools.Resource) {
	p.putCount.Add(1)
	p.DestroyableSessionPool.Put(p.unwrap(r))
}

func (p *countingSessionPool) Destroy(r pools.Resource) {
	p.destroyCount.Add(1)
	p.DestroyableSessionPool.Destroy(p.unwrap(r))
}

func (p *countingSessionPool) recordedSQLs() []string {
	p.mu.Lock()
	defer p.mu.Unlock()
	out := make([]string, len(p.sqls))
	copy(out, p.sqls)
	return out
}

func (p *countingSessionPool) resetSQLs() {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.sqls = nil
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
	provider := statsstorage.NewOnDemandStatsProvider(context.Background(), spyPool)

	// First lookup hits storage once in batched SQL queries (never reading stats_fm_sketch)
	// and populates Table.Version, LastAnalyzeVersion, and full column/index distributions.
	gotT := provider.GetPhysicalTableStats(tblT.Meta().ID, tblT.Meta())
	require.NotNil(t, gotT)
	require.False(t, gotT.Pseudo)
	require.True(t, gotT.IsAnalyzed())
	require.Greater(t, gotT.Version, uint64(0))
	require.Equal(t, int64(4), gotT.RealtimeCount)
	require.Equal(t, int64(1), spyPool.getCount.Load())
	require.Equal(t, int64(1), spyPool.putCount.Load())
	require.Equal(t, int64(0), spyPool.destroyCount.Load())

	colA := tblT.Meta().FindPublicColumnByName("a")
	require.NotNil(t, colA)
	colAStats := gotT.GetCol(colA.ID)
	require.NotNil(t, colAStats)
	require.True(t, colAStats.IsFullLoad())
	require.Nil(t, colAStats.FMSketch)

	idxA := tblT.Meta().FindIndexByName("idx_a")
	require.NotNil(t, idxA)
	idxAStats := gotT.GetIdx(idxA.ID)
	require.NotNil(t, idxAStats)
	require.True(t, idxAStats.IsFullLoad())
	require.Nil(t, idxAStats.FMSketch)

	// Batched load executes BEGIN + stats_meta + stats_histograms + stats_top_n + stats_buckets (<= 5 SQLs)
	// and never queries mysql.stats_fm_sketch.
	sqls := spyPool.recordedSQLs()
	require.LessOrEqual(t, len(sqls), 5)
	for _, sql := range sqls {
		require.NotContains(t, strings.ToLower(sql), "stats_fm_sketch")
	}

	// Second lookup hits the provider cache without borrowing another session.
	gotTCached := provider.GetPhysicalTableStats(tblT.Meta().ID, tblT.Meta())
	require.Same(t, gotT, gotTCached)
	require.Equal(t, int64(1), spyPool.getCount.Load())

	// WithSkipColumnDistributions loads column metadata + full index distributions
	// while filtering stats_top_n/stats_buckets to indexes only.
	restrictedPool := &countingSessionPool{DestroyableSessionPool: dom.SysSessionPool()}
	restrictedProvider := statsstorage.NewOnDemandStatsProvider(
		context.Background(), restrictedPool, statsstorage.WithSkipColumnDistributions(),
	)
	gotTRestricted := restrictedProvider.GetPhysicalTableStats(tblT.Meta().ID, tblT.Meta())
	require.False(t, gotTRestricted.Pseudo)
	require.Greater(t, gotTRestricted.Version, uint64(0))
	restrictedColA := gotTRestricted.GetCol(colA.ID)
	require.NotNil(t, restrictedColA)
	require.True(t, restrictedColA.IsStatsInitialized())
	require.True(t, restrictedColA.IsAllEvicted())
	require.Greater(t, restrictedColA.NDV, int64(0))
	require.Nil(t, restrictedColA.TopN)
	require.Empty(t, restrictedColA.Buckets)
	restrictedIdxA := gotTRestricted.GetIdx(idxA.ID)
	require.NotNil(t, restrictedIdxA)
	require.True(t, restrictedIdxA.IsFullLoad())
	for _, sql := range restrictedPool.recordedSQLs() {
		low := strings.ToLower(sql)
		if strings.Contains(low, "stats_top_n") || strings.Contains(low, "stats_buckets") {
			require.Contains(t, low, "is_index = 1")
		}
	}

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
	errPoolProvider := statsstorage.NewOnDemandStatsProvider(
		context.Background(), &countingSessionPool{getErr: errors.New("pool exhausted")},
	)
	require.True(t, errPoolProvider.GetPhysicalTableStats(tblT.Meta().ID, tblT.Meta()).Pseudo)

	// Verify planning against a partitioned table without a Domain (simulating cross-KS session).
	p := parser.New()
	stmt, err := p.ParseOneStmt("select * from test.tp where id = 5 and a = 1", "", "")
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

	// 2. With OnDemandStatsProvider bound, planning succeeds under restricted SQL,
	// non-restricted SQL, and InternalSQLScanUserTable=true (without nil Domain panics
	// in CollectPredicateColumnsPoint / SyncWaitStatsLoadPoint), keeping dynamic pruning enabled.
	for _, tc := range []struct {
		name            string
		inRestricted    bool
		scanUserTable   bool
		statsSyncWaitMS int64
	}{
		{name: "restricted", inRestricted: true, scanUserTable: false, statsSyncWaitMS: 0},
		{name: "non-restricted-sync", inRestricted: false, scanUserTable: false, statsSyncWaitMS: 100},
		{name: "non-restricted-async", inRestricted: false, scanUserTable: false, statsSyncWaitMS: 0},
		{name: "internal-scan-user-table", inRestricted: true, scanUserTable: true, statsSyncWaitMS: 100},
	} {
		t.Run("optimize/"+tc.name, func(t *testing.T) {
			ctxWithStats := mock.NewContext()
			ctxWithStats.Store = store
			ctxWithStats.GetSessionVars().InRestrictedSQL = tc.inRestricted
			ctxWithStats.GetSessionVars().InternalSQLScanUserTable = tc.scanUserTable
			ctxWithStats.GetSessionVars().StatsLoadSyncWait.Store(tc.statsSyncWaitMS)
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
			require.Greater(t, gotTP.Version, uint64(0))
			require.Equal(t, int64(4), gotTP.RealtimeCount)
		})
	}
}

func TestOnDemandStatsProviderSnapshotFallbackAndPartitionBatching(t *testing.T) {
	store, dom := testkit.CreateMockStoreAndDomain(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")

	// 1. Partitioned table with stats_meta.snapshot > 0 and no stats_histograms rows
	// (simulating tidb_analyze_column_options='PREDICATE' with no predicate columns/indexes).
	tk.MustExec(`create table tp_snap (id int, v int)
		partition by range (id) (
			partition p0 values less than (10),
			partition p1 values less than (20)
		)`)
	tk.MustExec("insert into tp_snap values (1, 10), (11, 20)")
	tk.MustExec("set @@tidb_partition_prune_mode = 'dynamic'")
	tk.MustExec("analyze table tp_snap")

	is := dom.InfoSchema()
	tblSnap, err := is.TableByName(context.Background(), ast.NewCIStr("test"), ast.NewCIStr("tp_snap"))
	require.NoError(t, err)
	tk.MustExec("delete from mysql.stats_histograms where table_id = ?", tblSnap.Meta().ID)
	tk.MustExec("update mysql.stats_meta set snapshot = 424242, version = 424242 where table_id = ?", tblSnap.Meta().ID)

	snapProvider := statsstorage.NewOnDemandStatsProvider(context.Background(), dom.SysSessionPool())
	gotSnap := snapProvider.GetPhysicalTableStats(tblSnap.Meta().ID, tblSnap.Meta())
	require.False(t, gotSnap.Pseudo)
	require.Equal(t, uint64(424242), gotSnap.Version)
	require.Equal(t, uint64(424242), gotSnap.LastAnalyzeVersion)
	require.True(t, gotSnap.IsAnalyzed())

	p := parser.New()
	stmt, err := p.ParseOneStmt("select * from test.tp_snap where id = 5", "", "")
	require.NoError(t, err)
	mockCtx := mock.NewContext()
	mockCtx.Store = store
	mockCtx.GetSessionVars().SkipMissingPartitionStats = false
	mockCtx.GetSessionVars().PartitionPruneMode.Store(string(variable.Dynamic))
	mockCtx.GetSessionVars().StmtCtx.UseDynamicPruneMode = true
	mockCtx.SetInfoSchema(is)
	mockCtx.GetSessionVars().StatsProvider = snapProvider
	defer func() { mockCtx.GetSessionVars().StatsProvider = nil }()
	_, _, err = planner.Optimize(context.Background(), mockCtx, resolve.NewNodeW(stmt), is)
	require.NoError(t, err)
	require.True(t, mockCtx.GetSessionVars().StmtCtx.UseDynamicPruneMode)

	// 2. Static partition pruning batch prefetching: unanalyzed 4-partition table
	// borrows only 1 session to negative-cache all partitions.
	tk.MustExec(`create table tp_batch (id int, v int, key idx_v(v))
		partition by range (id) (
			partition p0 values less than (10),
			partition p1 values less than (20),
			partition p2 values less than (30),
			partition p3 values less than (40)
		)`)
	is = dom.InfoSchema()
	tblBatch, err := is.TableByName(context.Background(), ast.NewCIStr("test"), ast.NewCIStr("tp_batch"))
	require.NoError(t, err)
	defs := tblBatch.Meta().GetPartitionInfo().Definitions
	require.Len(t, defs, 4)

	unanaPool := &countingSessionPool{DestroyableSessionPool: dom.SysSessionPool()}
	unanaProvider := statsstorage.NewOnDemandStatsProvider(context.Background(), unanaPool)
	for _, def := range defs {
		got := unanaProvider.GetPhysicalTableStats(def.ID, tblBatch.Meta())
		require.True(t, got.Pseudo)
	}
	require.Equal(t, int64(1), unanaPool.getCount.Load())
	require.Equal(t, int64(1), unanaPool.putCount.Load())

	// 3. Analyzed 4-partition table with WithMaxPartitionBatchLoad(2):
	// borrows only 1 session, loads requested partition + 1 sibling, and caps the rest to pseudo.
	tk.MustExec("insert into tp_batch values (1, 1), (11, 11), (21, 21), (31, 31)")
	tk.MustExec("analyze table tp_batch")

	cappedPool := &countingSessionPool{DestroyableSessionPool: dom.SysSessionPool()}
	cappedProvider := statsstorage.NewOnDemandStatsProvider(
		context.Background(), cappedPool, statsstorage.WithMaxPartitionBatchLoad(2),
	)
	requested := cappedProvider.GetPhysicalTableStats(defs[2].ID, tblBatch.Meta())
	require.False(t, requested.Pseudo)
	require.Greater(t, requested.Version, uint64(0))
	nonPseudoCount := 1
	for i, def := range defs {
		if i == 2 {
			continue
		}
		if !cappedProvider.GetPhysicalTableStats(def.ID, tblBatch.Meta()).Pseudo {
			nonPseudoCount++
		}
	}
	require.Equal(t, 2, nonPseudoCount)
	require.Equal(t, int64(1), cappedPool.getCount.Load())
	require.Equal(t, int64(1), cappedPool.putCount.Load())
}

func TestOnDemandStatsProviderCancellationAndPanicDestroy(t *testing.T) {
	store, dom := testkit.CreateMockStoreAndDomain(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("create table t (id int primary key, a int, key idx_a(a))")
	tk.MustExec("insert into t values (1, 10), (2, 20)")
	tk.MustExec("analyze table t")

	is := dom.InfoSchema()
	tblT, err := is.TableByName(context.Background(), ast.NewCIStr("test"), ast.NewCIStr("t"))
	require.NoError(t, err)

	// 1. Pre-canceled context returns pseudo stats immediately without borrowing a session.
	canceledCtx, cancel := context.WithCancel(context.Background())
	cancel()
	preCancelPool := &countingSessionPool{DestroyableSessionPool: dom.SysSessionPool()}
	preCancelProvider := statsstorage.NewOnDemandStatsProvider(canceledCtx, preCancelPool)
	require.True(t, preCancelProvider.GetPhysicalTableStats(tblT.Meta().ID, tblT.Meta()).Pseudo)
	require.Equal(t, int64(0), preCancelPool.getCount.Load())

	// 2. Context canceled mid-load destroys the borrowed session instead of returning it to the pool.
	midCtx, midCancel := context.WithCancel(context.Background())
	midCancelPool := &countingSessionPool{
		DestroyableSessionPool: dom.SysSessionPool(),
		onQuery: func(sql string) {
			if strings.Contains(strings.ToLower(sql), "stats_meta") {
				midCancel()
			}
		},
	}
	midCancelProvider := statsstorage.NewOnDemandStatsProvider(midCtx, midCancelPool)
	require.True(t, midCancelProvider.GetPhysicalTableStats(tblT.Meta().ID, tblT.Meta()).Pseudo)
	require.Equal(t, int64(1), midCancelPool.getCount.Load())
	require.Equal(t, int64(0), midCancelPool.putCount.Load())
	require.Equal(t, int64(1), midCancelPool.destroyCount.Load())

	// 3. Panic after BEGIN destroys the borrowed session instead of calling Put.
	panicPool := &countingSessionPool{
		DestroyableSessionPool: dom.SysSessionPool(),
		onQuery: func(sql string) {
			if strings.Contains(strings.ToLower(sql), "stats_meta") {
				panic("simulated OOM panic during stats load")
			}
		},
	}
	panicProvider := statsstorage.NewOnDemandStatsProvider(context.Background(), panicPool)
	require.PanicsWithValue(t, "simulated OOM panic during stats load", func() {
		_ = panicProvider.GetPhysicalTableStats(tblT.Meta().ID, tblT.Meta())
	})
	require.Equal(t, int64(1), panicPool.getCount.Load())
	require.Equal(t, int64(0), panicPool.putCount.Load())
	require.Equal(t, int64(1), panicPool.destroyCount.Load())
}
