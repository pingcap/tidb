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

package executor_test

import (
	"context"
	"encoding/json"
	"fmt"
	"sort"
	"strings"
	"testing"

	"github.com/pingcap/tidb/pkg/meta/autoid"
	"github.com/pingcap/tidb/pkg/store/mockstore"
	"github.com/pingcap/tidb/pkg/tablecodec"
	"github.com/tikv/client-go/v2/testutils"
	"golang.org/x/sync/errgroup"

	"github.com/pingcap/tidb/pkg/domain"
	"github.com/pingcap/tidb/pkg/executor"
	"github.com/pingcap/tidb/pkg/executor/importer"
	"github.com/pingcap/tidb/pkg/infoschema"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/meta"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/objstore"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/planner/core"
	"github.com/pingcap/tidb/pkg/planner/core/base"
	"github.com/pingcap/tidb/pkg/session"
	"github.com/pingcap/tidb/pkg/sessionctx/vardef"
	"github.com/pingcap/tidb/pkg/statistics/asyncload"
	"github.com/pingcap/tidb/pkg/testkit"
	"github.com/pingcap/tidb/pkg/testkit/testfailpoint"
	"github.com/stretchr/testify/require"
)

func TestImportQueryPlanExecution(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("create table query_src(g bigint, v decimal(20,2), i bigint, key idx_i(i))")
	tk.MustExec("insert into query_src values (1,10,1),(1,20,2),(1,NULL,3),(2,7,4),(NULL,3,5),(NULL,NULL,6),(3,NULL,7)")
	tk.MustExec("analyze table query_src all columns")
	tk.MustExec("set collation_connection='utf8mb4_general_ci'")
	tk.MustExec("set tidb_isolation_read_engines='tikv'")
	queries := []string{
		"select g,count(*),sum(v) from (select g,v from query_src where i>0) c group by g",
		"select i from query_src where i=1 limit 1",
		"select g from (select /*+ SET_VAR(tidb_distsql_scan_concurrency=9) */ g from query_src force index(idx_i)) x",
		`select hex(_binary'\0\\a'), _latin1'a', 'a''b'`,
		"select 'a' = 'A'",
		"select /*+ SET_VAR(tidb_distsql_scan_concurrency=2) */ count(*) from query_src",
		"select /*+ SET_VAR(tidb_mem_quota_query=1073741824) */ count(*) from query_src",
		"select /*+ MEMORY_QUOTA(1 GB) */ count(*) from query_src",
		"select /*+ HASH_AGG() */ g,count(*),count(v),sum(v),min(v),max(v) from query_src group by g",
		"select /*+ HASH_AGG() */ g,count(*),sum(v) from query_src where round(v,2)>5 group by g",
		"select /*+ STREAM_AGG() */ i,count(*) from query_src use index(idx_i) group by i",
		"select /*+ HASH_AGG() */ g,count(*)+1 from query_src where i>100 group by g",
	}
	tk.MustExec("set tidb_mem_quota_query=16777216")
	for _, mode := range []string{"", "NO_BACKSLASH_ESCAPES"} {
		tk.MustExec("set sql_mode='" + mode + "'")
		for _, sql := range queries {
			t.Run(mode+"/"+sql, func(t *testing.T) {
				expectedConcurrency := tk.Session().GetSessionVars().DistSQLScanConcurrency()
				captured, _, err := executor.CaptureImportQuery(tk.Session(), "import into unused_target from ("+sql+") with thread=1")
				require.NoError(t, err)
				require.Equal(t, "import into unused_target from ("+sql+") with thread=1", captured.SQL)
				testfailpoint.EnableCall(t, "github.com/pingcap/tidb/pkg/executor/afterImportQueryOptimize", func(p base.PhysicalPlan) {
					require.EqualValues(t, 32<<20, p.SCtx().GetSessionVars().MemQuotaQuery)
					require.Equal(t, expectedConcurrency, p.SCtx().GetSessionVars().DistSQLScanConcurrency())
					require.Empty(t, p.SCtx().GetSessionVars().StmtCtx.StmtHints.SetVars)
					if strings.Contains(sql, "force index") {
						require.NotContains(t, core.ToString(p), "IndexLookUp")
					}
				})
				data, err := json.Marshal(captured)
				require.NoError(t, err)
				captured = &importer.QueryPlan{}
				require.NoError(t, json.Unmarshal(data, captured))
				var expected []string
				for _, row := range tk.MustQuery(sql).Rows() {
					expected = append(expected, fmt.Sprint(row))
				}
				se, err := session.CreateSession4Test(store)
				require.NoError(t, err)
				defer se.Close()
				output := make(chan importer.QueryChunk, len(expected)+1)
				err = importer.RunImportQuery(context.Background(), captured, importer.QueryRuntime{Session: se, SessionPool: domain.GetDomain(tk.Session()).SysSessionPool(), Storage: objstore.NewMemStorage(), Prefix: "query-test", TotalMemoryLimit: 32 << 20, MemoryLimit: 1 << 19}, output)
				require.NoError(t, err)
				close(output)
				var got []string
				for result := range output {
					chk := result.Chk
					for i := range chk.NumRows() {
						row := chk.GetRow(i).GetDatumRow(result.Fields)
						values := make([]string, len(row))
						for j, d := range row {
							if d.IsNull() {
								values[j] = "<nil>"
							} else {
								values[j], err = d.ToString()
								require.NoError(t, err)
							}
						}
						got = append(got, fmt.Sprint(values))
					}
				}
				sort.Strings(expected)
				sort.Strings(got)
				require.Equal(t, expected, got)
			})
		}
	}
	t.Run("source tables", func(t *testing.T) {
		tk.MustExec("create table query_other(i int)")
		q, _, err := executor.CaptureImportQuery(tk.Session(), "import into unused_target from (select i from query_src union all select i from query_other)")
		require.NoError(t, err)
		require.Len(t, q.Databases, 1)
		db, ok := tk.Session().GetLatestInfoSchema().SchemaByName(ast.NewCIStr("test"))
		require.True(t, ok)
		require.Contains(t, q.Databases, db.ID)
		var tables []string
		for _, tbl := range q.Tables[db.ID] {
			tables = append(tables, tbl.Name.L)
		}
		require.ElementsMatch(t, []string{"query_src", "query_other"}, tables)
		_, _, err = executor.CaptureImportQuery(tk.Session(), "import into unused_target from select ? from query_src")
		require.ErrorContains(t, err, "unexpected '?'")
		for _, sql := range []string{
			"select @v from query_src", "select @v:=i from query_src",
			"select @@sql_mode", "with c as (select @v) select * from c",
		} {
			_, _, err = executor.CaptureImportQuery(tk.Session(), "import into unused_target from ("+sql+")")
			require.ErrorContains(t, err, "does not support variables")
		}
	})
	t.Run("unsupported queries", func(t *testing.T) {
		for _, tt := range []struct{ sql, unsupported string }{
			{"select a.i from query_src a join query_src b on a.i=b.i", "JOIN"},
			{"select a.i from query_src a, query_src b", "JOIN"},
			{"select a.i from query_src a left join query_src b on a.i=b.i", "JOIN"},
			{"select * from query_src a natural join query_src b", "JOIN"},
			{"select * from (select a.i from query_src a cross join query_src b) s", "JOIN"},
			{"with c as (select i from query_src) select * from c", "CTE"},
			{"with recursive c(n) as (select 1 union all select n+1 from c where n<3) select n from c", "CTE"},
			{"select * from (with c as (select i from query_src) select * from c) s", "CTE"},
			{"select i from query_src order by i", "ORDER BY"},
			{"select i from query_src order by i limit 1", "ORDER BY"},
			{"select * from (select i from query_src order by i limit 1) s", "ORDER BY"},
			{"select i from query_src union all select i from query_src order by i limit 1", "ORDER BY"},
			{"select row_number() over (order by i) from query_src", "ORDER BY"},
		} {
			t.Run(tt.sql, func(t *testing.T) {
				q, _, err := executor.CaptureImportQuery(tk.Session(), "import into unused_target from ("+tt.sql+")")
				require.ErrorContains(t, err, "does not support "+tt.unsupported)
				require.Nil(t, q)
			})
		}
	})
	t.Run("close after open error", func(t *testing.T) {
		q, _, err := executor.CaptureImportQuery(tk.Session(), "import into unused_target from select i+1 from query_src")
		require.NoError(t, err)
		se, err := session.CreateSession4Test(store)
		require.NoError(t, err)
		defer se.Close()
		var planID int
		testfailpoint.EnableCall(t, "github.com/pingcap/tidb/pkg/executor/afterImportQueryOptimize", func(p base.PhysicalPlan) {
			require.Equal(t, "Projection", p.TP())
			planID = p.ID()
			testfailpoint.Enable(t, "github.com/pingcap/tidb/pkg/executor/mockProjectionExecBaseExecutorOpenReturnedError", `return(true)`)
		})
		err = importer.RunImportQuery(context.Background(), q, importer.QueryRuntime{
			Session: se, SessionPool: domain.GetDomain(tk.Session()).SysSessionPool(), Storage: objstore.NewMemStorage(), Prefix: "open-error", TotalMemoryLimit: 1 << 20, MemoryLimit: 1 << 19,
		}, nil)
		require.ErrorContains(t, err, "mock ProjectionExec.baseExecutor.Open returned error")
		// Projection publishes its concurrency statistics only when Close runs.
		require.Contains(t, se.GetSessionVars().StmtCtx.RuntimeStatsColl.GetRootStats(planID).String(), "Concurrency:")
	})
	t.Run("submitted table definitions", func(t *testing.T) {
		q, _, err := executor.CaptureImportQuery(tk.Session(), "import into unused_target from select count(*) from query_src")
		require.NoError(t, err)
		data, err := json.Marshal(q)
		require.NoError(t, err)
		q = &importer.QueryPlan{}
		require.NoError(t, json.Unmarshal(data, q))
		tk.MustExec("rename table query_src to query_renamed")
		se, err := session.CreateSession4Test(store)
		require.NoError(t, err)
		defer se.Close()
		output := make(chan importer.QueryChunk, 1)
		err = importer.RunImportQuery(context.Background(), q, importer.QueryRuntime{Session: se, SessionPool: domain.GetDomain(tk.Session()).SysSessionPool(), Storage: objstore.NewMemStorage(), Prefix: "query-test", TotalMemoryLimit: 1 << 20, MemoryLimit: 1 << 19}, output)
		require.NoError(t, err)
		close(output)
		var rows []int64
		for result := range output {
			for i := range result.Chk.NumRows() {
				rows = append(rows, result.Chk.GetRow(i).GetInt64(0))
			}
		}
		require.Equal(t, []int64{7}, rows)
	})
}

func TestImportQueryPlanTiFlashOptimization(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("create table query_flash(g int, v bigint)")
	tk.MustExec("insert into query_flash values (1,10),(1,20),(2,30)")
	tk.MustExec("analyze table query_flash all columns")
	is := tk.Session().GetInfoSchema().(infoschema.InfoSchema)
	tbl, err := is.TableByName(context.Background(), ast.NewCIStr("test"), ast.NewCIStr("query_flash"))
	require.NoError(t, err)
	tbl.Meta().TiFlashReplica = &model.TiFlashReplicaInfo{Count: 1, Available: true}
	require.NoError(t, kv.RunInNewTxn(kv.WithInternalSourceType(context.Background(), kv.InternalTxnOthers), store, true,
		func(_ context.Context, txn kv.Transaction) error {
			return meta.NewMutator(txn).UpdateTable(tbl.Meta().DBID, tbl.Meta())
		}))
	tk.MustExec("set tidb_allow_mpp=1")
	tk.MustExec("set tidb_enforce_mpp=1")
	tk.MustExec("set tidb_isolation_read_engines='tiflash'")

	sql := "import into unused_target from select g,count(*),sum(v) from query_flash group by g"
	captured, _, err := executor.CaptureImportQuery(tk.Session(), sql)
	require.NoError(t, err)
	se, err := session.CreateSession4Test(store)
	require.NoError(t, err)
	defer se.Close()
	var optimized bool
	testfailpoint.EnableCall(t, "github.com/pingcap/tidb/pkg/executor/afterImportQueryOptimize", func(p base.PhysicalPlan) {
		optimized = true
		require.True(t, strings.Contains(core.ToString(p), "Send("), core.ToString(p))
	})
	// Validate the real optimizer output without dispatching to a TiFlash server.
	testfailpoint.Enable(t, "github.com/pingcap/tidb/pkg/executor/failAfterImportQueryOptimize", `return(true)`)
	err = importer.RunImportQuery(context.Background(), captured, importer.QueryRuntime{
		Session: se, SessionPool: domain.GetDomain(tk.Session()).SysSessionPool(), Storage: objstore.NewMemStorage(), Prefix: "query-flash", TotalMemoryLimit: 1 << 20, MemoryLimit: 1 << 19,
	}, nil)
	require.ErrorContains(t, err, "injected failure after import query optimization")
	require.True(t, optimized)
}

func TestImportQueryPlanColdStats(t *testing.T) {
	store, dom := testkit.CreateMockStoreAndDomain(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	db, ok := tk.Session().GetLatestInfoSchema().SchemaByName(ast.NewCIStr("test"))
	require.True(t, ok)
	tk.MustExec("create table query_stats(g int, i int, key idx_i(i))")
	values := make([]string, 1000)
	for i := range values {
		values[i] = fmt.Sprintf("(%d,%d)", i/100, i)
	}
	tk.MustExec("insert into query_stats values " + strings.Join(values, ","))
	tk.MustExec("analyze table query_stats all columns")
	tk.MustExec("set tidb_isolation_read_engines='tikv'")
	dom.StatsHandle().Clear()
	for _, sql := range []string{
		"select g from query_stats where g=7",
		"select g from query_stats where i=7",
	} {
		t.Run(sql, func(t *testing.T) {
			q, _, err := executor.CaptureImportQuery(tk.Session(), "import into unused_target from ("+sql+")")
			require.NoError(t, err)
			se, err := session.CreateSession4Test(store)
			require.NoError(t, err)
			defer se.Close()
			var optimized bool
			testfailpoint.EnableCall(t, "github.com/pingcap/tidb/pkg/executor/afterImportQueryOptimize", func(p base.PhysicalPlan) {
				optimized = true
				if strings.Contains(sql, "g=7") {
					// Pseudo selectivity would estimate approximately one row.
					require.InDelta(t, 100, p.StatsInfo().RowCount, 1)
				} else {
					require.Contains(t, core.ToString(p), "IndexLookUp")
				}
			})
			output := make(chan importer.QueryChunk, 100)
			err = importer.RunImportQuery(context.Background(), q, importer.QueryRuntime{
				Session: se, SessionPool: domain.GetDomain(tk.Session()).SysSessionPool(), Storage: objstore.NewMemStorage(), Prefix: "cold-stats", TotalMemoryLimit: 4 << 20, MemoryLimit: 4 << 19,
			}, output)
			require.NoError(t, err)
			require.True(t, optimized)
			close(output)
			rows := 0
			for result := range output {
				require.EqualValues(t, rows, result.RowIDOffset)
				rows += result.Chk.NumRows()
			}
			if strings.Contains(sql, "g=7") {
				require.Equal(t, 100, rows)
			} else {
				require.Equal(t, 1, rows)
			}
			_, cached := dom.StatsHandle().GetNonPseudoPhysicalTableStats(q.Tables[db.ID][0].ID)
			require.False(t, cached, "task statistics must not warm the domain cache")
		})
	}

	tk.MustExec("create table query_no_stats(g int)")
	tk.MustExec("insert into query_no_stats values (1)")
	q, _, err := executor.CaptureImportQuery(tk.Session(), "import into unused_target from select count(*) from query_no_stats")
	require.NoError(t, err)
	run := func(ctx context.Context) error {
		se, err := session.CreateSession4Test(store)
		require.NoError(t, err)
		defer se.Close()
		runtime := importer.QueryRuntime{Session: se, SessionPool: domain.GetDomain(tk.Session()).SysSessionPool(), Storage: objstore.NewMemStorage(), Prefix: "missing-stats", TotalMemoryLimit: 4 << 20, MemoryLimit: 4 << 19}
		return importer.RunImportQuery(ctx, q, runtime, make(chan importer.QueryChunk, 1))
	}
	err = run(context.Background())
	require.ErrorContains(t, err, "ANALYZE TABLE")
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	err = run(ctx)
	require.ErrorIs(t, err, context.Canceled)
	testfailpoint.Enable(t, "github.com/pingcap/tidb/pkg/statistics/handle/util/ExecRowsTimeout", `return(true)`)
	err = run(context.Background())
	require.ErrorContains(t, err, "inject timeout error")
}

func TestImportQueryPlanDomain(t *testing.T) {
	store, host := testkit.CreateMockStoreAndDomain(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	db, ok := tk.Session().GetLatestInfoSchema().SchemaByName(ast.NewCIStr("test"))
	require.True(t, ok)
	tk.MustExec("create table query_partition(id bigint primary key,v bigint) partition by range(id) (partition p0 values less than(10),partition p1 values less than(maxvalue))")
	tk.MustExec("insert into query_partition values (1,10),(2,20),(11,30)")
	tk.MustExec("analyze table query_partition all columns")
	tk.MustExec("set tidb_isolation_read_engines='tikv'")
	q, _, err := executor.CaptureImportQuery(tk.Session(), "import into unused_target from select /*+ SET_VAR(TIDB_STATS_LOAD_SYNC_WAIT=0) */ sum(v) from query_partition where id>0")
	require.NoError(t, err)
	q.SessionVars[vardef.TiDBStatsLoadSyncWait] = "0"
	sourceID := q.Tables[db.ID][0].ID
	for _, item := range asyncload.AsyncLoadHistogramNeededItems.AllItems() {
		if item.TableID == sourceID {
			asyncload.AsyncLoadHistogramNeededItems.Delete(item.TableItemID)
		}
	}
	host.StatsHandle().Clear()
	quota := vardef.StatsCacheMemQuota.Load()
	vardef.StatsCacheMemQuota.Store(1)
	defer vardef.StatsCacheMemQuota.Store(quota)
	se, err := session.CreateSession4Test(store)
	require.NoError(t, err)
	defer se.Close()
	var observed *domain.Domain
	testfailpoint.EnableCall(t, "github.com/pingcap/tidb/pkg/executor/afterImportQueryOptimize", func(p base.PhysicalPlan) {
		observed = domain.GetDomain(p.SCtx())
		require.False(t, p.SCtx().GetSessionVars().InRestrictedSQL)
		require.Positive(t, p.SCtx().GetSessionVars().StatsLoadSyncWait.Load())
		require.Empty(t, p.SCtx().GetSessionVars().StmtCtx.StatsLoad.NeededItems)
		require.False(t, p.SCtx().GetSessionVars().EnableNonPreparedPlanCache)
		require.NotNil(t, observed)
		require.NotSame(t, host, observed)
		require.Same(t, store, observed.Store())
		require.Equal(t, host.ServerID(), observed.ServerID())
		require.True(t, p.SCtx().GetSessionVars().StmtCtx.UseDynamicPartitionPrune())
		source := q.Tables[db.ID][0]
		tbl, ok := observed.InfoSchema().TableByID(context.Background(), source.ID)
		require.True(t, ok)
		require.Equal(t, source.Name, tbl.Meta().Name)
		ids := []int64{source.ID}
		for _, part := range source.Partition.Definitions {
			ids = append(ids, part.ID)
		}
		for _, id := range ids {
			stats := observed.StatsHandle().GetPhysicalTableStats(id, source)
			require.False(t, stats.Pseudo)
			require.True(t, stats.IsInitialized())
			require.True(t, stats.CanNotTriggerLoad)
		}
		// A cache miss still has the submitted database schema available.
		missing := source.Clone()
		missing.ID += 100000
		require.True(t, observed.StatsHandle().GetPhysicalTableStats(missing.ID, missing).Pseudo)
	})
	output := make(chan importer.QueryChunk, 1)
	err = importer.RunImportQuery(context.Background(), q, importer.QueryRuntime{
		Session: se, SessionPool: host.SysSessionPool(), Storage: objstore.NewMemStorage(), Prefix: "query-domain", TotalMemoryLimit: 4 << 20, MemoryLimit: 4 << 19,
	}, output)
	require.NoError(t, err)
	require.NotNil(t, observed)
	close(output)
	var rows int
	for batch := range output {
		for i := range batch.Chk.NumRows() {
			rows++
			v, err := batch.Chk.GetRow(i).GetMyDecimal(0).ToInt()
			require.NoError(t, err)
			require.EqualValues(t, 60, v)
		}
	}
	require.Equal(t, 1, rows)
	// Query cleanup must leave the hosting domain, its pool and the store alive.
	for _, item := range asyncload.AsyncLoadHistogramNeededItems.AllItems() {
		require.NotEqual(t, sourceID, item.TableID)
	}
	require.Same(t, host, domain.GetDomain(se))
	resource, err := host.SysSessionPool().Get()
	require.NoError(t, err)
	host.SysSessionPool().Put(resource)
	_, err = store.CurrentVersion(kv.GlobalTxnScope)
	require.NoError(t, err)
	_, cached := host.StatsHandle().GetNonPseudoPhysicalTableStats(q.Tables[db.ID][0].ID)
	require.False(t, cached)
}

func TestImportQueryRanges(t *testing.T) {
	var cluster testutils.Cluster
	store, dom := testkit.CreateMockStoreAndDomain(t, mockstore.WithClusterInspector(func(c testutils.Cluster) {
		mockstore.BootstrapWithSingleStore(c)
		cluster = c
	}))
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("set tidb_isolation_read_engines='tikv'")
	tk.MustExec("create table range_dst(id bigint primary key nonclustered, v bigint)")
	target, err := dom.InfoSchema().TableByName(context.Background(), ast.NewCIStr("test"), ast.NewCIStr("range_dst"))
	require.NoError(t, err)
	db, ok := dom.InfoSchema().SchemaByName(ast.NewCIStr("test"))
	require.True(t, ok)
	ctx := context.Background()
	prepare := func(t *testing.T, sql string) *importer.Plan {
		t.Helper()
		nodes, err := tk.Session().Parse(ctx, sql)
		require.NoError(t, err)
		require.NoError(t, executor.ResetContextOfStmt(tk.Session(), nodes[0]))
		require.NoError(t, tk.Session().PrepareTxnCtx(ctx, nodes[0]))
		stmt, err := (&executor.Compiler{Ctx: tk.Session()}).Compile(ctx, nodes[0])
		require.NoError(t, err)
		q, node, err := executor.CaptureImportQuery(tk.Session(), "import into unused_target from ("+sql+")")
		require.NoError(t, err)
		plan := &importer.Plan{Query: q}
		require.NoError(t, executor.PrepareImportQueryRanges(ctx, tk.Session(), node, stmt.Plan.(base.PhysicalPlan), plan))
		return plan
	}
	for _, tc := range []struct{ name, ddl, values, predicate string }{
		{"signed", "id bigint primary key clustered,v bigint", "(-5,1),(1,2),(2,NULL),(3,2),(4,5),(7,2),(9,5)", "id>=1 and id<9 and (v is null or v<5)"},
		{"unsigned", "id bigint unsigned primary key clustered,v bigint", "(1,2),(2,NULL),(3,2),(4,5),(9223372036854775808,2),(18446744073709551615,2)", "id>=2 and (v is null or v<5)"},
		{"nonclustered", "id bigint primary key nonclustered,v bigint", "(9,2),(8,2),(7,NULL),(6,5),(5,2),(4,2)", "v is null or v<5"},
		{"common", "id varchar(20),v bigint,primary key(id,v) clustered", "('a',1),('a',2),('b',2),('c',3),('d',2),('e',2)", "id>='b' and v<3"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			tk.MustExec("create table range_src(" + tc.ddl + ")")
			defer tk.MustExec("drop table range_src")
			tk.MustExec("insert into range_src values " + tc.values)
			source, err := dom.InfoSchema().TableByName(ctx, ast.NewCIStr("test"), ast.NewCIStr("range_src"))
			require.NoError(t, err)
			start := tablecodec.GenTableRecordPrefix(source.Meta().ID)
			cluster.SplitKeys(store.GetCodec().EncodeKey(start), store.GetCodec().EncodeKey(start.PrefixNext()), 4)
			tk.MustExec("analyze table range_src all columns")
			sql := "select v,v+1 from range_src where " + tc.predicate
			var expected []string
			for _, row := range tk.MustQuery(sql).Rows() {
				expected = append(expected, fmt.Sprint(row))
			}
			plan := prepare(t, sql)
			require.NotNil(t, plan.Query.Scan)
			require.Greater(t, plan.MaxNodeCnt, 1)
			require.Greater(t, len(plan.Query.Scan.Ranges), 1)
			for i := 1; i < len(plan.Query.Scan.Ranges); i++ {
				require.Equal(t, plan.Query.Scan.Ranges[i-1].EndKey, plan.Query.Scan.Ranges[i].StartKey)
			}
			data, err := json.Marshal(plan.Query)
			require.NoError(t, err)
			q := &importer.QueryPlan{}
			require.NoError(t, json.Unmarshal(data, q))
			// Workers and retries must keep the captured snapshot across source writes.
			tk.MustExec("delete from range_src")
			run := func(r kv.KeyRange) ([]string, []int64, error) {
				se, err := session.CreateSession4Test(store)
				if err != nil {
					return nil, nil, err
				}
				defer se.Close()
				alloc := autoid.NewAllocator(dom.InfoSchema().GetAutoIDRequirement(), db.ID, target.Meta().ID, false,
					autoid.RowIDAllocType, autoid.AllocOptionTableInfoVersion(target.Meta().Version))
				output := make(chan importer.QueryChunk, 64)
				err = importer.RunImportQuery(ctx, q, importer.QueryRuntime{
					Session: se, SessionPool: dom.SysSessionPool(), Storage: objstore.NewMemStorage(), Prefix: "ranges", TotalMemoryLimit: 4 << 20, MemoryLimit: 4 << 19,
					Range: &r, RowIDAllocator: alloc, ScanConcurrency: 1,
				}, output)
				if err != nil {
					return nil, nil, err
				}
				if se.GetDistSQLCtx().DistSQLConcurrency != 1 {
					return nil, nil, fmt.Errorf("unexpected cop concurrency: %d", se.GetDistSQLCtx().DistSQLConcurrency)
				}
				close(output)
				var rows []string
				var ids []int64
				for result := range output {
					for i := range result.Chk.NumRows() {
						datumRow := result.Chk.GetRow(i).GetDatumRow(result.Fields)
						values := make([]string, len(datumRow))
						for j, d := range datumRow {
							values[j] = "<nil>"
							if !d.IsNull() {
								values[j], err = d.ToString()
								if err != nil {
									return nil, nil, err
								}
							}
						}
						rows = append(rows, fmt.Sprint(values))
						ids = append(ids, result.RowIDOffset+int64(i)+1)
					}
				}
				return rows, ids, nil
			}
			got := make([][]string, len(q.Scan.Ranges))
			ids := make([][]int64, len(q.Scan.Ranges))
			var group errgroup.Group
			for i, r := range q.Scan.Ranges {
				group.Go(func() error { var err error; got[i], ids[i], err = run(r); return err })
			}
			require.NoError(t, group.Wait())
			var all []string
			seen := make(map[int64]bool)
			for i := range got {
				all = append(all, got[i]...)
				for _, id := range ids[i] {
					require.False(t, seen[id])
					seen[id] = true
				}
			}
			sort.Strings(all)
			sort.Strings(expected)
			require.Equal(t, expected, all)
			for i, r := range q.Scan.Ranges {
				retryRows, retryIDs, err := run(r)
				require.NoError(t, err)
				require.Equal(t, got[i], retryRows)
				for _, id := range retryIDs {
					require.False(t, seen[id])
					seen[id] = true
				}
			}
		})
	}
	tk.MustExec("create table range_src(id bigint primary key,v bigint,payload varchar(100) default 'payload',key vi(v))")
	tk.MustExec("insert into range_src(id,v) values (1,1),(2,2),(3,3),(4,4)")
	source, err := dom.InfoSchema().TableByName(ctx, ast.NewCIStr("test"), ast.NewCIStr("range_src"))
	require.NoError(t, err)
	start := tablecodec.GenTableRecordPrefix(source.Meta().ID)
	cluster.SplitKeys(store.GetCodec().EncodeKey(start), store.GetCodec().EncodeKey(start.PrefixNext()), 3)
	tk.MustExec("analyze table range_src all columns")
	for _, sql := range []string{
		"select /*+ SET_VAR(tidb_distsql_scan_concurrency=2) */ * from range_src",
		"select * from range_src use index()",
		"select count(*) from range_src", "select v,count(*) from range_src group by v",
		"select distinct v from range_src", "select * from range_src limit 2",
		"select now(),id from range_src",
		"select rand(),id from range_src", "select id,(select max(v) from range_src) from range_src",
		"select row_number() over() from range_src", "select id from range_src use index(vi) where v>1",
	} {
		t.Run(sql, func(t *testing.T) {
			plan := prepare(t, sql)
			require.Nil(t, plan.Query.Scan)
			require.Equal(t, 1, plan.MaxNodeCnt)
		})
	}
	tk.MustExec("create table range_part(id bigint primary key,v bigint) partition by hash(id) partitions 2")
	require.Nil(t, prepare(t, "select * from range_part").Query.Scan)
	tk.MustExec("create table range_generated(id bigint primary key,v bigint as (id+1))")
	require.Nil(t, prepare(t, "select * from range_generated").Query.Scan)
	source.Meta().TiFlashReplica = &model.TiFlashReplicaInfo{Count: 1, Available: true}
	tk.MustExec("set tidb_isolation_read_engines='tikv,tiflash'")
	tk.MustExec("set tidb_enforce_mpp=1")
	require.Nil(t, prepare(t, "select /*+ READ_FROM_STORAGE(TIFLASH[range_src]) */ * from range_src").Query.Scan)
	tk.MustExec("set tidb_isolation_read_engines='tikv'")
	tk.MustExec("set tidb_enforce_mpp=0")
	// A changed access path must fail before opening an unrestricted reader.
	plan := prepare(t, "select * from range_src")
	require.NotNil(t, plan.Query.Scan)
	plan.Query.SQL = "import into unused_target from select count(*) from range_src"
	se, err := session.CreateSession4Test(store)
	require.NoError(t, err)
	defer se.Close()
	err = importer.RunImportQuery(ctx, plan.Query, importer.QueryRuntime{Session: se, SessionPool: dom.SysSessionPool(), TotalMemoryLimit: 4 << 20, MemoryLimit: 4 << 19}, nil)
	require.ErrorContains(t, err, "no longer has a supported TiKV table scan")
}
