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

	"github.com/pingcap/tidb/pkg/executor"
	"github.com/pingcap/tidb/pkg/executor/importer"
	"github.com/pingcap/tidb/pkg/infoschema"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/meta"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/planner/core"
	"github.com/pingcap/tidb/pkg/planner/core/base"
	"github.com/pingcap/tidb/pkg/session"
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
	tk.MustExec("set collation_connection='utf8mb4_general_ci'")
	tk.MustExec("set tidb_isolation_read_engines='tikv'")
	queries := []string{
		"with c as (select g,v,i from query_src where i>0) select g,count(*),sum(v) from c group by g",
		"with c as (select i from query_src where i=1) select x.i,y.i from c x join (with c as (select i from query_src where i=2) select i from c) y",
		"with query_src as (select 1 as i) select c.i,t.i from query_src c join test.query_src t on t.i=c.i",
		"with recursive c(n) as (select 1 union all select n+1 from c where n<3) select n from c",
		"select a.i,b.v from query_src a left join query_src b on a.i=b.i where b.v>5",
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
				captured, err := executor.CaptureImportQuery(tk.Session(), "import into unused_target from ("+sql+") with thread=1")
				require.NoError(t, err)
				require.Equal(t, "import into unused_target from ("+sql+") with thread=1", captured.SQL)
				testfailpoint.EnableCall(t, "github.com/pingcap/tidb/pkg/executor/afterImportQueryOptimize", func(p base.PhysicalPlan) {
					require.EqualValues(t, 32<<20, p.SCtx().GetSessionVars().MemQuotaQuery)
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
				err = importer.RunImportQuery(context.Background(), se, captured, 32<<20, output)
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
	t.Run("CTE source tables", func(t *testing.T) {
		tk.MustExec("create table query_other(i int)")
		q, err := executor.CaptureImportQuery(tk.Session(), "import into unused_target from with c as (select a.i from query_src a join query_other b on a.i=b.i) select * from c")
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
		_, err = executor.CaptureImportQuery(tk.Session(), "import into unused_target from select ? from query_src")
		require.ErrorContains(t, err, "unexpected '?'")
		for _, sql := range []string{
			"select @v from query_src", "select @v:=i from query_src",
			"select @@sql_mode", "with c as (select @v) select * from c",
		} {
			_, err = executor.CaptureImportQuery(tk.Session(), "import into unused_target from ("+sql+")")
			require.ErrorContains(t, err, "does not support variables")
		}
	})
	t.Run("close after open error", func(t *testing.T) {
		q, err := executor.CaptureImportQuery(tk.Session(), "import into unused_target from select i+1 from query_src")
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
		err = importer.RunImportQuery(context.Background(), se, q, 1<<20, nil)
		require.ErrorContains(t, err, "mock ProjectionExec.baseExecutor.Open returned error")
		// Projection publishes its concurrency statistics only when Close runs.
		require.Contains(t, se.GetSessionVars().StmtCtx.RuntimeStatsColl.GetRootStats(planID).String(), "Concurrency:")
	})
	t.Run("submitted table definitions", func(t *testing.T) {
		q, err := executor.CaptureImportQuery(tk.Session(), "import into unused_target from select count(*) from query_src")
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
		err = importer.RunImportQuery(context.Background(), se, q, 1<<20, output)
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
	captured, err := executor.CaptureImportQuery(tk.Session(), sql)
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
	err = importer.RunImportQuery(context.Background(), se, captured, 1<<20, nil)
	require.ErrorContains(t, err, "injected failure after import query optimization")
	require.True(t, optimized)
}
