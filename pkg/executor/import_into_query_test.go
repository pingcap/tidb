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
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/pingcap/tidb/pkg/executor/importer"
	"github.com/pingcap/tidb/pkg/infoschema"
	"github.com/pingcap/tidb/pkg/planner/core/base"
	"github.com/pingcap/tidb/pkg/session"
	"github.com/pingcap/tidb/pkg/sessionctx/vardef"
	"github.com/pingcap/tidb/pkg/testkit"
	"github.com/pingcap/tidb/pkg/testkit/testfailpoint"
	"github.com/stretchr/testify/require"
)

func TestImportQueryPlanExecution(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("create table query_src(g bigint, v decimal(20,2), i bigint, key idx_i(i))")
	tk.MustExec("create table unused_target(i bigint)")
	tk.MustExec("insert into query_src values (1,10,1),(1,20,2),(1,NULL,3),(2,7,4),(NULL,3,5),(NULL,NULL,6),(3,NULL,7)")
	tk.MustExec("create view query_src_v as select g, i from query_src where i > 2")
	tk.MustExec("create table query_cached(i bigint)")
	tk.MustExec("insert into query_cached values (3),(4),(8)")
	tk.MustExec("alter table query_cached cache")
	tk.MustExec("set collation_connection='utf8mb4_general_ci'")
	tk.MustExec("set tidb_isolation_read_engines='tikv'")
	tk.MustExec("set tidb_mem_quota_query=16777216")
	tk.MustExec("set div_precision_increment=8")
	tk.MustExec("set group_concat_max_len=7")
	tk.MustExec("set @runtime_v = 42")
	cases := []struct{ sqlMode, sql string }{
		{"", "select i from (select i from query_src where i>0) c"},
		{"", `select hex(_binary'\0\\a'), _latin1'a', 'a''b'`},
		{"NO_BACKSLASH_ESCAPES", `select hex(_binary'\0\\a'), _latin1'a', 'a''b'`},
		{"", "select /*+ STREAM_AGG() SET_VAR(tidb_distsql_scan_concurrency=2) */ count(*) from query_src"},
		{"", "select /*+ STREAM_AGG() SET_VAR(tidb_mem_quota_query=1073741824) */ count(*) from query_src"},
		{"", "select /*+ STREAM_AGG() MEMORY_QUOTA(1 GB) */ count(*) from query_src"},
		{"", "select /*+ READ_FROM_STORAGE(TIFLASH[query_src]) MPP_1PHASE_AGG() */ i from query_src"},
		{"", "select /*+ STREAM_AGG() */ i,count(*),sum(v) from query_src use index(idx_i) where round(v,2)>5 group by i"},
		{"", "with c as (select /*+ MERGE() */ i from query_src) select i from c"},
		{"", "select /*+ HASH_JOIN(a,b) */ a.i from query_src a join query_src b on a.i=b.i"},
		{"", "select a.i from query_src_v a join query_cached b on a.i=b.i"},
		{"", "select i/7, v/3 from query_src"},
		{"", "select group_concat(i order by i) from query_src"},
		{"", "select @runtime_v + i from query_src"},
	}
	for _, c := range cases {
		sql := c.sql
		t.Run(c.sqlMode+"/"+sql, func(t *testing.T) {
			tk.MustExec("set sql_mode='" + c.sqlMode + "'")
			require.NoError(t, tk.Session().PrepareTxnCtx(context.Background(), nil))
			stmtSQL := "import into unused_target from (" + sql + ") with thread=1"
			require.NoError(t, importer.PlanImportQueryForTest(tk.Session(), stmtSQL))
			captured, err := importer.CaptureQuerySource(tk.Session(), stmtSQL)
			require.NoError(t, err)
			se, err := session.CreateSession4Test(store)
			require.NoError(t, err)
			defer se.Close()
			testfailpoint.EnableCall(t, "github.com/pingcap/tidb/pkg/executor/afterImportQueryOptimize", func(p base.PhysicalPlan) {
				vars := p.SCtx().GetSessionVars()
				require.EqualValues(t, 32<<20, vars.MemQuotaQuery)
				require.EqualValues(t, 32<<20, vars.MemTracker.GetBytesLimit())
				if strings.HasPrefix(sql, "select /*+") {
					require.True(t, vars.StmtCtx.StmtHints.QueryHasHints)
				}
				if strings.Contains(sql, "tidb_distsql_scan_concurrency=2") {
					require.Equal(t, 2, vars.DistSQLScanConcurrency())
				}
			})
			data, err := json.Marshal(captured)
			require.NoError(t, err)
			captured = &importer.QuerySource{}
			require.NoError(t, json.Unmarshal(data, captured))
			var expected []string
			for _, row := range tk.MustQuery(sql).Rows() {
				expected = append(expected, fmt.Sprint(row))
			}
			output := make(chan importer.QueryChunk, len(expected)+1)
			err = importer.RunImportQuery(context.Background(), se, captured, stmtSQL, 32<<20, output)
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
	t.Run("mv maintenance follows the submitter", func(t *testing.T) {
		sql := "import into unused_target from select i from query_src"
		for _, inMaintenance := range []bool{false, true} {
			submitVars := tk.Session().GetSessionVars()
			submitVars.InMViewMaintenance = inMaintenance
			require.NoError(t, tk.Session().PrepareTxnCtx(context.Background(), nil))
			require.NoError(t, importer.PlanImportQueryForTest(tk.Session(), sql))
			q, err := importer.CaptureQuerySource(tk.Session(), sql)
			submitVars.InMViewMaintenance = false
			require.NoError(t, err)
			require.Equal(t, inMaintenance, q.InMViewMaintenance)
			testfailpoint.EnableCall(t, "github.com/pingcap/tidb/pkg/executor/afterImportQueryOptimize", func(p base.PhysicalPlan) {
				require.Equal(t, inMaintenance, p.SCtx().GetSessionVars().InMViewMaintenance)
			})
			se, err := session.CreateSession4Test(store)
			require.NoError(t, err)
			output := make(chan importer.QueryChunk, 16)
			require.NoError(t, importer.RunImportQuery(context.Background(), se, q, sql, 1<<20, output))
			se.Close()
		}
	})
	t.Run("statement time", func(t *testing.T) {
		tk.MustExec("set @@timestamp = 1000000000")
		defer tk.MustExec("set @@timestamp = default")
		tk.MustExec("begin")
		sql := "import into unused_target from select unix_timestamp(now())"
		require.NoError(t, importer.PlanImportQueryForTest(tk.Session(), sql))
		q, err := importer.CaptureQuerySource(tk.Session(), sql)
		require.NoError(t, err)
		tk.MustExec("rollback")
		se, err := session.CreateSession4Test(store)
		require.NoError(t, err)
		defer se.Close()
		output := make(chan importer.QueryChunk, 1)
		require.NoError(t, importer.RunImportQuery(context.Background(), se, q, sql, 1<<20, output))
		close(output)
		result := <-output
		require.EqualValues(t, 1000000000, result.Chk.GetRow(0).GetInt64(0))
		// The submitter's transaction does not carry over to the worker.
		require.False(t, se.GetSessionVars().InTxn())
		require.True(t, se.GetSessionVars().IsAutocommit())
	})
	t.Run("statement time by default", func(t *testing.T) {
		require.NoError(t, tk.Session().PrepareTxnCtx(context.Background(), nil))
		sql := "import into unused_target from select unix_timestamp(now())"
		require.NoError(t, importer.PlanImportQueryForTest(tk.Session(), sql))
		q, err := importer.CaptureQuerySource(tk.Session(), sql)
		require.NoError(t, err)
		captured, err := strconv.ParseFloat(q.SessionStates.SystemVars[vardef.Timestamp], 64)
		require.NoError(t, err)
		// The worker runs the query later, but still at the time of the statement.
		time.Sleep(1100 * time.Millisecond)
		se, err := session.CreateSession4Test(store)
		require.NoError(t, err)
		defer se.Close()
		output := make(chan importer.QueryChunk, 1)
		require.NoError(t, importer.RunImportQuery(context.Background(), se, q, sql, 1<<20, output))
		close(output)
		result := <-output
		require.EqualValues(t, int64(captured), result.Chk.GetRow(0).GetInt64(0))
	})
	t.Run("source tables at read ts", func(t *testing.T) {
		tk.MustExec("create resource group rg1 ru_per_sec=1000")
		require.NoError(t, tk.Session().PrepareTxnCtx(context.Background(), nil))
		sql := "import into unused_target from select /*+ STREAM_AGG() */ count(*) from query_src"
		tk.Session().GetSessionVars().StmtCtx.ResourceGroupName = "rg1"
		require.NoError(t, importer.PlanImportQueryForTest(tk.Session(), sql))
		q, err := importer.CaptureQuerySource(tk.Session(), sql)
		require.NoError(t, err)
		data, err := json.Marshal(q)
		require.NoError(t, err)
		q = &importer.QuerySource{}
		require.NoError(t, json.Unmarshal(data, q))
		require.NotZero(t, q.SchemaVersion)
		testfailpoint.EnableCall(t, "github.com/pingcap/tidb/pkg/executor/afterImportQueryOptimize", func(p base.PhysicalPlan) {
			require.Equal(t, q.SchemaVersion, p.SCtx().GetSessionVars().SnapshotInfoschema.(infoschema.InfoSchema).SchemaMetaVersion())
			require.Equal(t, "rg1", p.SCtx().GetSessionVars().StmtCtx.ResourceGroupName)
		})
		tk.Session().RollbackTxn(context.Background())
		tk.MustExec("insert into query_src values (9,90,9)")
		tk.MustExec("rename table query_src to query_renamed")
		se, err := session.CreateSession4Test(store)
		require.NoError(t, err)
		defer se.Close()
		output := make(chan importer.QueryChunk, 1)
		err = importer.RunImportQuery(context.Background(), se, q, sql, 1<<20, output)
		require.NoError(t, err)
		close(output)
		var rows []int64
		for result := range output {
			for i := range result.Chk.NumRows() {
				rows = append(rows, result.Chk.GetRow(i).GetInt64(0))
			}
		}
		require.Equal(t, []int64{7}, rows)
		// Retrying the persisted query must keep the same snapshot.
		output = make(chan importer.QueryChunk, 1)
		require.NoError(t, importer.RunImportQuery(context.Background(), se, q, sql, 1<<20, output))
		close(output)
		require.Len(t, output, 1)
		for result := range output {
			require.EqualValues(t, 7, result.Chk.GetRow(0).GetInt64(0))
		}
	})
	t.Run("source table changed before capture", func(t *testing.T) {
		ddlTK := testkit.NewTestKit(t, store)
		ddlTK.MustExec("use test")
		ddlTK.MustExec("create table query_changed(i int)")
		ddlTK.MustExec("insert into query_changed values (1)")
		queryTK := testkit.NewTestKit(t, store)
		queryTK.MustExec("use test")
		queryTK.MustQuery("select * from query_changed").Check(testkit.Rows("1"))
		require.NoError(t, queryTK.Session().PrepareTxnCtx(context.Background(), nil))
		// Capture runs inside the IMPORT INTO statement, which is not read-only.
		queryTK.Session().GetSessionVars().StmtCtx.IsReadOnly = false
		ddlTK.MustExec("alter table query_changed add column j int default 42")
		sql := "import into unused_target from select * from query_changed"
		require.NoError(t, importer.PlanImportQueryForTest(queryTK.Session(), sql))
		q, err := importer.CaptureQuerySource(queryTK.Session(), sql)
		require.NoError(t, err)
		se, err := session.CreateSession4Test(store)
		require.NoError(t, err)
		defer se.Close()
		output := make(chan importer.QueryChunk, 1)
		// The planner of the statement takes the definition after the DDL, which
		// the worker reads the snapshot with.
		require.NoError(t, importer.RunImportQuery(context.Background(), se, q, sql, 1<<20, output))
		chk := (<-output).Chk
		require.Equal(t, 1, chk.NumRows())
		require.Equal(t, 2, chk.NumCols())
		require.Equal(t, int64(42), chk.GetRow(0).GetInt64(1))
	})
}
