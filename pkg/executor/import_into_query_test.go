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
	"testing"
	"time"

	"github.com/pingcap/tidb/pkg/executor/importer"
	"github.com/pingcap/tidb/pkg/infoschema"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/planner/core/base"
	"github.com/pingcap/tidb/pkg/session"
	"github.com/pingcap/tidb/pkg/sessionctx"
	"github.com/pingcap/tidb/pkg/sessionctx/vardef"
	"github.com/pingcap/tidb/pkg/sessionctx/variable"
	"github.com/pingcap/tidb/pkg/testkit"
	"github.com/pingcap/tidb/pkg/testkit/testfailpoint"
	"github.com/stretchr/testify/require"
)

const importQueryMemQuota = 32 << 20

// captureImportQuery captures the IMPORT INTO statement sql that tk runs, and
// returns it as a worker decodes it from the task meta.
func captureImportQuery(t *testing.T, tk *testkit.TestKit, sql string) *importer.QuerySource {
	require.NoError(t, importer.PlanImportQueryForTest(tk.Session(), sql))
	q, err := importer.CaptureQuerySource(tk.Session(), sql)
	require.NoError(t, err)
	data, err := json.Marshal(q)
	require.NoError(t, err)
	q = &importer.QuerySource{}
	require.NoError(t, json.Unmarshal(data, q))
	return q
}

// newWorkerSession returns a session to run import queries in, like a worker.
func newWorkerSession(t *testing.T, store kv.Storage) sessionctx.Context {
	se, err := session.CreateSession4Test(store)
	require.NoError(t, err)
	t.Cleanup(se.Close)
	return se
}

// runImportQuery runs the captured query q in se, and returns its rows.
func runImportQuery(t *testing.T, se sessionctx.Context, q *importer.QuerySource, sql string) []string {
	output := make(chan importer.QueryChunk)
	errCh := make(chan error, 1)
	go func() {
		errCh <- importer.RunImportQuery(context.Background(), se, q, sql, importQueryMemQuota, output)
		close(output)
	}()
	var rows []string
	for result := range output {
		for i := range result.Chk.NumRows() {
			row := result.Chk.GetRow(i).GetDatumRow(result.Fields)
			values := make([]string, len(row))
			for j, d := range row {
				if d.IsNull() {
					values[j] = "<nil>"
					continue
				}
				var err error
				values[j], err = d.ToString()
				require.NoError(t, err)
			}
			rows = append(rows, fmt.Sprint(values))
		}
	}
	require.NoError(t, <-errCh)
	return rows
}

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
	hinted := func(t *testing.T, vars *variable.SessionVars) {
		require.True(t, vars.StmtCtx.StmtHints.QueryHasHints)
	}
	cases := []struct {
		sqlMode, sql string
		check        func(*testing.T, *variable.SessionVars)
	}{
		{sql: "select i from (select i from query_src where i>0) c"},
		{sql: `select hex(_binary'\0\\a'), _latin1'a', 'a''b'`},
		{sqlMode: "NO_BACKSLASH_ESCAPES", sql: `select hex(_binary'\0\\a'), _latin1'a', 'a''b'`},
		{sql: "select /*+ STREAM_AGG() SET_VAR(tidb_distsql_scan_concurrency=2) */ count(*) from query_src", check: func(t *testing.T, vars *variable.SessionVars) {
			hinted(t, vars)
			require.Equal(t, 2, vars.DistSQLScanConcurrency())
		}},
		// The quota of the worker overrides the quota of the submitter and hints.
		{sql: "select /*+ STREAM_AGG() SET_VAR(tidb_mem_quota_query=1073741824) */ count(*) from query_src", check: hinted},
		{sql: "select /*+ STREAM_AGG() MEMORY_QUOTA(1 GB) */ count(*) from query_src", check: hinted},
		{sql: "select /*+ STREAM_AGG() */ i,count(*),sum(v) from query_src use index(idx_i) where round(v,2)>5 group by i", check: hinted},
		{sql: "with c as (select /*+ MERGE() */ i from query_src) select i from c"},
		{sql: "select /*+ HASH_JOIN(a,b) */ a.i from query_src a join query_src b on a.i=b.i", check: hinted},
		{sql: "select a.i from query_src_v a join query_cached b on a.i=b.i"},
		{sql: "select i/7, v/3 from query_src"},
		{sql: "select group_concat(i order by i) from query_src"},
		{sql: "select @runtime_v + i from query_src"},
	}
	for _, c := range cases {
		t.Run(c.sqlMode+"/"+c.sql, func(t *testing.T) {
			tk.MustExec("set sql_mode='" + c.sqlMode + "'")
			require.NoError(t, tk.Session().PrepareTxnCtx(context.Background(), nil))
			sql := "import into unused_target from (" + c.sql + ") with thread=1"
			q := captureImportQuery(t, tk, sql)
			testfailpoint.EnableCall(t, "github.com/pingcap/tidb/pkg/executor/afterImportQueryOptimize", func(p base.PhysicalPlan) {
				vars := p.SCtx().GetSessionVars()
				require.EqualValues(t, importQueryMemQuota, vars.MemQuotaQuery)
				require.EqualValues(t, importQueryMemQuota, vars.MemTracker.GetBytesLimit())
				if c.check != nil {
					c.check(t, vars)
				}
			})
			var expected []string
			for _, row := range tk.MustQuery(c.sql).Rows() {
				expected = append(expected, fmt.Sprint(row))
			}
			got := runImportQuery(t, newWorkerSession(t, store), q, sql)
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
			q := captureImportQuery(t, tk, sql)
			submitVars.InMViewMaintenance = false
			require.Equal(t, inMaintenance, q.InMViewMaintenance)
			testfailpoint.EnableCall(t, "github.com/pingcap/tidb/pkg/executor/afterImportQueryOptimize", func(p base.PhysicalPlan) {
				require.Equal(t, inMaintenance, p.SCtx().GetSessionVars().InMViewMaintenance)
			})
			runImportQuery(t, newWorkerSession(t, store), q, sql)
		}
	})
	t.Run("statement time", func(t *testing.T) {
		sql := "import into unused_target from select unix_timestamp(now())"
		tk.MustExec("set @@timestamp = 1000000000")
		tk.MustExec("begin")
		q := captureImportQuery(t, tk, sql)
		tk.MustExec("rollback")
		tk.MustExec("set @@timestamp = default")
		se := newWorkerSession(t, store)
		require.Equal(t, []string{"[1000000000]"}, runImportQuery(t, se, q, sql))
		// The submitter's transaction does not carry over to the worker.
		require.False(t, se.GetSessionVars().InTxn())
		require.True(t, se.GetSessionVars().IsAutocommit())

		// Without @@timestamp, the worker still runs at the time of the statement.
		require.NoError(t, tk.Session().PrepareTxnCtx(context.Background(), nil))
		q = captureImportQuery(t, tk, sql)
		captured, err := strconv.ParseFloat(q.SessionStates.SystemVars[vardef.Timestamp], 64)
		require.NoError(t, err)
		time.Sleep(1100 * time.Millisecond)
		require.Equal(t, []string{fmt.Sprintf("[%d]", int64(captured))}, runImportQuery(t, newWorkerSession(t, store), q, sql))
	})
	t.Run("source tables at read ts", func(t *testing.T) {
		tk.MustExec("create resource group rg1 ru_per_sec=1000")
		require.NoError(t, tk.Session().PrepareTxnCtx(context.Background(), nil))
		sql := "import into unused_target from select /*+ STREAM_AGG() */ count(*) from query_src"
		tk.Session().GetSessionVars().StmtCtx.ResourceGroupName = "rg1"
		q := captureImportQuery(t, tk, sql)
		require.NotZero(t, q.SchemaVersion)
		testfailpoint.EnableCall(t, "github.com/pingcap/tidb/pkg/executor/afterImportQueryOptimize", func(p base.PhysicalPlan) {
			require.Equal(t, q.SchemaVersion, p.SCtx().GetSessionVars().SnapshotInfoschema.(infoschema.InfoSchema).SchemaMetaVersion())
			require.Equal(t, "rg1", p.SCtx().GetSessionVars().StmtCtx.ResourceGroupName)
		})
		tk.Session().RollbackTxn(context.Background())
		tk.MustExec("insert into query_src values (9,90,9)")
		tk.MustExec("rename table query_src to query_renamed")
		se := newWorkerSession(t, store)
		require.Equal(t, []string{"[7]"}, runImportQuery(t, se, q, sql))
		// Retrying the persisted query must keep the same snapshot.
		require.Equal(t, []string{"[7]"}, runImportQuery(t, se, q, sql))
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
		q := captureImportQuery(t, queryTK, sql)
		// The planner of the statement takes the definition after the DDL, which
		// the worker reads the snapshot with.
		require.Equal(t, []string{"[1 42]"}, runImportQuery(t, newWorkerSession(t, store), q, sql))
	})
}
