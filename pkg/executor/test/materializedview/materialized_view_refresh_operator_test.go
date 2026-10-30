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

package materializedview

import (
	"context"
	"strconv"
	"testing"

	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/parser/ast"
	plannercore "github.com/pingcap/tidb/pkg/planner/core"
	"github.com/pingcap/tidb/pkg/planner/core/base"
	"github.com/pingcap/tidb/pkg/testkit"
	"github.com/stretchr/testify/require"
)

func TestMaterializedViewRefreshOperators(t *testing.T) {
	for _, fixedReadTSO := range []bool{false, true} {
		t.Run("fixed_read_tso="+strconv.FormatBool(fixedReadTSO), func(t *testing.T) {
			store := testkit.CreateMockStore(t)
			tk := newMViewTestKit(t, store)
			tk.MustExec("use test")
			tk.MustExec("create table t_refresh_ops (id int primary key, g int not null, v int, key(g))")
			tk.MustExec("create materialized view log on t_refresh_ops (id, g, v)")
			tk.MustExec("insert into t_refresh_ops values (1, 1, 10), (2, 1, 20), (3, 2, 30)")
			tk.MustExec("create materialized view mv_refresh_ops (g, cnt, vcnt, s, mn, mx) " +
				"refresh fast next date_add(now(), interval 1 hour) as " +
				"select g, count(*), count(v), sum(v), min(v), max(v) from t_refresh_ops group by g")
			tk.MustQuery("select * from mv_refresh_ops order by g").Check(testkit.Rows(
				"1 2 2 30 10 20", "2 1 1 30 30 30"))
			lastReadTSO, err := strconv.ParseUint(tk.MustQuery(
				"select last_success_read_tso from mysql.tidb_mview_refresh_info").Rows()[0][0].(string), 10, 64)
			require.NoError(t, err)

			tk.MustExec("delete from t_refresh_ops where id = 2")
			tk.MustExec("update t_refresh_ops set v = 5 where id = 3")
			tk.MustExec("insert into t_refresh_ops values (4, 1, null), (5, 3, 40)")
			var targetReadTSO uint64
			if fixedReadTSO {
				tk.MustExec("insert into mysql.tidb values ('tikv_gc_safe_point', '20060102-15:04:05 +0000', '')")
				tk.MustExec("begin")
				targetReadTSO, err = strconv.ParseUint(tk.MustQuery("select @@tidb_current_ts").Rows()[0][0].(string), 10, 64)
				require.NoError(t, err)
				require.NotZero(t, targetReadTSO)
				tk.MustExec("commit")
				// Recomputing MIN/MAX must use the target snapshot, even when a newer
				// value has committed before the internal maintenance statement starts.
				tk.MustExec("update t_refresh_ops set v = 1 where id = 3")
			}
			tk.MustExec("begin pessimistic")
			// Execute the internal AST directly to inspect operator statistics
			// independently of the refresh lifecycle.
			plan, stats := executeRefreshOperator(t, tk, &ast.RefreshMaterializedViewImplementStmt{
				RefreshStmt: &ast.RefreshMaterializedViewStmt{
					ViewName: &ast.TableName{Name: ast.NewCIStr("mv_refresh_ops")},
					Type:     ast.RefreshMaterializedViewTypeFast,
				},
				LastSuccessfulRefreshReadTSO: lastReadTSO,
				TargetRefreshReadTSO:         targetReadTSO,
			})
			require.IsType(t, &plannercore.MViewDeltaMerge{}, plan)
			require.Contains(t, stats, "mview_delta_merge_agg")
			require.Contains(t, stats, "rows:{insert:1, update:2, delete:0, noop:0}")
			tk.MustExec("commit")
			tk.MustQuery("select * from mv_refresh_ops order by g").Check(testkit.Rows(
				"1 2 1 10 10 10", "2 1 1 5 5 5", "3 1 1 40 40 40"))

			tk.MustExec("delete from t_refresh_ops where id in (1, 3)")
			tk.MustExec("insert into t_refresh_ops values (6, 3, 60), (7, 4, 7)")
			tk.MustExec("begin pessimistic")
			plan, stats = executeRefreshOperator(t, tk, &ast.RefreshMaterializedViewImplementStmt{
				RefreshStmt: &ast.RefreshMaterializedViewStmt{
					ViewName:     &ast.TableName{Name: ast.NewCIStr("mv_refresh_ops")},
					Type:         ast.RefreshMaterializedViewTypeComplete,
					CompleteType: ast.RefreshMaterializedViewCompleteTypeDeltaApply,
				},
			})
			require.IsType(t, &plannercore.MViewCompleteDeltaApply{}, plan)
			require.Contains(t, stats, "mview_complete_delta_apply")
			require.Contains(t, stats, "rows:{insert:1, update:2, delete:1}")
			tk.MustExec("commit")
			tk.MustQuery("select * from mv_refresh_ops order by g").Check(testkit.Rows(
				"1 1 0 <nil> <nil> <nil>", "3 2 2 100 40 60", "4 1 1 7 7 7"))
		})
	}
}

func executeRefreshOperator(t *testing.T, tk *testkit.TestKit, stmt *ast.RefreshMaterializedViewImplementStmt) (base.Plan, string) {
	t.Helper()
	ctx := kv.WithInternalSourceType(context.Background(), kv.InternalTxnMViewMaintenance)
	vars := tk.Session().GetSessionVars()
	origMaint, origRestricted := vars.InMViewMaintenance, vars.InRestrictedSQL
	vars.InMViewMaintenance, vars.InRestrictedSQL = true, true
	defer func() {
		vars.InMViewMaintenance, vars.InRestrictedSQL = origMaint, origRestricted
	}()
	rs, err := tk.Session().ExecuteStmt(ctx, stmt)
	require.NoError(t, err)
	if rs != nil {
		defer func() { require.NoError(t, rs.Close()) }()
		chk := rs.NewChunk(nil)
		for {
			require.NoError(t, rs.Next(ctx, chk))
			if chk.NumRows() == 0 {
				break
			}
			chk.Reset()
		}
	}
	processInfo := tk.Session().ShowProcess()
	require.NotNil(t, processInfo)
	plan, ok := processInfo.Plan.(base.Plan)
	require.True(t, ok)
	require.NotNil(t, vars.StmtCtx.RuntimeStatsColl)
	return plan, vars.StmtCtx.RuntimeStatsColl.GetRootStats(plan.ID()).String()
}
