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

package mviewnested

import (
	"context"
	"fmt"
	"testing"

	"github.com/pingcap/failpoint"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/testkit"
	"github.com/pingcap/tidb/pkg/testkit/testfailpoint"
	"github.com/stretchr/testify/require"
)

func newMViewRefreshSourceFixture(t *testing.T) (kv.Storage, *testkit.TestKit, int64, int64) {
	t.Helper()
	store, dom := testkit.CreateMockStoreAndDomain(t)
	tk := newMViewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("create table t_source_init (a int not null, b int not null)")
	tk.MustExec("insert into t_source_init values (1, 10), (2, 20)")
	tk.MustExec("create materialized view log on t_source_init (a, b)")
	tk.MustExec("create materialized view mv_source_init (a, s, cnt) refresh fast as select a, sum(b), count(*) from t_source_init group by a")
	mv, err := dom.InfoSchema().TableByName(context.Background(), ast.NewCIStr("test"), ast.NewCIStr("mv_source_init"))
	require.NoError(t, err)
	base, err := dom.InfoSchema().TableByName(context.Background(), ast.NewCIStr("test"), ast.NewCIStr("t_source_init"))
	require.NoError(t, err)
	return store, tk, mv.Meta().ID, base.Meta().MaterializedViewBase.MLogID
}

func TestNestedMViewRefreshSourceInfoLifecycle(t *testing.T) {
	store, dom := testkit.CreateMockStoreAndDomain(t)
	tk := newMViewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("create table t_sources (a int not null, b int not null)")
	tk.MustExec("insert into t_sources values (1, 10), (2, 20)")
	tk.MustExec("create materialized view log on t_sources (a, b)")
	tk.MustExec("create materialized view mv_sources (a, s, cnt) refresh fast as select a, sum(b), count(*) from t_sources group by a")
	tk.MustExec("create materialized view log on mv_sources (a, s, cnt)")
	tk.MustExec("create materialized view mv_sources_child (a, cnt) refresh fast as select a, count(*) from mv_sources group by a")
	getID := func(name string) int64 {
		tbl, err := dom.InfoSchema().TableByName(context.Background(), ast.NewCIStr("test"), ast.NewCIStr(name))
		require.NoError(t, err)
		return tbl.Meta().ID
	}
	mvID, childID := getID("mv_sources"), getID("mv_sources_child")
	mlogID, ownLogID := getID("$mlog$t_sources"), getID("$mlog$mv_sources")
	tk.MustQuery("select MVIEW_ID, MLOG_ID from mysql.tidb_mview_fast_refresh_source_info order by MVIEW_ID").Check(testkit.Rows(
		fmt.Sprintf("%d %d", mvID, mlogID), fmt.Sprintf("%d %d", childID, ownLogID),
	))
	tk.MustExec("drop materialized view mv_sources_child")
	tk.MustQuery("select MVIEW_ID, MLOG_ID from mysql.tidb_mview_fast_refresh_source_info use index(idx_mlog)").Check(testkit.Rows(fmt.Sprintf("%d %d", mvID, mlogID)))
	tk.MustExec("drop materialized view log on mv_sources")
	tk.MustQuery("select MVIEW_ID, MLOG_ID from mysql.tidb_mview_fast_refresh_source_info").Check(testkit.Rows(fmt.Sprintf("%d %d", mvID, mlogID)))
	tk.MustExec("drop materialized view mv_sources")
	tk.MustQuery("select count(*) from mysql.tidb_mview_fast_refresh_source_info").Check(testkit.Rows("0"))
}

func TestNestedMViewRefreshSourceInfoRecoveryAtomicity(t *testing.T) {
	_, tk, mviewID, _ := newMViewRefreshSourceFixture(t)
	tk.MustExec("create materialized view log on mv_source_init (a, s, cnt)")
	tk.MustExec("create materialized view mv_source_child (a, cnt) refresh fast as select a, count(*) from mv_source_init group by a")
	tk.MustExec("create materialized view mv_source_peer (a, s, cnt) refresh fast as select a, sum(b), count(*) from t_source_init group by a")
	tk.MustExec(fmt.Sprintf("update mysql.tidb_mview_fast_refresh_source_info set MLOG_ID = -1 where MVIEW_ID = %d", mviewID))
	before := tk.MustQuery("select * from mysql.tidb_mview_refresh_info order by MVIEW_ID").Rows()
	beforeSources := tk.MustQuery("select * from mysql.tidb_mview_fast_refresh_source_info order by MVIEW_ID, MLOG_ID").Rows()
	tk.MustExec("insert into t_source_init values (1, 5), (3, 7)")
	testfailpoint.Enable(t, "github.com/pingcap/tidb/pkg/executor/mockRefreshMaterializedViewErrorAfterDataChanges", `return("source recovery rollback")`)
	require.ErrorContains(t, tk.ExecToErr("refresh materialized view mv_source_init complete delta apply"), "source recovery rollback")
	require.NoError(t, failpoint.Disable("github.com/pingcap/tidb/pkg/executor/mockRefreshMaterializedViewErrorAfterDataChanges"))
	require.Equal(t, before, tk.MustQuery("select * from mysql.tidb_mview_refresh_info order by MVIEW_ID").Rows())
	require.Equal(t, beforeSources, tk.MustQuery("select * from mysql.tidb_mview_fast_refresh_source_info order by MVIEW_ID, MLOG_ID").Rows())
	tk.MustQuery("select * from mv_source_init order by a").Check(testkit.Rows("1 10 1", "2 20 1"))
	testfailpoint.Enable(t, "github.com/pingcap/tidb/pkg/executor/mockRefreshMaterializedViewAfterReplaceSourceInfoError", "return()")
	require.ErrorContains(t, tk.ExecToErr("refresh materialized view mv_source_init complete delta apply"), "after replacing source info")
	require.NoError(t, failpoint.Disable("github.com/pingcap/tidb/pkg/executor/mockRefreshMaterializedViewAfterReplaceSourceInfoError"))
	require.Equal(t, before, tk.MustQuery("select * from mysql.tidb_mview_refresh_info order by MVIEW_ID").Rows())
	require.Equal(t, beforeSources, tk.MustQuery("select * from mysql.tidb_mview_fast_refresh_source_info order by MVIEW_ID, MLOG_ID").Rows())
	tk.MustQuery("select * from mv_source_init order by a").Check(testkit.Rows("1 10 1", "2 20 1"))
	// A successful COMPLETE preserves the output log, so the child still FAST
	// refreshes. The peer's input baseline and watermark are unchanged.
	peerBefore := tk.MustQuery("select * from mysql.tidb_mview_refresh_info where MVIEW_ID = (select TIDB_TABLE_ID from information_schema.tables where table_schema='test' and table_name='mv_source_peer')").Rows()
	var computationTS uint64
	testfailpoint.EnableCall(t, "github.com/pingcap/tidb/pkg/executor/refreshMaterializedViewAfterReplaceSourceInfo", func(ts uint64) { computationTS = ts })
	tk.MustExec("refresh materialized view mv_source_init complete delta apply")
	require.NotZero(t, computationTS)
	tk.MustQuery(fmt.Sprintf("select LAST_SUCCESS_READ_TSO from mysql.tidb_mview_refresh_info where MVIEW_ID = %d", mviewID)).Check(testkit.Rows(fmt.Sprint(computationTS)))
	require.Equal(t, peerBefore, tk.MustQuery("select * from mysql.tidb_mview_refresh_info where MVIEW_ID = (select TIDB_TABLE_ID from information_schema.tables where table_schema='test' and table_name='mv_source_peer')").Rows())
	tk.MustExec("refresh materialized view mv_source_child fast")
	tk.MustQuery("select * from mv_source_child order by a").Check(testkit.Rows("1 1", "2 1", "3 1"))

	t.Run("child_baseline_recovery", func(t *testing.T) {
		const childID = "(select TIDB_TABLE_ID from information_schema.tables where table_schema='test' and table_name='mv_source_child')"
		tk.MustExec("update mysql.tidb_mview_fast_refresh_source_info set MLOG_ID = -1 where MVIEW_ID = " + childID)
		tk.MustExec("insert into t_source_init values (4, 11)")
		tk.MustExec("refresh materialized view mv_source_init fast")
		beforeChild := tk.MustQuery("select * from mv_source_child order by a").Rows()
		beforeCheckpoint := tk.MustQuery("select LAST_SUCCESS_READ_TSO from mysql.tidb_mview_refresh_info where MVIEW_ID = " + childID).Rows()
		require.ErrorContains(t, tk.ExecToErr("refresh materialized view mv_source_child fast"), "run COMPLETE refresh")
		require.Equal(t, beforeChild, tk.MustQuery("select * from mv_source_child order by a").Rows())
		require.Equal(t, beforeCheckpoint, tk.MustQuery("select LAST_SUCCESS_READ_TSO from mysql.tidb_mview_refresh_info where MVIEW_ID = "+childID).Rows())
		tk.MustExec("refresh materialized view mv_source_child complete delta apply")
		tk.MustQuery("select MLOG_ID from mysql.tidb_mview_fast_refresh_source_info where MVIEW_ID = " + childID).
			Check(tk.MustQuery("select MLOG_ID from information_schema.tidb_mlogs where table_schema='test' and base_table_name='mv_source_init'").Rows())
		tk.MustQuery("select * from mv_source_child order by a").Check(testkit.Rows("1 1", "2 1", "3 1", "4 1"))
		tk.MustExec("insert into t_source_init values (5, 13)")
		tk.MustExec("refresh materialized view mv_source_init fast")
		tk.MustExec("refresh materialized view mv_source_child fast")
		tk.MustQuery("select * from mv_source_child order by a").Check(testkit.Rows("1 1", "2 1", "3 1", "4 1", "5 1"))
	})
}
