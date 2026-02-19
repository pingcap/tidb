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

package ddl_test

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/pingcap/failpoint"
	pmodel "github.com/pingcap/tidb/pkg/parser/model"
	"github.com/pingcap/tidb/pkg/testkit"
	"github.com/stretchr/testify/require"
)

func TestMaterializedViewDDLBasic(t *testing.T) {
	store, dom := testkit.CreateMockStoreAndDomain(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("create table t (a int not null, b int not null)")

	// Base table must have MV LOG first.
	err := tk.ExecToErr("create materialized view mv_no_log (a, cnt) as select a, count(1) from t group by a")
	require.ErrorContains(t, err, "materialized view log does not exist")

	tk.MustExec("insert into t values (1, 10), (1, 5), (2, 7)")
	tk.MustExec("create materialized view log on t (a, b) purge immediate")
	tk.MustExec("create materialized view mv (a, s, cnt) refresh fast next 300 as select a, sum(b), count(1) from t group by a")
	tk.MustQuery("select a, s, cnt from mv order by a").Check(testkit.Rows("1 15 2", "2 7 1"))

	// Physical table created.
	tk.MustQuery("select count(*) from information_schema.tables where table_schema='test' and table_name='mv'").Check(testkit.Rows("1"))

	showCreate := tk.MustQuery("show create table mv").Rows()[0][1].(string)
	require.Contains(t, showCreate, "PRIMARY KEY (`a`)")

	is := dom.InfoSchema()
	baseTable, err := is.TableByName(context.Background(), pmodel.NewCIStr("test"), pmodel.NewCIStr("t"))
	require.NoError(t, err)
	mlogTable, err := is.TableByName(context.Background(), pmodel.NewCIStr("test"), pmodel.NewCIStr("$mlog$t"))
	require.NoError(t, err)
	mvTable, err := is.TableByName(context.Background(), pmodel.NewCIStr("test"), pmodel.NewCIStr("mv"))
	require.NoError(t, err)

	require.NotNil(t, mvTable.Meta().MaterializedView)
	require.Equal(t, []int64{baseTable.Meta().ID}, mvTable.Meta().MaterializedView.BaseTableIDs)
	require.Equal(t, "FAST", mvTable.Meta().MaterializedView.RefreshMethod)
	require.Equal(t, "NOW()", mvTable.Meta().MaterializedView.RefreshStartWith)
	require.Equal(t, "300", mvTable.Meta().MaterializedView.RefreshNext)
	tk.MustQuery(fmt.Sprintf("select LAST_SUCCESS_READ_TSO > 0, NEXT_TIME is not null from mysql.tidb_mview_refresh where MVIEW_ID = %d", mvTable.Meta().ID)).
		Check(testkit.Rows("1 1"))

	// Base table reverse mapping maintained by DDL.
	require.NotNil(t, baseTable.Meta().MaterializedViewBase)
	require.Equal(t, mlogTable.Meta().ID, baseTable.Meta().MaterializedViewBase.MLogID)
	require.Contains(t, baseTable.Meta().MaterializedViewBase.MViewIDs, mvTable.Meta().ID)

	// MV must contain count(*|1).
	err = tk.ExecToErr("create materialized view mv_bad (a, s) as select a, sum(b) from t group by a")
	require.ErrorContains(t, err, "must contain count(*)/count(1)")

	// Count(column), non-deterministic WHERE and unsupported aggregate should fail.
	err = tk.ExecToErr("create materialized view mv_bad_count_col (a, c) as select a, count(b) from t group by a")
	require.ErrorContains(t, err, "only supports count(*)/count(1)")
	err = tk.ExecToErr("create materialized view mv_bad_avg (a, avgv, c) as select a, avg(b), count(1) from t group by a")
	require.ErrorContains(t, err, "unsupported aggregate function")
	err = tk.ExecToErr("create materialized view mv_bad_where (a, c) as select a, count(1) from t where rand() > 0 group by a")
	require.ErrorContains(t, err, "WHERE clause must be deterministic")

	// SUM/MIN/MAX currently require NOT NULL argument columns.
	tk.MustExec("create table t_sum_nullable (a int not null, b int)")
	tk.MustExec("create materialized view log on t_sum_nullable (a, b) purge immediate")
	err = tk.ExecToErr("create materialized view mv_bad_sum_nullable (a, s, c) as select a, sum(b), count(1) from t_sum_nullable group by a")
	require.ErrorContains(t, err, "only supports SUM/MIN/MAX on NOT NULL column")

	// MIN/MAX requires a base-table index whose leading columns cover all GROUP BY columns.
	tk.MustExec("create table t_minmax_bad (a int not null, b int not null, c int not null, index idx_cab(c, a, b))")
	tk.MustExec("create materialized view log on t_minmax_bad (a, b, c) purge immediate")
	err = tk.ExecToErr("create materialized view mv_bad_minmax_index (a, b, minc, c1) as select a, b, min(c), count(1) from t_minmax_bad group by a, b")
	require.ErrorContains(t, err, "requires base table index whose leading columns cover all GROUP BY columns")
	tk.MustExec("create table t_minmax_ok (a int not null, b int not null, c int not null, index idx_bac(b, a, c))")
	tk.MustExec("create materialized view log on t_minmax_ok (a, b, c) purge immediate")
	tk.MustExec("create materialized view mv_minmax_ok (a, b, minc, c1) as select a, b, min(c), count(1) from t_minmax_ok group by a, b")

	// SELECT clauses outside Stage-1 scope should be rejected.
	err = tk.ExecToErr("create materialized view mv_bad_distinct (a, c) as select distinct a, count(1) from t group by a")
	require.ErrorContains(t, err, "does not support SELECT DISTINCT")
	err = tk.ExecToErr("create materialized view mv_bad_having (a, c) as select a, count(1) from t group by a having count(1) > 0")
	require.ErrorContains(t, err, "does not support HAVING clause")
	err = tk.ExecToErr("create materialized view mv_bad_order (a, c) as select a, count(1) from t group by a order by a")
	require.ErrorContains(t, err, "does not support ORDER BY clause")
	err = tk.ExecToErr("create materialized view mv_bad_limit (a, c) as select a, count(1) from t group by a limit 1")
	require.ErrorContains(t, err, "does not support LIMIT clause")
	err = tk.ExecToErr("create materialized view mv_bad_rollup (a, c) as select a, count(1) from t group by a with rollup")
	require.ErrorContains(t, err, "does not support GROUP BY WITH ROLLUP")

	// Aliased base table in WHERE should work.
	tk.MustExec("create materialized view mv_alias (a, c) as select x.a, count(1) from t x where x.b > 0 group by x.a")
	tk.MustQuery("select a, c from mv_alias order by a").Check(testkit.Rows("1 2", "2 1"))

	// MV LOG must contain all referenced columns.
	tk.MustExec("create table t_mlog_missing (a int not null, b int not null)")
	tk.MustExec("create materialized view log on t_mlog_missing (a) purge immediate")
	err = tk.ExecToErr("create materialized view mv_bad_mlog_cols (a, s, c) as select a, sum(b), count(1) from t_mlog_missing group by a")
	require.ErrorContains(t, err, "does not contain column b")

	// Nullable group-by key should use UNIQUE KEY.
	tk.MustExec("create table t_nullable (a int, b int)")
	tk.MustExec("create materialized view log on t_nullable (a, b) purge immediate")
	tk.MustExec("create materialized view mv_nullable (a, c) as select a, count(1) from t_nullable group by a")
	showCreate = tk.MustQuery("show create table mv_nullable").Rows()[0][1].(string)
	require.Contains(t, showCreate, "UNIQUE KEY")
	require.False(t, strings.Contains(showCreate, "PRIMARY KEY (`a`)"))

	// Index DDL on MV-related tables: base remains forbidden, MV table is allowed.
	err = tk.ExecToErr("create index idx_forbidden_base on t (b)")
	require.ErrorContains(t, err, "CREATE INDEX on base table with materialized view dependencies")
	err = tk.ExecToErr("drop index idx_bac on t_minmax_ok")
	require.ErrorContains(t, err, "DROP INDEX on base table with materialized view dependencies")
	tk.MustExec("create index idx_mv_s on mv (s)")
	tk.MustExec("drop index idx_mv_s on mv")

	// ALTER TABLE ... SET TIFLASH REPLICA is allowed on MV table, but still forbidden on base.
	err = tk.ExecToErr("alter table t set tiflash replica 1")
	require.ErrorContains(t, err, "ALTER TABLE on base table with materialized view dependencies")
	err = tk.ExecToErr("alter table mv set tiflash replica 1")
	if err != nil {
		require.NotContains(t, err.Error(), "ALTER TABLE on materialized view table")
	}

	// MV LOG cannot be dropped while dependent MVs exist.
	err = tk.ExecToErr("drop materialized view log on t")
	require.ErrorContains(t, err, "dependent materialized views exist")

	// Drop MV and then drop MV LOG.
	tk.MustExec("drop materialized view mv")
	tk.MustExec("drop materialized view mv_alias")
	tk.MustExec("drop materialized view mv_nullable")
	tk.MustExec("drop materialized view mv_minmax_ok")
	tk.MustExec("drop materialized view log on t")
	tk.MustExec("drop materialized view log on t_nullable")
	tk.MustExec("drop materialized view log on t_sum_nullable")
	tk.MustExec("drop materialized view log on t_minmax_bad")
	tk.MustExec("drop materialized view log on t_minmax_ok")
	tk.MustQuery("select count(*) from mysql.tidb_mview_refresh").Check(testkit.Rows("0"))

	// Reverse mapping cleared.
	is = dom.InfoSchema()
	baseTable, err = is.TableByName(context.Background(), pmodel.NewCIStr("test"), pmodel.NewCIStr("t"))
	require.NoError(t, err)
	require.True(t, baseTable.Meta().MaterializedViewBase == nil || (baseTable.Meta().MaterializedViewBase.MLogID == 0 && len(baseTable.Meta().MaterializedViewBase.MViewIDs) == 0))
}

func TestCreateMaterializedViewBuildFailureRollback(t *testing.T) {
	store, dom := testkit.CreateMockStoreAndDomain(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("create table t (a int not null, b int not null)")
	tk.MustExec("insert into t values (1, 10), (1, 5), (2, 7)")
	tk.MustExec("create materialized view log on t (a, b) purge immediate")

	require.NoError(t, failpoint.Enable("github.com/pingcap/tidb/pkg/ddl/mockCreateMaterializedViewBuildErr", "return"))
	defer func() {
		require.NoError(t, failpoint.Disable("github.com/pingcap/tidb/pkg/ddl/mockCreateMaterializedViewBuildErr"))
	}()

	err := tk.ExecToErr("create materialized view mv_fail (a, s, cnt) refresh fast next 300 as select a, sum(b), count(1) from t group by a")
	require.Error(t, err)

	tk.MustQuery("show tables like 'mv_fail'").Check(testkit.Rows())
	tk.MustQuery("select count(*) from mysql.tidb_mview_refresh").Check(testkit.Rows("0"))

	is := dom.InfoSchema()
	baseTable, err := is.TableByName(context.Background(), pmodel.NewCIStr("test"), pmodel.NewCIStr("t"))
	require.NoError(t, err)
	require.NotNil(t, baseTable.Meta().MaterializedViewBase)
	require.NotZero(t, baseTable.Meta().MaterializedViewBase.MLogID)
	require.Empty(t, baseTable.Meta().MaterializedViewBase.MViewIDs)
}

func TestCreateMaterializedViewBuildContextCanceledRollback(t *testing.T) {
	store, dom := testkit.CreateMockStoreAndDomain(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("create table t (a int not null, b int not null)")
	tk.MustExec("insert into t values (1, 10), (1, 5), (2, 7)")
	tk.MustExec("create materialized view log on t (a, b) purge immediate")

	require.NoError(t, failpoint.Enable("github.com/pingcap/tidb/pkg/ddl/mockCreateMaterializedViewBuildErr", `return("context-canceled")`))
	defer func() {
		require.NoError(t, failpoint.Disable("github.com/pingcap/tidb/pkg/ddl/mockCreateMaterializedViewBuildErr"))
	}()

	err := tk.ExecToErr("create materialized view mv_ctx_cancel (a, s, cnt) refresh fast next 300 as select a, sum(b), count(1) from t group by a")
	require.Error(t, err)

	tk.MustQuery("show tables like 'mv_ctx_cancel'").Check(testkit.Rows())
	tk.MustQuery("select count(*) from mysql.tidb_mview_refresh").Check(testkit.Rows("0"))

	is := dom.InfoSchema()
	baseTable, err := is.TableByName(context.Background(), pmodel.NewCIStr("test"), pmodel.NewCIStr("t"))
	require.NoError(t, err)
	require.NotNil(t, baseTable.Meta().MaterializedViewBase)
	require.NotZero(t, baseTable.Meta().MaterializedViewBase.MLogID)
	require.Empty(t, baseTable.Meta().MaterializedViewBase.MViewIDs)
}

func TestCreateMaterializedViewRejectNonBaseObject(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("create table t (a int)")
	tk.MustExec("create materialized view log on t (a)")
	tk.MustExec("create view v as select a from t")

	err := tk.ExecToErr("create materialized view mv_v (a, c) as select a, count(1) from v group by a")
	require.ErrorContains(t, err, "is not BASE TABLE")
}

func TestCreateMaterializedViewCancelRollback(t *testing.T) {
	store, dom := testkit.CreateMockStoreAndDomain(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("create table t (a int not null, b int not null)")
	tk.MustExec("insert into t values (1, 10), (1, 5), (2, 7)")
	tk.MustExec("create materialized view log on t (a, b) purge immediate")

	tkCancel := testkit.NewTestKit(t, store)
	tkCancel.MustExec("use test")

	const pauseBuildFailpoint = "github.com/pingcap/tidb/pkg/ddl/pauseCreateMaterializedViewBuild"
	require.NoError(t, failpoint.Enable(pauseBuildFailpoint, "pause"))
	enabled := true
	defer func() {
		if enabled {
			require.NoError(t, failpoint.Disable(pauseBuildFailpoint))
		}
	}()

	ddlDone := make(chan error, 1)
	go func() {
		tkDDL := testkit.NewTestKit(t, store)
		tkDDL.MustExec("use test")
		ddlDone <- tkDDL.ExecToErr("create materialized view mv_cancel (a, s, cnt) refresh fast next 300 as select a, sum(b), count(1) from t group by a")
	}()

	jobID := ""
	require.Eventually(t, func() bool {
		rows := tkCancel.MustQuery("admin show ddl jobs where JOB_TYPE='create materialized view'").Rows()
		if len(rows) == 0 {
			return false
		}
		if len(rows[0]) < 5 || strings.ToLower(fmt.Sprint(rows[0][4])) != "write reorganization" {
			return false
		}
		jobID = rows[0][0].(string)
		return jobID != ""
	}, 30*time.Second, 100*time.Millisecond)

	tkCancel.MustExec("admin cancel ddl jobs " + jobID)
	require.NoError(t, failpoint.Disable(pauseBuildFailpoint))
	enabled = false

	err := <-ddlDone
	require.ErrorContains(t, err, "Cancelled DDL job")

	rows := tkCancel.MustQuery("admin show ddl jobs where JOB_ID=" + jobID).Rows()
	require.Equal(t, 1, len(rows))
	require.Equal(t, "rollback done", rows[0][len(rows[0])-2])

	tk.MustQuery("show tables like 'mv_cancel'").Check(testkit.Rows())
	tk.MustQuery("select count(*) from mysql.tidb_mview_refresh").Check(testkit.Rows("0"))

	is := dom.InfoSchema()
	baseTable, err := is.TableByName(context.Background(), pmodel.NewCIStr("test"), pmodel.NewCIStr("t"))
	require.NoError(t, err)
	require.NotNil(t, baseTable.Meta().MaterializedViewBase)
	require.NotZero(t, baseTable.Meta().MaterializedViewBase.MLogID)
	require.Empty(t, baseTable.Meta().MaterializedViewBase.MViewIDs)
}

func TestCreateMaterializedViewPauseAndResume(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("create table t (a int not null, b int not null)")
	tk.MustExec("insert into t values (1, 10), (1, 5), (2, 7)")
	tk.MustExec("create materialized view log on t (a, b) purge immediate")

	pauseBuildFailpoint := "github.com/pingcap/tidb/pkg/ddl/pauseCreateMaterializedViewBuild"
	require.NoError(t, failpoint.Enable(pauseBuildFailpoint, "pause"))
	enabled := true
	defer func() {
		if enabled {
			require.NoError(t, failpoint.Disable(pauseBuildFailpoint))
		}
	}()

	ddlDone := make(chan error, 1)
	go func() {
		tkDDL := testkit.NewTestKit(t, store)
		tkDDL.MustExec("use test")
		ddlDone <- tkDDL.ExecToErr("create materialized view mv_pause (a, s, cnt) refresh fast next 300 as select a, sum(b), count(1) from t group by a")
	}()

	tkCtl := testkit.NewTestKit(t, store)
	tkCtl.MustExec("use test")
	jobID := ""
	require.Eventually(t, func() bool {
		rows := tkCtl.MustQuery("admin show ddl jobs where JOB_TYPE='create materialized view'").Rows()
		if len(rows) == 0 {
			return false
		}
		if len(rows[0]) < 5 || strings.ToLower(fmt.Sprint(rows[0][4])) != "write reorganization" {
			return false
		}
		jobID = fmt.Sprint(rows[0][0])
		return jobID != ""
	}, 30*time.Second, 100*time.Millisecond)

	tkCtl.MustExec("admin pause ddl jobs " + jobID)
	require.NoError(t, failpoint.Disable(pauseBuildFailpoint))
	enabled = false

	require.Eventually(t, func() bool {
		rows := tkCtl.MustQuery("admin show ddl jobs where JOB_ID=" + jobID).Rows()
		if len(rows) == 0 {
			return false
		}
		state := strings.ToLower(fmt.Sprint(rows[0][len(rows[0])-2]))
		return state == "paused" || state == "pausing"
	}, 30*time.Second, 100*time.Millisecond)

	// Current Stage-1 semantics: MV table is visible once phase-1 finishes, even before job completion.
	tk.MustQuery("show tables like 'mv_pause'").Check(testkit.Rows("mv_pause"))

	tkCtl.MustExec("admin resume ddl jobs " + jobID)
	select {
	case err := <-ddlDone:
		if err != nil {
			require.ErrorContains(t, err, "detected residual build rows on retry")
			tk.MustQuery("show tables like 'mv_pause'").Check(testkit.Rows())
			return
		}
	case <-time.After(60 * time.Second):
		t.Fatalf("timed out waiting CREATE MATERIALIZED VIEW to finish after resume")
	}

	tk.MustQuery("select a, s, cnt from mv_pause order by a").Check(testkit.Rows("1 15 2", "2 7 1"))
}

func TestCreateMaterializedViewRollbackIgnoreMissingRefreshInfoTable(t *testing.T) {
	store, dom := testkit.CreateMockStoreAndDomain(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("create table t (a int not null, b int not null)")
	tk.MustExec("insert into t values (1, 10), (1, 5), (2, 7)")
	tk.MustExec("create materialized view log on t (a, b) purge immediate")

	require.NoError(t, failpoint.Enable("github.com/pingcap/tidb/pkg/ddl/mockCreateMaterializedViewBuildErr", "return"))
	require.NoError(t, failpoint.Enable("github.com/pingcap/tidb/pkg/ddl/mockDeleteCreateMaterializedViewRefreshInfoTableNotExists", "return(true)"))
	defer func() {
		require.NoError(t, failpoint.Disable("github.com/pingcap/tidb/pkg/ddl/mockDeleteCreateMaterializedViewRefreshInfoTableNotExists"))
		require.NoError(t, failpoint.Disable("github.com/pingcap/tidb/pkg/ddl/mockCreateMaterializedViewBuildErr"))
	}()

	err := tk.ExecToErr("create materialized view mv_missing_refresh_meta (a, s, cnt) refresh fast next 300 as select a, sum(b), count(1) from t group by a")
	require.Error(t, err)

	tk.MustQuery("show tables like 'mv_missing_refresh_meta'").Check(testkit.Rows())
	tk.MustQuery("select count(*) from mysql.tidb_mview_refresh").Check(testkit.Rows("0"))

	is := dom.InfoSchema()
	baseTable, err := is.TableByName(context.Background(), pmodel.NewCIStr("test"), pmodel.NewCIStr("t"))
	require.NoError(t, err)
	require.NotNil(t, baseTable.Meta().MaterializedViewBase)
	require.NotZero(t, baseTable.Meta().MaterializedViewBase.MLogID)
	require.Empty(t, baseTable.Meta().MaterializedViewBase.MViewIDs)
}

func TestCreateMaterializedViewRefreshInfoUpsertFailureRollback(t *testing.T) {
	store, dom := testkit.CreateMockStoreAndDomain(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("create table t (a int not null, b int not null)")
	tk.MustExec("insert into t values (1, 10), (1, 5), (2, 7)")
	tk.MustExec("create materialized view log on t (a, b) purge immediate")

	require.NoError(t, failpoint.Enable("github.com/pingcap/tidb/pkg/ddl/mockUpsertCreateMaterializedViewRefreshInfoTableNotExists", "1*return(true)"))
	defer func() {
		require.NoError(t, failpoint.Disable("github.com/pingcap/tidb/pkg/ddl/mockUpsertCreateMaterializedViewRefreshInfoTableNotExists"))
	}()

	err := tk.ExecToErr("create materialized view mv_upsert_fail (a, s, cnt) refresh fast next 300 as select a, sum(b), count(1) from t group by a")
	require.Error(t, err)
	require.ErrorContains(t, err, "tidb_mview_refresh")
	require.NotContains(t, err.Error(), "Information schema is changed")
	require.NotContains(t, err.Error(), "Duplicate entry")

	tk.MustQuery("show tables like 'mv_upsert_fail'").Check(testkit.Rows())
	tk.MustQuery("select count(*) from mysql.tidb_mview_refresh").Check(testkit.Rows("0"))
	rows := tk.MustQuery("admin show ddl jobs where JOB_TYPE='create materialized view'").Rows()
	require.NotEmpty(t, rows)
	jobID := fmt.Sprint(rows[0][0])
	tk.MustQuery("select ((select count(*) from mysql.gc_delete_range where job_id=" + jobID + ") + (select count(*) from mysql.gc_delete_range_done where job_id=" + jobID + ")) > 0").Check(testkit.Rows("1"))

	is := dom.InfoSchema()
	baseTable, err := is.TableByName(context.Background(), pmodel.NewCIStr("test"), pmodel.NewCIStr("t"))
	require.NoError(t, err)
	require.NotNil(t, baseTable.Meta().MaterializedViewBase)
	require.NotZero(t, baseTable.Meta().MaterializedViewBase.MLogID)
	require.Empty(t, baseTable.Meta().MaterializedViewBase.MViewIDs)
}

func TestCreateMaterializedViewRetryWithResidualBuildRowsRollback(t *testing.T) {
	store, dom := testkit.CreateMockStoreAndDomain(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("create table t (a int, b int not null)")
	tk.MustExec("insert into t values (1, 10), (1, 5), (null, 7), (null, 3)")
	tk.MustExec("create materialized view log on t (a, b) purge immediate")

	require.NoError(t, failpoint.Enable("github.com/pingcap/tidb/pkg/ddl/mockCreateMaterializedViewPostBuildRetryableErr", "1*return(true)"))
	defer func() {
		require.NoError(t, failpoint.Disable("github.com/pingcap/tidb/pkg/ddl/mockCreateMaterializedViewPostBuildRetryableErr"))
	}()

	err := tk.ExecToErr("create materialized view mv_retry_residual (a, s, cnt) refresh fast next 300 as select a, sum(b), count(1) from t group by a")
	require.Error(t, err)
	require.ErrorContains(t, err, "detected residual build rows on retry")
	require.NotContains(t, err.Error(), "Duplicate entry")

	tk.MustQuery("show tables like 'mv_retry_residual'").Check(testkit.Rows())
	tk.MustQuery("select count(*) from mysql.tidb_mview_refresh").Check(testkit.Rows("0"))

	is := dom.InfoSchema()
	baseTable, err := is.TableByName(context.Background(), pmodel.NewCIStr("test"), pmodel.NewCIStr("t"))
	require.NoError(t, err)
	require.NotNil(t, baseTable.Meta().MaterializedViewBase)
	require.NotZero(t, baseTable.Meta().MaterializedViewBase.MLogID)
	require.Empty(t, baseTable.Meta().MaterializedViewBase.MViewIDs)
}

func TestDropMaterializedViewLogRecheckWithConcurrentCreateMaterializedView(t *testing.T) {
	store, dom := testkit.CreateMockStoreAndDomain(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("create table t (a int not null, b int not null)")
	tk.MustExec("insert into t values (1, 10), (1, 5), (2, 7)")
	tk.MustExec("create materialized view log on t (a, b) purge immediate")

	const pauseDropFailpoint = "github.com/pingcap/tidb/pkg/ddl/pauseDropMaterializedViewLogAfterCheck"
	require.NoError(t, failpoint.Enable(pauseDropFailpoint, "pause"))
	enabled := true
	defer func() {
		if enabled {
			require.NoError(t, failpoint.Disable(pauseDropFailpoint))
		}
	}()

	dropErrCh := make(chan error, 1)
	go func() {
		tkDrop := testkit.NewTestKit(t, store)
		tkDrop.MustExec("use test")
		dropErrCh <- tkDrop.ExecToErr("drop materialized view log on t")
	}()

	// Let the drop SQL pass executor pre-check and pause before DDL job submit.
	time.Sleep(500 * time.Millisecond)

	tk.MustExec("create materialized view mv_dep (a, s, cnt) refresh fast next 300 as select a, sum(b), count(1) from t group by a")

	require.NoError(t, failpoint.Disable(pauseDropFailpoint))
	enabled = false

	err := <-dropErrCh
	require.ErrorContains(t, err, "dependent materialized views exist")
	tk.MustQuery("show tables like '$mlog$t'").Check(testkit.Rows("$mlog$t"))

	tk.MustExec("drop materialized view mv_dep")
	tk.MustExec("drop materialized view log on t")

	is := dom.InfoSchema()
	baseTable, err := is.TableByName(context.Background(), pmodel.NewCIStr("test"), pmodel.NewCIStr("t"))
	require.NoError(t, err)
	require.True(t, baseTable.Meta().MaterializedViewBase == nil || (baseTable.Meta().MaterializedViewBase.MLogID == 0 && len(baseTable.Meta().MaterializedViewBase.MViewIDs) == 0))
}

func TestCreateMaterializedViewRetryAfterUpsertFailure(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("create table t (a int not null, b int not null)")
	tk.MustExec("insert into t values (1, 10), (1, 5), (2, 7)")
	tk.MustExec("create materialized view log on t (a, b) purge immediate")

	require.NoError(t, failpoint.Enable("github.com/pingcap/tidb/pkg/ddl/mockUpsertCreateMaterializedViewRefreshInfoTableNotExists", "1*return(true)"))
	defer func() {
		require.NoError(t, failpoint.Disable("github.com/pingcap/tidb/pkg/ddl/mockUpsertCreateMaterializedViewRefreshInfoTableNotExists"))
	}()

	err := tk.ExecToErr("create materialized view mv_retry (a, s, cnt) refresh fast next 300 as select a, sum(b), count(1) from t group by a")
	require.Error(t, err)
	require.ErrorContains(t, err, "tidb_mview_refresh")
	require.NotContains(t, err.Error(), "Information schema is changed")
	tk.MustQuery("show tables like 'mv_retry'").Check(testkit.Rows())

	tk.MustExec("create materialized view mv_retry (a, s, cnt) refresh fast next 300 as select a, sum(b), count(1) from t group by a")
	tk.MustQuery("select a, s, cnt from mv_retry order by a").Check(testkit.Rows("1 15 2", "2 7 1"))
}

func TestCreateTableLikeShouldNotCarryMaterializedViewMetadata(t *testing.T) {
	store, dom := testkit.CreateMockStoreAndDomain(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("create table t (a int not null, b int not null)")
	tk.MustExec("insert into t values (1, 10), (1, 5), (2, 7)")
	tk.MustExec("create materialized view log on t (a, b) purge immediate")
	tk.MustExec("create materialized view mv_src (a, s, cnt) refresh fast next 300 as select a, sum(b), count(1) from t group by a")

	tk.MustExec("create table t_like like t")
	tk.MustExec("create table mv_like like mv_src")
	tk.MustExec("create table mlog_like like `$mlog$t`")

	is := dom.InfoSchema()
	baseTable, err := is.TableByName(context.Background(), pmodel.NewCIStr("test"), pmodel.NewCIStr("t"))
	require.NoError(t, err)
	mvSrc, err := is.TableByName(context.Background(), pmodel.NewCIStr("test"), pmodel.NewCIStr("mv_src"))
	require.NoError(t, err)
	mvLike, err := is.TableByName(context.Background(), pmodel.NewCIStr("test"), pmodel.NewCIStr("mv_like"))
	require.NoError(t, err)
	mlogSrc, err := is.TableByName(context.Background(), pmodel.NewCIStr("test"), pmodel.NewCIStr("$mlog$t"))
	require.NoError(t, err)
	mlogLike, err := is.TableByName(context.Background(), pmodel.NewCIStr("test"), pmodel.NewCIStr("mlog_like"))
	require.NoError(t, err)

	require.NotNil(t, mvSrc.Meta().MaterializedView)
	require.NotNil(t, mlogSrc.Meta().MaterializedViewLog)

	require.Nil(t, mvLike.Meta().MaterializedView)
	require.Nil(t, mvLike.Meta().MaterializedViewLog)
	require.Nil(t, mvLike.Meta().MaterializedViewBase)
	require.Nil(t, mlogLike.Meta().MaterializedView)
	require.Nil(t, mlogLike.Meta().MaterializedViewLog)
	require.Nil(t, mlogLike.Meta().MaterializedViewBase)

	require.NotNil(t, baseTable.Meta().MaterializedViewBase)
	require.Equal(t, mlogSrc.Meta().ID, baseTable.Meta().MaterializedViewBase.MLogID)
	require.Equal(t, []int64{mvSrc.Meta().ID}, baseTable.Meta().MaterializedViewBase.MViewIDs)
}
