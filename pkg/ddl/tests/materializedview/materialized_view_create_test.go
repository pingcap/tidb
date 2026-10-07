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

package materializedview_test

import (
	"context"
	"fmt"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/pingcap/failpoint"
	"github.com/pingcap/tidb/pkg/ddl"
	ddlsess "github.com/pingcap/tidb/pkg/ddl/session"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/testkit"
	"github.com/pingcap/tidb/pkg/testkit/testfailpoint"
	"github.com/stretchr/testify/require"
)

func newMViewTestKit(t testing.TB, store kv.Storage) *testkit.TestKit {
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("set tidb_mview_enable = on")
	return tk
}

func TestCreateMaterializedViewBuildFailureRollback(t *testing.T) {
	store, dom := testkit.CreateMockStoreAndDomain(t)
	tk := newMViewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("create table t (a int not null, b int not null)")
	tk.MustExec("insert into t values (1, 10), (1, 5), (2, 7)")
	tk.MustExec("create materialized view log on t (a, b) purge next date_add(now(), interval 1 hour)")

	require.NoError(t, failpoint.Enable("github.com/pingcap/tidb/pkg/ddl/mockCreateMaterializedViewBuildErr", "return"))
	defer func() {
		require.NoError(t, failpoint.Disable("github.com/pingcap/tidb/pkg/ddl/mockCreateMaterializedViewBuildErr"))
	}()

	err := tk.ExecToErr("create materialized view mv_fail (a, s, cnt) refresh fast next date_add(now(), interval 1 hour) as select a, sum(b), count(1) from t group by a")
	require.Error(t, err)
	tk.MustQuery("select count(*) from information_schema.tables where table_schema = 'test' and table_name = 'mv_fail'").Check(testkit.Rows("0"))
	tk.MustQuery("select count(*) from mysql.tidb_mview_refresh_info").Check(testkit.Rows("0"))

	is := dom.InfoSchema()
	baseTable, err := is.TableByName(context.Background(), ast.NewCIStr("test"), ast.NewCIStr("t"))
	require.NoError(t, err)
	require.NotNil(t, baseTable.Meta().MaterializedViewBase)
	require.NotZero(t, baseTable.Meta().MaterializedViewBase.MLogID)
	require.Empty(t, baseTable.Meta().MaterializedViewBase.MViewIDs)
}

func TestCreateMaterializedViewBuildContextCanceledRollback(t *testing.T) {
	store, dom := testkit.CreateMockStoreAndDomain(t)
	tk := newMViewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("create table t (a int not null, b int not null)")
	tk.MustExec("insert into t values (1, 10), (1, 5), (2, 7)")
	tk.MustExec("create materialized view log on t (a, b) purge next date_add(now(), interval 1 hour)")

	require.NoError(t, failpoint.Enable("github.com/pingcap/tidb/pkg/ddl/mockCreateMaterializedViewBuildErr", `return("context-canceled")`))
	defer func() {
		require.NoError(t, failpoint.Disable("github.com/pingcap/tidb/pkg/ddl/mockCreateMaterializedViewBuildErr"))
	}()

	err := tk.ExecToErr("create materialized view mv_ctx_cancel (a, s, cnt) refresh fast next date_add(now(), interval 1 hour) as select a, sum(b), count(1) from t group by a")
	require.Error(t, err)
	tk.MustQuery("show tables like 'mv_ctx_cancel'").Check(testkit.Rows())
	tk.MustQuery("select count(*) from mysql.tidb_mview_refresh_info").Check(testkit.Rows("0"))

	is := dom.InfoSchema()
	baseTable, err := is.TableByName(context.Background(), ast.NewCIStr("test"), ast.NewCIStr("t"))
	require.NoError(t, err)
	require.NotNil(t, baseTable.Meta().MaterializedViewBase)
	require.NotZero(t, baseTable.Meta().MaterializedViewBase.MLogID)
	require.Empty(t, baseTable.Meta().MaterializedViewBase.MViewIDs)
}

func TestCreateMaterializedViewRollbackIgnoreMissingRefreshInfoTable(t *testing.T) {
	store, dom := testkit.CreateMockStoreAndDomain(t)
	tk := newMViewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("create table t (a int not null, b int not null)")
	tk.MustExec("insert into t values (1, 10), (1, 5), (2, 7)")
	tk.MustExec("create materialized view log on t (a, b) purge next date_add(now(), interval 1 hour)")

	require.NoError(t, failpoint.Enable("github.com/pingcap/tidb/pkg/ddl/mockCreateMaterializedViewBuildErr", "return"))
	require.NoError(t, failpoint.Enable("github.com/pingcap/tidb/pkg/ddl/mockDeleteCreateMaterializedViewRefreshInfoTableNotExists", "return(true)"))
	defer func() {
		require.NoError(t, failpoint.Disable("github.com/pingcap/tidb/pkg/ddl/mockDeleteCreateMaterializedViewRefreshInfoTableNotExists"))
		require.NoError(t, failpoint.Disable("github.com/pingcap/tidb/pkg/ddl/mockCreateMaterializedViewBuildErr"))
	}()

	err := tk.ExecToErr("create materialized view mv_missing_refresh_meta (a, s, cnt) refresh fast next date_add(now(), interval 1 hour) as select a, sum(b), count(1) from t group by a")
	require.Error(t, err)
	tk.MustQuery("show tables like 'mv_missing_refresh_meta'").Check(testkit.Rows())
	tk.MustQuery("select count(*) from mysql.tidb_mview_refresh_info").Check(testkit.Rows("0"))

	is := dom.InfoSchema()
	baseTable, err := is.TableByName(context.Background(), ast.NewCIStr("test"), ast.NewCIStr("t"))
	require.NoError(t, err)
	require.NotNil(t, baseTable.Meta().MaterializedViewBase)
	require.NotZero(t, baseTable.Meta().MaterializedViewBase.MLogID)
	require.Empty(t, baseTable.Meta().MaterializedViewBase.MViewIDs)
}

func TestCreateMaterializedViewRefreshInfoUpsertFailureRollback(t *testing.T) {
	store, dom := testkit.CreateMockStoreAndDomain(t)
	tk := newMViewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("create table t (a int not null, b int not null)")
	tk.MustExec("insert into t values (1, 10), (1, 5), (2, 7)")
	tk.MustExec("create materialized view log on t (a, b) purge next date_add(now(), interval 1 hour)")

	require.NoError(t, failpoint.Enable("github.com/pingcap/tidb/pkg/ddl/mockUpsertCreateMaterializedViewRefreshInfoTableNotExists", "1*return(true)"))
	defer func() {
		require.NoError(t, failpoint.Disable("github.com/pingcap/tidb/pkg/ddl/mockUpsertCreateMaterializedViewRefreshInfoTableNotExists"))
	}()

	err := tk.ExecToErr("create materialized view mv_upsert_fail (a, s, cnt) refresh fast next date_add(now(), interval 1 hour) as select a, sum(b), count(1) from t group by a")
	require.Error(t, err)
	require.ErrorContains(t, err, "tidb_mview_refresh_info")
	require.NotContains(t, err.Error(), "Information schema is changed")
	require.NotContains(t, err.Error(), "Duplicate entry")
	tk.MustQuery("show tables like 'mv_upsert_fail'").Check(testkit.Rows())
	tk.MustQuery("select count(*) from mysql.tidb_mview_refresh_info").Check(testkit.Rows("0"))
	rows := tk.MustQuery("admin show ddl jobs where JOB_TYPE='create materialized view'").Rows()
	require.NotEmpty(t, rows)
	jobID := fmt.Sprint(rows[0][0])
	tk.MustQuery("select ((select count(*) from mysql.gc_delete_range where job_id=" + jobID + ") + (select count(*) from mysql.gc_delete_range_done where job_id=" + jobID + ")) > 0").Check(testkit.Rows("1"))

	is := dom.InfoSchema()
	baseTable, err := is.TableByName(context.Background(), ast.NewCIStr("test"), ast.NewCIStr("t"))
	require.NoError(t, err)
	require.NotNil(t, baseTable.Meta().MaterializedViewBase)
	require.NotZero(t, baseTable.Meta().MaterializedViewBase.MLogID)
	require.Empty(t, baseTable.Meta().MaterializedViewBase.MViewIDs)
}

func TestCreateMaterializedViewRetryWithResidualBuildRowsRollback(t *testing.T) {
	store, dom := testkit.CreateMockStoreAndDomain(t)
	tk := newMViewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("create table t (a int, b int not null)")
	tk.MustExec("insert into t values (1, 10), (1, 5), (null, 7), (null, 3)")
	tk.MustExec("create materialized view log on t (a, b) purge next date_add(now(), interval 1 hour)")

	require.NoError(t, failpoint.Enable("github.com/pingcap/tidb/pkg/ddl/mockCreateMaterializedViewPostBuildRetryableErr", "1*return(true)"))
	defer func() {
		require.NoError(t, failpoint.Disable("github.com/pingcap/tidb/pkg/ddl/mockCreateMaterializedViewPostBuildRetryableErr"))
	}()

	err := tk.ExecToErr("create materialized view mv_retry_residual (a, s, cnt) refresh fast next date_add(now(), interval 1 hour) as select a, sum(b), count(1) from t group by a")
	require.Error(t, err)
	require.ErrorContains(t, err, "detected residual build rows on retry")
	require.NotContains(t, err.Error(), "Duplicate entry")
	tk.MustQuery("show tables like 'mv_retry_residual'").Check(testkit.Rows())
	tk.MustQuery("select count(*) from mysql.tidb_mview_refresh_info").Check(testkit.Rows("0"))

	is := dom.InfoSchema()
	baseTable, err := is.TableByName(context.Background(), ast.NewCIStr("test"), ast.NewCIStr("t"))
	require.NoError(t, err)
	require.NotNil(t, baseTable.Meta().MaterializedViewBase)
	require.NotZero(t, baseTable.Meta().MaterializedViewBase.MLogID)
	require.Empty(t, baseTable.Meta().MaterializedViewBase.MViewIDs)
}

func TestCreateMaterializedViewRetryAfterUpsertFailure(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := newMViewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("create table t (a int not null, b int not null)")
	tk.MustExec("insert into t values (1, 10), (1, 5), (2, 7)")
	tk.MustExec("create materialized view log on t (a, b) purge next date_add(now(), interval 1 hour)")

	require.NoError(t, failpoint.Enable("github.com/pingcap/tidb/pkg/ddl/mockUpsertCreateMaterializedViewRefreshInfoTableNotExists", "1*return(true)"))
	require.NoError(t, failpoint.Enable("github.com/pingcap/tidb/pkg/ddl/mockDeleteCreateMaterializedViewRefreshAlertErr", `return("mock rollback alert delete error")`))
	defer func() {
		require.NoError(t, failpoint.Disable("github.com/pingcap/tidb/pkg/ddl/mockUpsertCreateMaterializedViewRefreshInfoTableNotExists"))
		require.NoError(t, failpoint.Disable("github.com/pingcap/tidb/pkg/ddl/mockDeleteCreateMaterializedViewRefreshAlertErr"))
	}()

	err := tk.ExecToErr("create materialized view mv_retry (a, s, cnt) refresh fast next date_add(now(), interval 1 hour) as select a, sum(b), count(1) from t group by a")
	require.Error(t, err)
	require.ErrorContains(t, err, "tidb_mview_refresh_info")
	require.NotContains(t, err.Error(), "Information schema is changed")
	require.NotContains(t, err.Error(), "mock rollback alert delete error")
	tk.MustQuery("show tables like 'mv_retry'").Check(testkit.Rows())

	tk.MustExec("create materialized view mv_retry (a, s, cnt) refresh fast next date_add(now(), interval 1 hour) as select a, sum(b), count(1) from t group by a")
	tk.MustQuery("select a, s, cnt from mv_retry order by a").Check(testkit.Rows("1 15 2", "2 7 1"))
}

func TestCreateMaterializedViewHistoryJobSchemaVersion(t *testing.T) {
	store, dom := testkit.CreateMockStoreAndDomain(t)
	tk := newMViewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("create table t (a int not null, b int not null)")
	tk.MustExec("insert into t values (1, 10), (1, 5), (2, 7)")
	tk.MustExec("create materialized view log on t (a, b) purge next date_add(now(), interval 1 hour)")
	tk.MustExec("create materialized view mv_hist_schema_ver (a, s, cnt) refresh fast next date_add(now(), interval 1 hour) as select a, sum(b), count(1) from t group by a")

	rows := tk.MustQuery("admin show ddl jobs where JOB_TYPE='create materialized view'").Rows()
	require.NotEmpty(t, rows)
	jobID, err := strconv.ParseInt(fmt.Sprint(rows[0][0]), 10, 64)
	require.NoError(t, err)
	historyJob, err := ddl.GetHistoryJobByID(tk.Session(), jobID)
	require.NoError(t, err)
	require.Greater(t, historyJob.BinlogInfo.SchemaVersion, int64(0))
	is := dom.InfoSchema()
	mvTable, err := is.TableByName(context.Background(), ast.NewCIStr("test"), ast.NewCIStr("mv_hist_schema_ver"))
	require.NoError(t, err)
	require.NotNil(t, historyJob.BinlogInfo.TableInfo)
	require.Equal(t, mvTable.Meta().ID, historyJob.BinlogInfo.TableInfo.ID)
	require.Equal(t, mvTable.Meta().Name, historyJob.BinlogInfo.TableInfo.Name)
}

func TestCreateMaterializedViewCancelRollback(t *testing.T) {
	store, dom := testkit.CreateMockStoreAndDomain(t)
	tk := newMViewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("create table t (a int not null, b int not null)")
	tk.MustExec("insert into t values (1, 10), (1, 5), (2, 7)")
	tk.MustExec("create materialized view log on t (a, b) purge next date_add(now(), interval 1 hour)")
	tkCancel := newMViewTestKit(t, store)
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
		tkDDL := newMViewTestKit(t, store)
		tkDDL.MustExec("use test")
		ddlDone <- tkDDL.ExecToErr("create materialized view mv_cancel (a, s, cnt) refresh fast next date_add(now(), interval 1 hour) as select a, sum(b), count(1) from t group by a")
	}()

	jobID := ""
	require.Eventually(t, func() bool {
		rows := tkCancel.MustQuery("admin show ddl jobs where JOB_TYPE='create materialized view'").Rows()
		if len(rows) == 0 || len(rows[0]) < 5 || strings.ToLower(fmt.Sprint(rows[0][4])) != "write reorganization" {
			return false
		}
		jobID = fmt.Sprint(rows[0][0])
		return jobID != ""
	}, 30*time.Second, 100*time.Millisecond)

	tkCancel.MustExec("admin cancel ddl jobs " + jobID)
	require.NoError(t, failpoint.Disable(pauseBuildFailpoint))
	enabled = false

	err := <-ddlDone
	require.ErrorContains(t, err, "Cancelled DDL job")
	rows := tkCancel.MustQuery("admin show ddl jobs where JOB_ID=" + jobID).Rows()
	require.Equal(t, "rollback done", rows[0][len(rows[0])-2])
	tk.MustQuery("show tables like 'mv_cancel'").Check(testkit.Rows())
	tk.MustQuery("select count(*) from mysql.tidb_mview_refresh_info").Check(testkit.Rows("0"))

	is := dom.InfoSchema()
	baseTable, err := is.TableByName(context.Background(), ast.NewCIStr("test"), ast.NewCIStr("t"))
	require.NoError(t, err)
	require.NotNil(t, baseTable.Meta().MaterializedViewBase)
	require.NotZero(t, baseTable.Meta().MaterializedViewBase.MLogID)
	require.Empty(t, baseTable.Meta().MaterializedViewBase.MViewIDs)
}

func TestCreateMaterializedViewRefreshInfoRunningAndSuccess(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := newMViewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("create table t (a int not null, b int not null)")
	tk.MustExec("insert into t values (1, 10), (1, 5), (2, 7)")
	tk.MustExec("create materialized view log on t (a, b) purge next date_add(now(), interval 1 hour)")

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
		tkDDL := newMViewTestKit(t, store)
		tkDDL.MustExec("use test")
		ddlDone <- tkDDL.ExecToErr("create materialized view mv_state (a, s, cnt) refresh fast next date_add(now(), interval 1 hour) as select a, sum(b), count(1) from t group by a")
	}()

	var initTS uint64
	var mviewID int64
	require.Eventually(t, func() bool {
		rows := tk.MustQuery("select MVIEW_ID, LAST_SUCCESS_READ_TSO from mysql.tidb_mview_refresh_info").Rows()
		if len(rows) != 1 {
			return false
		}
		id, err := strconv.ParseInt(fmt.Sprint(rows[0][0]), 10, 64)
		if err != nil || id == 0 {
			return false
		}
		ts, err := strconv.ParseUint(fmt.Sprint(rows[0][1]), 10, 64)
		if err != nil || ts == 0 {
			return false
		}
		mviewID, initTS = id, ts
		return true
	}, 30*time.Second, 100*time.Millisecond)

	require.NoError(t, failpoint.Disable(pauseBuildFailpoint))
	enabled = false
	require.NoError(t, <-ddlDone)
	tk.MustQuery("select a, s, cnt from mv_state order by a").Check(testkit.Rows("1 15 2", "2 7 1"))

	rows := tk.MustQuery(fmt.Sprintf("select LAST_SUCCESS_READ_TSO from mysql.tidb_mview_refresh_info where MVIEW_ID = %d", mviewID)).Rows()
	finalTS, err := strconv.ParseUint(fmt.Sprint(rows[0][0]), 10, 64)
	require.NoError(t, err)
	require.Greater(t, finalTS, initTS)
}

func TestCreateMaterializedViewBuildReadTSQueryTypeAlignment(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := newMViewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("create table t (a int)")
	tk.MustExec("insert into t values (1)")

	ddlSe := ddlsess.NewSession(tk.Session())
	tk.MustExec("select * from t")
	expected := tk.Session().GetSessionVars().LastQueryInfo.StartTS
	require.NotZero(t, expected)

	rows, err := ddlSe.Execute(context.Background(),
		"SELECT COALESCE(CAST(JSON_UNQUOTE(JSON_EXTRACT(@@tidb_last_query_info, '$.start_ts')) AS UNSIGNED), CAST(0 AS UNSIGNED))",
		"create-materialized-view-build-read-ts-ut",
	)
	require.NoError(t, err)
	require.Len(t, rows, 1)
	require.Equal(t, expected, rows[0].GetUint64(0))
}

func TestCreateMaterializedViewSuccessRefreshInfoVisibilityBeforeCommit(t *testing.T) {
	store, dom := testkit.CreateMockStoreAndDomain(t)
	tk := newMViewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("create table t (a int not null, b int not null)")
	tk.MustExec("insert into t values (1, 10), (1, 5), (2, 7)")
	tk.MustExec("create materialized view log on t (a, b) purge next date_add(now(), interval 1 hour)")

	const afterUpsertFailpoint = "github.com/pingcap/tidb/pkg/ddl/afterCreateMaterializedViewSuccessRefreshInfoUpsert"
	const postUpsertRetryableErr = "github.com/pingcap/tidb/pkg/ddl/mockCreateMaterializedViewPostBuildAfterRefreshInfoUpsertRetryableErr"

	paused := make(chan struct{})
	resume := make(chan struct{})
	var pausedOnce sync.Once
	var resumeOnce sync.Once
	release := func() { resumeOnce.Do(func() { close(resume) }) }
	testfailpoint.EnableCall(t, afterUpsertFailpoint, func() {
		pausedOnce.Do(func() { close(paused) })
		<-resume
	})

	require.NoError(t, failpoint.Enable(postUpsertRetryableErr, "1*return(true)"))
	defer func() {
		release()
		require.NoError(t, failpoint.Disable(postUpsertRetryableErr))
	}()

	ddlDone := make(chan error, 1)
	go func() {
		tkDDL := newMViewTestKit(t, store)
		tkDDL.MustExec("use test")
		ddlDone <- tkDDL.ExecToErr("create materialized view mv_upsert_visibility (a, s, cnt) refresh fast next date_add(now(), interval 1 hour) as select a, sum(b), count(1) from t group by a")
	}()

	var prewriteTS uint64
	require.Eventually(t, func() bool {
		rows := tk.MustQuery("select LAST_SUCCESS_READ_TSO from mysql.tidb_mview_refresh_info").Rows()
		if len(rows) != 1 {
			return false
		}
		ts, err := strconv.ParseUint(fmt.Sprint(rows[0][0]), 10, 64)
		if err != nil || ts == 0 {
			return false
		}
		prewriteTS = ts
		return true
	}, 30*time.Second, 100*time.Millisecond)

	select {
	case <-paused:
	case <-time.After(30 * time.Second):
		t.Fatal("timed out waiting for post-upsert failpoint")
	}

	rows := tk.MustQuery("select LAST_SUCCESS_READ_TSO from mysql.tidb_mview_refresh_info").Rows()
	require.Len(t, rows, 1)
	visibleTS, err := strconv.ParseUint(fmt.Sprint(rows[0][0]), 10, 64)
	require.NoError(t, err)
	require.Equal(t, prewriteTS, visibleTS)

	release()
	err = <-ddlDone
	require.Error(t, err)
	require.ErrorContains(t, err, "detected residual build rows on retry")
	require.NotContains(t, err.Error(), "Duplicate entry")
	tk.MustQuery("show tables like 'mv_upsert_visibility'").Check(testkit.Rows())
	tk.MustQuery("select count(*) from mysql.tidb_mview_refresh_info").Check(testkit.Rows("0"))

	is := dom.InfoSchema()
	baseTable, err := is.TableByName(context.Background(), ast.NewCIStr("test"), ast.NewCIStr("t"))
	require.NoError(t, err)
	require.NotNil(t, baseTable.Meta().MaterializedViewBase)
	require.NotZero(t, baseTable.Meta().MaterializedViewBase.MLogID)
	require.Empty(t, baseTable.Meta().MaterializedViewBase.MViewIDs)
}

func TestCreateMaterializedViewPauseAndResume(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := newMViewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("create table t (a int not null, b int not null)")
	tk.MustExec("insert into t values (1, 10), (1, 5), (2, 7)")
	tk.MustExec("create materialized view log on t (a, b) purge next date_add(now(), interval 1 hour)")

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
		tkDDL := newMViewTestKit(t, store)
		tkDDL.MustExec("use test")
		ddlDone <- tkDDL.ExecToErr("create materialized view mv_pause (a, s, cnt) refresh fast next date_add(now(), interval 1 hour) as select a, sum(b), count(1) from t group by a")
	}()

	tkCtl := newMViewTestKit(t, store)
	tkCtl.MustExec("use test")
	jobID := ""
	require.Eventually(t, func() bool {
		rows := tkCtl.MustQuery("admin show ddl jobs where JOB_TYPE='create materialized view'").Rows()
		if len(rows) == 0 || len(rows[0]) < 5 || strings.ToLower(fmt.Sprint(rows[0][4])) != "write reorganization" {
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
		return state == "paused"
	}, 30*time.Second, 100*time.Millisecond)

	tk.MustQuery("show tables like 'mv_pause'").Check(testkit.Rows("mv_pause"))
	err := tk.ExecToErr("select * from mv_pause")
	require.ErrorContains(t, err, "initial build is in progress")
	err = tk.ExecToErr("select * from mv_pause where a = 1")
	require.ErrorContains(t, err, "initial build is in progress")
	err = tk.ExecToErr("select * from mv_pause where a in (1, 2)")
	require.ErrorContains(t, err, "initial build is in progress")
	for _, sql := range []string{
		"insert into mv_pause values (9, 1, 1)",
		"replace into mv_pause values (9, 1, 1)",
		"load data local infile '/tmp/nonexistent.csv' into table mv_pause",
		"import into mv_pause from '/tmp/nonexistent.csv'",
	} {
		err = tk.ExecToErr(sql)
		require.ErrorContains(t, err, "not updatable", sql)
	}
	tkCtl.MustQuery("admin resume ddl jobs " + jobID).Check(testkit.Rows(jobID + " successful"))
	select {
	case err := <-ddlDone:
		if err != nil {
			require.ErrorContains(t, err, "detected residual build rows on retry")
			tk.MustQuery("show tables like 'mv_pause'").Check(testkit.Rows())
			return
		}
	case <-time.After(60 * time.Second):
		t.Fatal("timed out waiting CREATE MATERIALIZED VIEW to finish after resume")
	}
	tk.MustQuery("select a, s, cnt from mv_pause order by a").Check(testkit.Rows("1 15 2", "2 7 1"))
}
