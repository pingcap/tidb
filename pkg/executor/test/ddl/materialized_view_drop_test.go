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
	"sync"
	"testing"
	"time"

	"github.com/pingcap/failpoint"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/meta"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/parser/auth"
	"github.com/pingcap/tidb/pkg/testkit"
	"github.com/pingcap/tidb/pkg/testkit/testfailpoint"
	"github.com/stretchr/testify/require"
)

func TestDropMaterializedViewIfExists(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := newMViewTestKit(t, store)
	tk.MustExec("use test")

	tk.MustExec("drop materialized view if exists missing_mv")
	tk.MustExec("drop materialized view if exists missing_schema.missing_mv")

	err := tk.ExecToErr("drop materialized view log if exists on missing_base")
	require.ErrorContains(t, err, "Table 'test.missing_base' doesn't exist")
	err = tk.ExecToErr("drop materialized view log if exists on missing_schema.missing_base")
	require.ErrorContains(t, err, "Table 'missing_schema.missing_base' doesn't exist")

	tk.MustExec("create table t_drop_if_exists (a int not null)")
	tk.MustExec("create table t_no_mlog_drop_if_exists (a int not null)")
	tk.MustExec("create materialized view log on t_drop_if_exists (a)")
	tk.MustExec("create materialized view mv_drop_if_exists (a, cnt) as select a, count(1) from t_drop_if_exists group by a")

	err = tk.ExecToErr("drop materialized view if exists t_drop_if_exists")
	require.ErrorContains(t, err, "is not MATERIALIZED VIEW")
	err = tk.ExecToErr("drop materialized view log if exists on mv_drop_if_exists")
	require.ErrorContains(t, err, "is not BASE TABLE")

	tk.MustExec("drop materialized view if exists mv_drop_if_exists")
	tk.MustExec("drop materialized view if exists mv_drop_if_exists")

	tk.MustExec("create view v_drop_if_exists as select * from t_drop_if_exists")
	err = tk.ExecToErr("drop materialized view log if exists on v_drop_if_exists")
	require.ErrorContains(t, err, "is not BASE TABLE")

	tk.MustExec("drop materialized view log if exists on t_no_mlog_drop_if_exists")
	tk.MustExec("drop materialized view log if exists on t_drop_if_exists")
	tk.MustExec("drop materialized view log if exists on t_drop_if_exists")
}

func TestDropTableMaterializedViewConstraints(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := newMViewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("create table t_drop_constraints (a int not null, b int not null)")
	tk.MustExec("create materialized view log on t_drop_constraints (a, b)")
	tk.MustExec("create materialized view mv_drop_constraints (a, s, cnt) as select a, sum(b), count(1) from t_drop_constraints group by a")

	err := tk.ExecToErr("drop table mv_drop_constraints")
	require.ErrorContains(t, err, "DROP TABLE on materialized view table")
	err = tk.ExecToErr("drop table `$mlog$t_drop_constraints`")
	require.ErrorContains(t, err, "DROP TABLE on materialized view log table")
	err = tk.ExecToErr("drop table t_drop_constraints")
	require.ErrorContains(t, err, "DROP TABLE on base table with materialized view dependencies")

	tk.MustExec("drop materialized view mv_drop_constraints")
	err = tk.ExecToErr("drop table t_drop_constraints")
	require.ErrorContains(t, err, "DROP TABLE on base table with materialized view log")

	tk.MustExec("drop materialized view log on t_drop_constraints")
	tk.MustExec("drop table t_drop_constraints")
}

func TestDropMaterializedViewLogRecheckWithConcurrentCreateMaterializedView(t *testing.T) {
	store, dom := testkit.CreateMockStoreAndDomain(t)
	tk := newMViewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("create table t_drop_recheck (a int not null, b int not null)")
	tk.MustExec("insert into t_drop_recheck values (1, 10), (1, 5), (2, 7)")
	tk.MustExec("create materialized view log on t_drop_recheck (a, b) purge next date_add(now(), interval 1 hour)")

	const pauseDropFailpoint = "github.com/pingcap/tidb/pkg/ddl/pauseDropMaterializedViewLogAfterCheck"
	const afterCheckDropFailpoint = "github.com/pingcap/tidb/pkg/ddl/afterCheckDropMaterializedViewLog"
	dropCheckDoneCh := make(chan struct{})
	var dropCheckDoneOnce sync.Once
	testfailpoint.EnableCall(t, afterCheckDropFailpoint, func() {
		dropCheckDoneOnce.Do(func() {
			close(dropCheckDoneCh)
		})
	})
	require.NoError(t, failpoint.Enable(pauseDropFailpoint, "pause"))
	enabled := true
	defer func() {
		if enabled {
			require.NoError(t, failpoint.Disable(pauseDropFailpoint))
		}
	}()

	dropErrCh := make(chan error, 1)
	go func() {
		tkDrop := newMViewTestKit(t, store)
		tkDrop.MustExec("use test")
		dropErrCh <- tkDrop.ExecToErr("drop materialized view log on t_drop_recheck")
	}()

	select {
	case <-dropCheckDoneCh:
	case <-time.After(10 * time.Second):
		t.Fatal("timeout waiting for DROP MATERIALIZED VIEW LOG precheck")
	}
	tk.MustExec("create materialized view mv_drop_dep (a, s, cnt) refresh fast next date_add(now(), interval 1 hour) as select a, sum(b), count(1) from t_drop_recheck group by a")

	require.NoError(t, failpoint.Disable(pauseDropFailpoint))
	enabled = false

	err := <-dropErrCh
	require.ErrorContains(t, err, "dependent materialized views exist")
	tk.MustQuery("show tables like '$mlog$t_drop_recheck'").Check(testkit.Rows("$mlog$t_drop_recheck"))

	tk.MustExec("drop materialized view mv_drop_dep")
	is := dom.InfoSchema()
	mlogTable, err := is.TableByName(context.Background(), ast.NewCIStr("test"), ast.NewCIStr("$mlog$t_drop_recheck"))
	require.NoError(t, err)
	require.Empty(t, mlogTable.Meta().MaterializedViewLog.DependentMViewIDs)
	tk.MustExec("drop materialized view log on t_drop_recheck")

	is = dom.InfoSchema()
	baseTable, err := is.TableByName(context.Background(), ast.NewCIStr("test"), ast.NewCIStr("t_drop_recheck"))
	require.NoError(t, err)
	require.True(t, baseTable.Meta().MaterializedViewBase == nil || (baseTable.Meta().MaterializedViewBase.MLogID == 0 && len(baseTable.Meta().MaterializedViewBase.MViewIDs) == 0))
}

func TestDropMaterializedViewLogRemovesPurgeState(t *testing.T) {
	store, dom := testkit.CreateMockStoreAndDomain(t)
	tk := newMViewTestKit(t, store)
	tk.MustExec("use test")

	tk.MustExec("create table t_drop_mlog_purge_state (a int)")
	tk.MustExec("create materialized view log on t_drop_mlog_purge_state (a)")

	is := dom.InfoSchema()
	mlogTable, err := is.TableByName(context.Background(), ast.NewCIStr("test"), ast.NewCIStr("$mlog$t_drop_mlog_purge_state"))
	require.NoError(t, err)
	mlogID := mlogTable.Meta().ID

	tk.MustQuery(fmt.Sprintf("select count(*) from mysql.tidb_mlog_purge_info where MLOG_ID = %d", mlogID)).
		Check(testkit.Rows("1"))

	tk.MustExec("drop materialized view log on t_drop_mlog_purge_state")
	tk.MustQuery(fmt.Sprintf("select count(*) from mysql.tidb_mlog_purge_info where MLOG_ID = %d", mlogID)).
		Check(testkit.Rows("0"))
}

func TestDropMaterializedViewRefreshInfoFailureRollsBackMetadata(t *testing.T) {
	store, dom := testkit.CreateMockStoreAndDomain(t)
	tk := newMViewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("create table t_drop_mv_atomic (a int)")
	tk.MustExec("create materialized view log on t_drop_mv_atomic (a)")
	tk.MustExec("create materialized view mv_drop_atomic (a, cnt) as select a, count(1) from t_drop_mv_atomic group by a")

	is := dom.InfoSchema()
	mvTable, err := is.TableByName(context.Background(), ast.NewCIStr("test"), ast.NewCIStr("mv_drop_atomic"))
	require.NoError(t, err)
	mvID := mvTable.Meta().ID

	const cleanupErrFP = "github.com/pingcap/tidb/pkg/ddl/mockDeleteCreateMaterializedViewRefreshInfoErr"
	require.NoError(t, failpoint.Enable(cleanupErrFP, `1*return("mock refresh info delete error")`))
	defer func() { require.NoError(t, failpoint.Disable(cleanupErrFP)) }()

	retryStarted := make(chan struct{})
	allowRetry := make(chan struct{})
	testfailpoint.EnableCall(t, "github.com/pingcap/tidb/pkg/ddl/beforeRunOneJobStep", func(job *model.Job) {
		if job.Type == model.ActionDropTable && job.TableID == mvID && job.SchemaState == model.StateDeleteOnly && job.ErrorCount > 0 {
			select {
			case <-retryStarted:
			default:
				close(retryStarted)
			}
			<-allowRetry
		}
	})

	tkInspect := newMViewTestKit(t, store)
	tkInspect.MustExec("use test")
	dropErrCh := make(chan error, 1)
	go func() { dropErrCh <- tk.ExecToErr("drop materialized view mv_drop_atomic") }()

	select {
	case <-retryStarted:
	case <-time.After(10 * time.Second):
		t.Fatal("timeout waiting for DROP MATERIALIZED VIEW retry")
	}
	tkInspect.MustQuery("show tables like 'mv_drop_atomic'").Check(testkit.Rows("mv_drop_atomic"))
	tkInspect.MustQuery(fmt.Sprintf("select count(*) from mysql.tidb_mview_refresh_info where mview_id = %d", mvID)).Check(testkit.Rows("1"))

	require.NoError(t, failpoint.Disable(cleanupErrFP))
	close(allowRetry)
	require.NoError(t, <-dropErrCh)
}

func TestDropMaterializedViewLogPurgeInfoFailureRollsBackMetadata(t *testing.T) {
	store, dom := testkit.CreateMockStoreAndDomain(t)
	tk := newMViewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("create table t_drop_mlog_atomic (a int)")
	tk.MustExec("create materialized view log on t_drop_mlog_atomic (a)")

	is := dom.InfoSchema()
	mlogTable, err := is.TableByName(context.Background(), ast.NewCIStr("test"), model.MaterializedViewLogTableName(ast.NewCIStr("t_drop_mlog_atomic")))
	require.NoError(t, err)
	mlogID := mlogTable.Meta().ID

	const cleanupErrFP = "github.com/pingcap/tidb/pkg/ddl/mockDeleteMaterializedViewLogPurgeInfoErr"
	require.NoError(t, failpoint.Enable(cleanupErrFP, `1*return("mock purge info delete error")`))
	defer func() { require.NoError(t, failpoint.Disable(cleanupErrFP)) }()

	retryStarted := make(chan struct{})
	allowRetry := make(chan struct{})
	testfailpoint.EnableCall(t, "github.com/pingcap/tidb/pkg/ddl/beforeRunOneJobStep", func(job *model.Job) {
		if job.Type == model.ActionDropTable && job.TableID == mlogID && job.SchemaState == model.StateDeleteOnly && job.ErrorCount > 0 {
			select {
			case <-retryStarted:
			default:
				close(retryStarted)
			}
			<-allowRetry
		}
	})

	tkInspect := newMViewTestKit(t, store)
	tkInspect.MustExec("use test")
	dropErrCh := make(chan error, 1)
	go func() { dropErrCh <- tk.ExecToErr("drop materialized view log on t_drop_mlog_atomic") }()

	select {
	case <-retryStarted:
	case <-time.After(10 * time.Second):
		t.Fatal("timeout waiting for DROP MATERIALIZED VIEW LOG retry")
	}
	tkInspect.MustQuery("show tables like '$mlog$t_drop_mlog_atomic'").Check(testkit.Rows("$mlog$t_drop_mlog_atomic"))
	tkInspect.MustQuery(fmt.Sprintf("select count(*) from mysql.tidb_mlog_purge_info where mlog_id = %d", mlogID)).Check(testkit.Rows("1"))

	require.NoError(t, failpoint.Disable(cleanupErrFP))
	close(allowRetry)
	require.NoError(t, <-dropErrCh)
}

func TestDropDatabaseMViewInfoFailureRollsBackMetadata(t *testing.T) {
	store, dom := testkit.CreateMockStoreAndDomain(t)
	tk := newMViewTestKit(t, store)

	const dbName = "mv_drop_db_atomic"
	tk.MustExec("create database " + dbName)
	tk.MustExec("use " + dbName)
	tk.MustExec("create table t (a int)")
	tk.MustExec("create materialized view log on t (a)")
	tk.MustExec("create materialized view mv (a, cnt) as select a, count(1) from t group by a")

	is := dom.InfoSchema()
	dbInfo, ok := is.SchemaByName(ast.NewCIStr(dbName))
	require.True(t, ok)
	mvTable, err := is.TableByName(context.Background(), ast.NewCIStr(dbName), ast.NewCIStr("mv"))
	require.NoError(t, err)
	mlogTable, err := is.TableByName(context.Background(), ast.NewCIStr(dbName), ast.NewCIStr("$mlog$t"))
	require.NoError(t, err)
	mvID := mvTable.Meta().ID
	mlogID := mlogTable.Meta().ID

	const cleanupErrFP = "github.com/pingcap/tidb/pkg/ddl/mockDeleteCreateMaterializedViewRefreshInfoErr"
	require.NoError(t, failpoint.Enable(cleanupErrFP, `1*return("mock refresh info delete error")`))
	defer func() { require.NoError(t, failpoint.Disable(cleanupErrFP)) }()

	retryStarted := make(chan struct{})
	allowRetry := make(chan struct{})
	testfailpoint.EnableCall(t, "github.com/pingcap/tidb/pkg/ddl/beforeRunOneJobStep", func(job *model.Job) {
		if job.Type == model.ActionDropSchema && job.SchemaState == model.StateDeleteOnly && job.ErrorCount > 0 {
			select {
			case <-retryStarted:
			default:
				close(retryStarted)
			}
			<-allowRetry
		}
	})

	tkInspect := newMViewTestKit(t, store)
	dropErrCh := make(chan error, 1)
	go func() { dropErrCh <- tk.ExecToErr("drop database " + dbName) }()

	select {
	case <-retryStarted:
	case <-time.After(10 * time.Second):
		t.Fatal("timeout waiting for DROP DATABASE retry")
	}
	tkInspect.MustQuery(fmt.Sprintf("select count(*) from mysql.tidb_mview_refresh_info where mview_id = %d", mvID)).Check(testkit.Rows("1"))
	tkInspect.MustQuery(fmt.Sprintf("select count(*) from mysql.tidb_mlog_purge_info where mlog_id = %d", mlogID)).Check(testkit.Rows("1"))
	require.NoError(t, kv.RunInNewTxn(context.Background(), store, false, func(_ context.Context, txn kv.Transaction) error {
		persistedDBInfo, err := meta.NewReader(txn).GetDatabase(dbInfo.ID)
		require.NoError(t, err)
		require.NotNil(t, persistedDBInfo)
		return nil
	}))

	require.NoError(t, failpoint.Disable(cleanupErrFP))
	close(allowRetry)
	require.NoError(t, <-dropErrCh)
	tkInspect.MustQuery(fmt.Sprintf("select count(*) from mysql.tidb_mview_refresh_info where mview_id = %d", mvID)).Check(testkit.Rows("0"))
	tkInspect.MustQuery(fmt.Sprintf("select count(*) from mysql.tidb_mlog_purge_info where mlog_id = %d", mlogID)).Check(testkit.Rows("0"))
	require.NoError(t, kv.RunInNewTxn(context.Background(), store, false, func(_ context.Context, txn kv.Transaction) error {
		persistedDBInfo, err := meta.NewReader(txn).GetDatabase(dbInfo.ID)
		require.NoError(t, err)
		require.Nil(t, persistedDBInfo)
		return nil
	}))
}

func TestDropMaterializedViewAndDatabaseCleanMViewState(t *testing.T) {
	store, dom := testkit.CreateMockStoreAndDomain(t)
	tk := newMViewTestKit(t, store)

	const dbName = "mv_drop_cleanup"
	tk.MustExec("drop database if exists " + dbName)
	tk.MustExec("create database " + dbName)
	tk.MustExec("use " + dbName)
	tk.MustExec("create table t (a int not null, b int not null)")
	tk.MustExec("insert into t values (1, 10), (2, 20)")
	tk.MustExec("create materialized view log on t (a, b) purge next date_add(now(), interval 1 hour)")
	tk.MustExec("create materialized view mv (a, s, cnt) refresh fast next date_add(now(), interval 1 hour) as select a, sum(b), count(1) from t group by a")

	is := dom.InfoSchema()
	mvTable, err := is.TableByName(context.Background(), ast.NewCIStr(dbName), ast.NewCIStr("mv"))
	require.NoError(t, err)
	mlogTable, err := is.TableByName(context.Background(), ast.NewCIStr(dbName), ast.NewCIStr("$mlog$t"))
	require.NoError(t, err)
	mvID := mvTable.Meta().ID
	mlogID := mlogTable.Meta().ID

	tk.MustExec(fmt.Sprintf(
		"insert into mysql.tidb_mview_refresh_alert (MVIEW_ID, MVIEW_SCHEMA, MVIEW_NAME, ALERT_LEVEL, LAST_SUCCESS_SNAPSHOT_TIME, UPDATE_TIME) values (%d, '%s', 'mv', 'warning', UTC_TIMESTAMP(), UTC_TIMESTAMP())",
		mvID, dbName,
	))
	tk.MustQuery(fmt.Sprintf("select count(*) from mysql.tidb_mview_refresh_info where mview_id = %d", mvID)).Check(testkit.Rows("1"))
	tk.MustQuery(fmt.Sprintf("select count(*) from mysql.tidb_mlog_purge_info where mlog_id = %d", mlogID)).Check(testkit.Rows("1"))
	tk.MustQuery(fmt.Sprintf("select count(*) from mysql.tidb_mview_refresh_alert where mview_id = %d", mvID)).Check(testkit.Rows("1"))

	tk.MustExec("drop materialized view mv")
	tk.MustQuery(fmt.Sprintf("select count(*) from mysql.tidb_mview_refresh_info where mview_id = %d", mvID)).Check(testkit.Rows("0"))
	tk.MustQuery(fmt.Sprintf("select count(*) from mysql.tidb_mview_refresh_alert where mview_id = %d", mvID)).Check(testkit.Rows("0"))
	tk.MustExec("drop materialized view log on t")
	tk.MustQuery(fmt.Sprintf("select count(*) from mysql.tidb_mlog_purge_info where mlog_id = %d", mlogID)).Check(testkit.Rows("0"))

	tk.MustExec("create materialized view log on t (a, b)")
	tk.MustExec("create materialized view mv (a, s, cnt) refresh fast next date_add(now(), interval 1 hour) as select a, sum(b), count(1) from t group by a")
	is = dom.InfoSchema()
	mvTable, err = is.TableByName(context.Background(), ast.NewCIStr(dbName), ast.NewCIStr("mv"))
	require.NoError(t, err)
	mlogTable, err = is.TableByName(context.Background(), ast.NewCIStr(dbName), ast.NewCIStr("$mlog$t"))
	require.NoError(t, err)
	mvID = mvTable.Meta().ID
	mlogID = mlogTable.Meta().ID
	tk.MustExec(fmt.Sprintf(
		"insert into mysql.tidb_mview_refresh_alert (MVIEW_ID, MVIEW_SCHEMA, MVIEW_NAME, ALERT_LEVEL, LAST_SUCCESS_SNAPSHOT_TIME, UPDATE_TIME) values (%d, '%s', 'mv', 'warning', UTC_TIMESTAMP(), UTC_TIMESTAMP())",
		mvID, dbName,
	))
	tk.MustQuery(fmt.Sprintf("select count(*) from mysql.tidb_mview_refresh_info where mview_id = %d", mvID)).Check(testkit.Rows("1"))
	tk.MustQuery(fmt.Sprintf("select count(*) from mysql.tidb_mview_refresh_alert where mview_id = %d", mvID)).Check(testkit.Rows("1"))
	tk.MustQuery(fmt.Sprintf("select count(*) from mysql.tidb_mlog_purge_info where mlog_id = %d", mlogID)).Check(testkit.Rows("1"))
	tk.MustExec("drop database " + dbName)
	tk.MustQuery(fmt.Sprintf("select count(*) from mysql.tidb_mview_refresh_info where mview_id = %d", mvID)).Check(testkit.Rows("0"))
	tk.MustQuery(fmt.Sprintf("select count(*) from mysql.tidb_mview_refresh_alert where mview_id = %d", mvID)).Check(testkit.Rows("0"))
	tk.MustQuery(fmt.Sprintf("select count(*) from mysql.tidb_mlog_purge_info where mlog_id = %d", mlogID)).Check(testkit.Rows("0"))
	_, ok := dom.InfoSchema().SchemaByName(ast.NewCIStr(dbName))
	require.False(t, ok)
}

func TestDropMaterializedViewCleansRefreshAlert(t *testing.T) {
	store, dom := testkit.CreateMockStoreAndDomain(t)
	tk := newMViewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("create table t_drop_alert_cleanup (a int not null, b int not null)")
	tk.MustExec("insert into t_drop_alert_cleanup values (1, 10), (1, 5), (2, 7)")
	tk.MustExec("create materialized view log on t_drop_alert_cleanup (a, b) purge next date_add(now(), interval 1 hour)")
	tk.MustExec("create materialized view mv_drop_alert_cleanup (a, s, cnt) refresh fast next date_add(now(), interval 1 hour) as select a, sum(b), count(1) from t_drop_alert_cleanup group by a")

	is := dom.InfoSchema()
	mvTable, err := is.TableByName(context.Background(), ast.NewCIStr("test"), ast.NewCIStr("mv_drop_alert_cleanup"))
	require.NoError(t, err)
	mvID := mvTable.Meta().ID
	tk.MustExec(fmt.Sprintf(
		"insert into mysql.tidb_mview_refresh_alert (MVIEW_ID, MVIEW_SCHEMA, MVIEW_NAME, ALERT_LEVEL, LAST_SUCCESS_SNAPSHOT_TIME, UPDATE_TIME) values (%d, 'test', 'mv_drop_alert_cleanup', 'warning', UTC_TIMESTAMP(), UTC_TIMESTAMP())",
		mvID,
	))
	tk.MustQuery(fmt.Sprintf("select count(*) from mysql.tidb_mview_refresh_alert where mview_id = %d", mvID)).
		Check(testkit.Rows("1"))

	tk.MustExec("drop materialized view mv_drop_alert_cleanup")
	tk.MustQuery(fmt.Sprintf("select count(*) from mysql.tidb_mview_refresh_info where mview_id = %d", mvID)).
		Check(testkit.Rows("0"))
	tk.MustQuery(fmt.Sprintf("select count(*) from mysql.tidb_mview_refresh_alert where mview_id = %d", mvID)).
		Check(testkit.Rows("0"))
	tk.MustExec("drop materialized view log on t_drop_alert_cleanup")
}

func TestDropDatabaseIgnoresRefreshAlertDeleteFailure(t *testing.T) {
	store, dom := testkit.CreateMockStoreAndDomain(t)
	tk := newMViewTestKit(t, store)

	const dbName = "mv_drop_db_alert_delete_fail"
	tk.MustExec("drop database if exists " + dbName)
	tk.MustExec("create database " + dbName)
	tk.MustExec("use " + dbName)
	tk.MustExec("create table t (a int not null, b int not null)")
	tk.MustExec("insert into t values (1, 10), (2, 20)")
	tk.MustExec("create materialized view log on t (a, b) purge next date_add(now(), interval 1 hour)")
	tk.MustExec("create materialized view mv (a, s, cnt) refresh fast next date_add(now(), interval 1 hour) as select a, sum(b), count(1) from t group by a")

	is := dom.InfoSchema()
	mvTable, err := is.TableByName(context.Background(), ast.NewCIStr(dbName), ast.NewCIStr("mv"))
	require.NoError(t, err)
	mvID := mvTable.Meta().ID
	tk.MustExec(fmt.Sprintf(
		"insert into mysql.tidb_mview_refresh_alert (MVIEW_ID, MVIEW_SCHEMA, MVIEW_NAME, ALERT_LEVEL, LAST_SUCCESS_SNAPSHOT_TIME, UPDATE_TIME) values (%d, '%s', 'mv', 'overdue', UTC_TIMESTAMP(), UTC_TIMESTAMP())",
		mvID,
		dbName,
	))
	tk.MustQuery(fmt.Sprintf("select count(*) from mysql.tidb_mview_refresh_alert where mview_id = %d", mvID)).
		Check(testkit.Rows("1"))

	const fp = "github.com/pingcap/tidb/pkg/ddl/mockDeleteCreateMaterializedViewRefreshAlertErr"
	require.NoError(t, failpoint.Enable(fp, `return("mock drop schema alert delete error")`))
	defer func() { require.NoError(t, failpoint.Disable(fp)) }()

	tk.MustExec("drop database " + dbName)
	tk.MustQuery(fmt.Sprintf("select count(*) from mysql.tidb_mview_refresh_info where mview_id = %d", mvID)).
		Check(testkit.Rows("0"))
	tk.MustQuery(fmt.Sprintf("select count(*) from mysql.tidb_mview_refresh_alert where mview_id = %d", mvID)).
		Check(testkit.Rows("1"))
}

func TestDropMaterializedViewIgnoresRefreshAlertDeleteFailure(t *testing.T) {
	store, dom := testkit.CreateMockStoreAndDomain(t)
	tk := newMViewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("create table t_drop_alert (a int not null, b int not null)")
	tk.MustExec("create materialized view log on t_drop_alert (a, b) purge next date_add(now(), interval 1 hour)")
	tk.MustExec("create materialized view mv_drop_alert (a, s, cnt) refresh fast next date_add(now(), interval 1 hour) as select a, sum(b), count(1) from t_drop_alert group by a")

	is := dom.InfoSchema()
	mvTable, err := is.TableByName(context.Background(), ast.NewCIStr("test"), ast.NewCIStr("mv_drop_alert"))
	require.NoError(t, err)
	mvID := mvTable.Meta().ID
	tk.MustExec(fmt.Sprintf(
		"insert into mysql.tidb_mview_refresh_alert (MVIEW_ID, MVIEW_SCHEMA, MVIEW_NAME, ALERT_LEVEL, LAST_SUCCESS_SNAPSHOT_TIME, UPDATE_TIME) values (%d, 'test', 'mv_drop_alert', 'warning', UTC_TIMESTAMP(), UTC_TIMESTAMP())",
		mvID,
	))

	const fp = "github.com/pingcap/tidb/pkg/ddl/mockDeleteCreateMaterializedViewRefreshAlertErr"
	require.NoError(t, failpoint.Enable(fp, `return("mock drop table alert delete error")`))
	defer func() { require.NoError(t, failpoint.Disable(fp)) }()
	tk.MustExec("drop materialized view mv_drop_alert")
	tk.MustQuery(fmt.Sprintf("select count(*) from mysql.tidb_mview_refresh_info where mview_id = %d", mvID)).Check(testkit.Rows("0"))
	tk.MustQuery(fmt.Sprintf("select count(*) from mysql.tidb_mview_refresh_alert where mview_id = %d", mvID)).Check(testkit.Rows("1"))
	tk.MustExec("drop materialized view log on t_drop_alert")
}

func TestDropMaterializedViewLogPrivilege(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := newMViewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("create table t_drop_mlog_priv (a int)")
	tk.MustExec("create materialized view log on t_drop_mlog_priv (a)")
	tk.MustExec("create user 'u_drop_mlog_select'@'%'")
	tk.MustExec("create user 'u_drop_mlog_ok'@'%'")
	defer tk.MustExec("drop user 'u_drop_mlog_select'@'%'")
	defer tk.MustExec("drop user 'u_drop_mlog_ok'@'%'")
	tk.MustExec("grant select on test.t_drop_mlog_priv to 'u_drop_mlog_select'@'%'")
	tk.MustExec("grant drop on test.`$mlog$t_drop_mlog_priv` to 'u_drop_mlog_ok'@'%'")

	tkSelect := newMViewTestKit(t, store)
	require.NoError(t, tkSelect.Session().Auth(&auth.UserIdentity{Username: "u_drop_mlog_select", Hostname: "%"}, nil, nil, nil))
	err := tkSelect.ExecToErr("drop materialized view log on test.t_drop_mlog_priv")
	require.ErrorContains(t, err, "DROP MATERIALIZED VIEW LOG command denied")
	require.ErrorContains(t, err, "for table 't_drop_mlog_priv'")

	tkDrop := newMViewTestKit(t, store)
	require.NoError(t, tkDrop.Session().Auth(&auth.UserIdentity{Username: "u_drop_mlog_ok", Hostname: "%"}, nil, nil, nil))
	tkDrop.MustExec("drop materialized view log on test.t_drop_mlog_priv")
}

func TestDropMaterializedViewPrivilege(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := newMViewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("create table t_drop_mv_priv (a int)")
	tk.MustExec("create materialized view log on t_drop_mv_priv (a)")
	tk.MustExec("create materialized view mv_drop_priv (a) as select a from t_drop_mv_priv")

	tk.MustExec("create user 'u_drop_mv_select'@'%'")
	tk.MustExec("create user 'u_drop_mv_ok'@'%'")
	t.Cleanup(func() {
		tk.MustExec("drop user 'u_drop_mv_select'@'%'")
		tk.MustExec("drop user 'u_drop_mv_ok'@'%'")
	})
	tk.MustExec("grant select on test.mv_drop_priv to 'u_drop_mv_select'@'%'")
	tk.MustExec("grant drop on test.mv_drop_priv to 'u_drop_mv_ok'@'%'")

	tkSelect := newMViewTestKit(t, store)
	require.NoError(t, tkSelect.Session().Auth(&auth.UserIdentity{Username: "u_drop_mv_select", Hostname: "%"}, nil, nil, nil))
	err := tkSelect.ExecToErr("drop materialized view test.mv_drop_priv")
	require.ErrorContains(t, err, "DROP command denied")

	tkDrop := newMViewTestKit(t, store)
	require.NoError(t, tkDrop.Session().Auth(&auth.UserIdentity{Username: "u_drop_mv_ok", Hostname: "%"}, nil, nil, nil))
	tkDrop.MustExec("drop materialized view test.mv_drop_priv")
	tk.MustExec("drop materialized view log on test.t_drop_mv_priv")
	tk.MustExec("drop table test.t_drop_mv_priv")
}

func TestDropMaterializedViewLogBeforeBaseTable(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := newMViewTestKit(t, store)
	tk.MustExec("use test")

	tk.MustExec("create table t_drop_seq (a int)")
	tk.MustExec("create materialized view log on t_drop_seq (a)")
	tk.MustExec("drop materialized view log on t_drop_seq")
	tk.MustExec("drop table if exists t_drop_seq")
}
