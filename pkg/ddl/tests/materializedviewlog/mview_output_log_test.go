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

package materializedviewlog

import (
	"context"
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	"github.com/pingcap/failpoint"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/testkit"
	"github.com/pingcap/tidb/pkg/testkit/testfailpoint"
	"github.com/stretchr/testify/require"
)

func TestCreateMaterializedViewLogRefreshLockLifetime(t *testing.T) {
	for _, rollback := range []bool{false, true} {
		t.Run(fmt.Sprintf("rollback=%t", rollback), func(t *testing.T) {
			store, dom := testkit.CreateMockStoreAndDomain(t)
			tk := newMViewTestKit(t, store)
			tk.MustExec("use test")
			tk.MustExec("create table t_mlog_lock (a int not null, b int not null)")
			tk.MustExec("create materialized view log on t_mlog_lock (a, b)")
			tk.MustExec("create materialized view mv_mlog_lock (a, s, cnt) as select a, sum(b), count(1) from t_mlog_lock group by a")
			mv, err := dom.InfoSchema().TableByName(context.Background(), ast.NewCIStr("test"), ast.NewCIStr("mv_mlog_lock"))
			require.NoError(t, err)
			lockName := fmt.Sprintf("mv_refresh_%d_%d", mv.Meta().DBID, mv.Meta().ID)
			paused := make(chan struct{})
			resume := make(chan struct{})
			var signalled atomic.Bool
			release := func() {
				select {
				case <-resume:
				default:
					close(resume)
				}
			}
			testfailpoint.EnableCall(t, "github.com/pingcap/tidb/pkg/ddl/beforeWaitSchemaSynced", func(job *model.Job, _ int64) {
				if job.Type == model.ActionCreateMaterializedViewLog && job.TableName == "$mlog$mv_mlog_lock" && signalled.CompareAndSwap(false, true) {
					close(paused)
					<-resume
				}
			})
			const failurePoint = "github.com/pingcap/tidb/pkg/ddl/mockInsertMLogPurgeTableNotExists"
			if rollback {
				require.NoError(t, failpoint.Enable(failurePoint, "return(true)"))
				defer func() { require.NoError(t, failpoint.Disable(failurePoint)) }()
			}
			ddlDone := make(chan error, 1)
			go func() { ddlDone <- tk.ExecToErr("create materialized view log on mv_mlog_lock (a, s, cnt)") }()
			finished := false
			defer func() {
				release()
				if !finished {
					select {
					case <-ddlDone:
					case <-time.After(15 * time.Second):
						t.Error("timeout cleaning up CREATE MLog")
					}
				}
			}()
			select {
			case <-paused:
			case <-time.After(15 * time.Second):
				t.Fatal("timeout waiting for CREATE MLog schema sync")
			}
			tkLock := newMViewTestKit(t, store)
			tkLock.MustExec("use test")
			err = tkLock.ExecToErr("refresh materialized view mv_mlog_lock fast")
			require.ErrorContains(t, err, "another refresh or materialized view log creation is running", "the job must hold the refresh lock after its metadata transaction commits")
			release()
			select {
			case err = <-ddlDone:
				finished = true
			case <-time.After(15 * time.Second):
				t.Fatal("timeout waiting for CREATE MLog completion")
			}
			if rollback {
				require.ErrorContains(t, err, "tidb_mlog_purge_info")
				require.NoError(t, failpoint.Disable(failurePoint))
				tk.MustQuery("show tables like '$mlog$mv_mlog_lock'").Check(testkit.Rows())
			} else {
				require.NoError(t, err)
			}
			require.NoError(t, tkLock.Session().GetAdvisoryLock(lockName, 1), "the worker must release the lock before reuse")
			require.True(t, tkLock.Session().ReleaseAdvisoryLock(lockName))
			if rollback {
				tk.MustExec("create materialized view log on mv_mlog_lock (a, s, cnt)")
			}
		})
	}
}

func TestMaterializedViewLogOnReadyMaterializedView(t *testing.T) {
	store, dom := testkit.CreateMockStoreAndDomain(t)
	tk := newMViewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("create table t_mv_mlog_ddl (a int not null, b int not null)")
	tk.MustExec("insert into t_mv_mlog_ddl values (1, 10)")
	tk.MustExec("create materialized view log on t_mv_mlog_ddl (a, b)")
	tk.MustExec("create materialized view mv_mlog_ddl (a, s, cnt) refresh fast as select a, sum(b), count(1) from t_mv_mlog_ddl group by a")
	tk.MustExec("create materialized view log on mv_mlog_ddl (a)")

	mlog, err := dom.InfoSchema().TableByName(context.Background(), ast.NewCIStr("test"), ast.NewCIStr("$mlog$mv_mlog_ddl"))
	require.NoError(t, err)
	mlogID := mlog.Meta().ID
	mv, err := dom.InfoSchema().TableByName(context.Background(), ast.NewCIStr("test"), ast.NewCIStr("mv_mlog_ddl"))
	require.NoError(t, err)
	require.Equal(t, mlogID, mv.Meta().MaterializedViewBase.MLogID)
	require.Equal(t, mv.Meta().ID, mlog.Meta().MaterializedViewLog.BaseTableID)

	showCreate := tk.MustQuery("show create materialized view log on mv_mlog_ddl").Rows()[0][1].(string)
	require.Contains(t, showCreate, "CREATE MATERIALIZED VIEW LOG ON `mv_mlog_ddl` (`a`)")
	tk.MustExec("alter materialized view log on mv_mlog_ddl add column (s, cnt)")
	showCreate = tk.MustQuery("show create materialized view log on mv_mlog_ddl").Rows()[0][1].(string)
	require.Contains(t, showCreate, "CREATE MATERIALIZED VIEW LOG ON `mv_mlog_ddl` (`a`, `s`, `cnt`)")
	tk.MustExec("alter materialized view log on mv_mlog_ddl purge next date_add(now(), interval 1 hour)")
	tk.MustQuery(fmt.Sprintf("select NEXT_PURGE_UNIX_SECONDS is not null from mysql.tidb_mlog_purge_info where MLOG_ID=%d", mlogID)).Check(testkit.Rows("1"))
	tk.MustExec("purge materialized view log on mv_mlog_ddl")

	for _, stmt := range []string{"drop materialized view mv_mlog_ddl", "drop materialized view if exists mv_mlog_ddl"} {
		require.ErrorContains(t, tk.ExecToErr(stmt), "DROP MATERIALIZED VIEW with materialized view log")
	}
	mvAfterDrop, err := dom.InfoSchema().TableByName(context.Background(), ast.NewCIStr("test"), ast.NewCIStr("mv_mlog_ddl"))
	require.NoError(t, err)
	require.Equal(t, mv.Meta().ID, mvAfterDrop.Meta().ID)
	require.Equal(t, mlogID, mvAfterDrop.Meta().MaterializedViewBase.MLogID)
	tk.MustQuery(fmt.Sprintf("select count(*) from mysql.tidb_mlog_purge_info where MLOG_ID=%d", mlogID)).Check(testkit.Rows("1"))
	tk.MustQuery("select * from mv_mlog_ddl").Check(testkit.Rows("1 10 1"))

	tk.MustExec("drop materialized view log on mv_mlog_ddl")
	mv, err = dom.InfoSchema().TableByName(context.Background(), ast.NewCIStr("test"), ast.NewCIStr("mv_mlog_ddl"))
	require.NoError(t, err)
	require.True(t, mv.Meta().MaterializedViewBase == nil || mv.Meta().MaterializedViewBase.MLogID == 0)
	tk.MustQuery(fmt.Sprintf("select count(*) from mysql.tidb_mlog_purge_info where MLOG_ID=%d", mlogID)).Check(testkit.Rows("0"))
	tk.MustQuery("select * from mv_mlog_ddl").Check(testkit.Rows("1 10 1"))
	tk.MustExec("drop materialized view mv_mlog_ddl")
	tk.MustQuery("show tables like 'mv_mlog_ddl'").Check(testkit.Rows())
	tk.MustQuery("show tables like '$mlog$mv_mlog_ddl'").Check(testkit.Rows())
	tk.MustExec("drop materialized view log on t_mv_mlog_ddl")
	tk.MustExec("drop table t_mv_mlog_ddl")
}

func TestDropMaterializedViewRechecksOwnedLog(t *testing.T) {
	store, dom := testkit.CreateMockStoreAndDomain(t)
	tk := newMViewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("create table t_drop_mv_mlog (a int)")
	tk.MustExec("create materialized view log on t_drop_mv_mlog (a)")
	tk.MustExec("create materialized view mv_drop_mlog (a, cnt) as select a, count(1) from t_drop_mv_mlog group by a")
	mv, err := dom.InfoSchema().TableByName(context.Background(), ast.NewCIStr("test"), ast.NewCIStr("mv_drop_mlog"))
	require.NoError(t, err)
	mvID := mv.Meta().ID

	createStarted := make(chan struct{})
	allowCreate := make(chan struct{})
	releaseCreate := func() {
		select {
		case <-allowCreate:
		default:
			close(allowCreate)
		}
	}
	t.Cleanup(releaseCreate)
	var signalled atomic.Bool
	testfailpoint.EnableCall(t, "github.com/pingcap/tidb/pkg/ddl/beforeRunOneJobStep", func(job *model.Job) {
		if job.Type != model.ActionCreateMaterializedViewLog || job.TableName != "$mlog$mv_drop_mlog" {
			return
		}
		if signalled.CompareAndSwap(false, true) {
			close(createStarted)
		}
		<-allowCreate
	})

	tkCreate := newMViewTestKit(t, store)
	tkCreate.MustExec("use test")
	createErrCh := make(chan error, 1)
	go func() { createErrCh <- tkCreate.ExecToErr("create materialized view log on mv_drop_mlog (a, cnt)") }()
	select {
	case <-createStarted:
	case <-time.After(10 * time.Second):
		t.Fatal("timeout waiting for CREATE MATERIALIZED VIEW LOG worker")
	}

	// Enqueue DROP with a stale executor precheck, behind the blocked CREATE.
	tkDrop := newMViewTestKit(t, store)
	tkDrop.MustExec("use test")
	dropErrCh := make(chan error, 1)
	go func() { dropErrCh <- tkDrop.ExecToErr("drop materialized view mv_drop_mlog") }()
	require.Eventually(t, func() bool {
		return len(tk.MustQuery("select job_id from mysql.tidb_ddl_job").Rows()) == 2
	}, 10*time.Second, 10*time.Millisecond)
	releaseCreate()
	select {
	case err := <-createErrCh:
		require.NoError(t, err)
	case <-time.After(10 * time.Second):
		t.Fatal("timeout waiting for CREATE MATERIALIZED VIEW LOG")
	}
	select {
	case err := <-dropErrCh:
		require.ErrorContains(t, err, "DROP MATERIALIZED VIEW with materialized view log")
	case <-time.After(10 * time.Second):
		t.Fatal("timeout waiting for DROP MATERIALIZED VIEW")
	}

	mv, err = dom.InfoSchema().TableByName(context.Background(), ast.NewCIStr("test"), ast.NewCIStr("mv_drop_mlog"))
	require.NoError(t, err)
	require.Equal(t, mvID, mv.Meta().ID)
	require.NotNil(t, mv.Meta().MaterializedViewBase)
	mlogID := mv.Meta().MaterializedViewBase.MLogID
	mlog, ok := dom.InfoSchema().TableByID(context.Background(), mlogID)
	require.True(t, ok)
	require.NotNil(t, mlog.Meta().MaterializedViewLog)
	require.Equal(t, mvID, mlog.Meta().MaterializedViewLog.BaseTableID)
	tk.MustQuery(fmt.Sprintf("select count(*) from mysql.tidb_mlog_purge_info where MLOG_ID=%d", mlogID)).Check(testkit.Rows("1"))
	tk.MustExec("drop materialized view log on mv_drop_mlog")
	tk.MustExec("drop materialized view mv_drop_mlog")
}

func TestMaterializedViewOutputLogRejectsNestedCreate(t *testing.T) {
	store, dom := testkit.CreateMockStoreAndDomain(t)
	tk := newMViewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("create table t_nested_guard (a int)")
	tk.MustExec("create materialized view log on t_nested_guard (a)")
	tk.MustExec("create materialized view mv_nested_parent (a, cnt) as select a, count(1) from t_nested_guard group by a")
	tk.MustExec("create materialized view log on mv_nested_parent (a, cnt)")
	require.ErrorContains(t, tk.ExecToErr("create materialized view mv_nested_executor (a, cnt) as select a, count(1) from mv_nested_parent group by a"), "is not BASE TABLE")
	parent, err := dom.InfoSchema().TableByName(context.Background(), ast.NewCIStr("test"), ast.NewCIStr("mv_nested_parent"))
	require.NoError(t, err)
	var checked atomic.Bool
	testfailpoint.EnableCall(t, "github.com/pingcap/tidb/pkg/ddl/beforeRunOneJobStep", func(job *model.Job) {
		if job.Type != model.ActionCreateMaterializedView || job.TableName != "mv_nested_worker" || !checked.CompareAndSwap(false, true) {
			return
		}
		args, err := model.GetCreateMaterializedViewArgs(job)
		require.NoError(t, err)
		// Bypass submission validation to exercise the worker's independent guard.
		args.TableInfo.MaterializedView.BaseTableIDs = []int64{parent.Meta().ID}
		args.MLogTableIDs = []int64{parent.Meta().MaterializedViewBase.MLogID}
		job.FillArgs(args)
		_, err = job.Encode(true) // V1 decoding reads the serialized arguments.
		require.NoError(t, err)
	})
	require.ErrorContains(t, tk.ExecToErr("create materialized view mv_nested_worker (a, cnt) as select a, count(1) from t_nested_guard group by a"), "is not BASE TABLE")
	require.True(t, checked.Load())
	tk.MustQuery("show tables like 'mv_nested_executor'").Check(testkit.Rows())
	tk.MustQuery("show tables like 'mv_nested_worker'").Check(testkit.Rows())
}
