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
	"errors"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/pingcap/tidb/pkg/config/kerneltype"
	ddlutil "github.com/pingcap/tidb/pkg/ddl/util"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/parser/auth"
	"github.com/pingcap/tidb/pkg/sessionctx"
	"github.com/pingcap/tidb/pkg/testkit"
	"github.com/pingcap/tidb/pkg/testkit/testfailpoint"
	"github.com/pingcap/tidb/pkg/util/chunk"
	"github.com/stretchr/testify/require"
)

func TestSchemaReadOnlyTransitions(t *testing.T) {
	t.Run("ConcurrentRequestsWait", func(t *testing.T) {
		enableReadOnlyDDLFp(t)
		store := testkit.CreateMockStore(t)
		txn := testkit.NewTestKit(t, store)
		first := testkit.NewTestKit(t, store)
		second := testkit.NewTestKit(t, store)
		txn.MustExec("create database ro")
		txn.MustExec("create table ro.t(a int)")
		txn.MustExec("begin optimistic")
		txn.MustExec("insert into ro.t values (1)")
		var observed atomic.Bool
		testfailpoint.EnableCall(t, "github.com/pingcap/tidb/pkg/ddl/checkUncommittedTxns", func(ids map[int64]struct{}) { observed.Store(len(ids) != 0) })
		firstDone := make(chan error, 1)
		go func() { firstDone <- first.ExecToErr("alter database ro read only = 1") }()
		defer func() {
			txn.MustExec("rollback")
			select {
			case err := <-firstDone:
				require.NoError(t, err)
			case <-time.After(5 * time.Second):
				t.Error("first ALTER did not finish")
			}
		}()
		require.Eventually(t, observed.Load, 5*time.Second, 10*time.Millisecond)
		secondDone := make(chan error, 1)
		go func() { secondDone <- second.ExecToErr("alter database ro read only = 1") }()
		select {
		case err := <-secondDone:
			require.NoError(t, err)
			t.Error("duplicate ALTER returned before the existing transaction finished")
		case <-time.After(time.Second):
			txn.MustExec("commit")
			require.NoError(t, <-secondDone)
		}
	})
	t.Run("MDLOffExistingWrite", func(t *testing.T) {
		if kerneltype.IsNextGen() {
			t.Skip("MDL is always enabled in nextgen")
		}
		enableReadOnlyDDLFp(t)
		store := testkit.CreateMockStore(t)
		txn := testkit.NewTestKit(t, store)
		other := testkit.NewTestKit(t, store)
		txn.MustExec("create database ro")
		txn.MustExec("create table ro.t(a int)")
		txn.MustExec("set global tidb_enable_metadata_lock = 0")
		defer other.MustExec("set global tidb_enable_metadata_lock = 1")
		txn.MustExec("begin optimistic")
		txn.MustExec("insert into ro.t values (1)")
		defer txn.MustExec("rollback")
		done := make(chan error, 1)
		go func() { done <- other.ExecToErr("alter database ro read only = 1") }()
		select {
		case err := <-done:
			require.NoError(t, err)
			t.Error("ALTER returned before the existing write committed")
		case <-time.After(2 * time.Second):
			txn.MustExec("commit")
			require.NoError(t, <-done)
		}
		txn.MustExec("commit")
		other.MustQuery("select * from ro.t").Check(testkit.Rows("1"))
	})
	t.Run("MDLOffLateAccess", func(t *testing.T) {
		if kerneltype.IsNextGen() {
			t.Skip("MDL is always enabled in nextgen")
		}
		enableReadOnlyDDLFp(t)
		store := testkit.CreateMockStore(t)
		txn := testkit.NewTestKit(t, store)
		other := testkit.NewTestKit(t, store)
		txn.MustExec("create database ro")
		txn.MustExec("create table ro.t(a int)")
		txn.MustExec("create table test.anchor(a int)")
		txn.MustExec("set global tidb_enable_metadata_lock = 0")
		defer other.MustExec("set global tidb_enable_metadata_lock = 1")
		txn.MustExec("begin optimistic")
		txn.MustQuery("select * from test.anchor").Check(testkit.Rows())
		defer txn.MustExec("rollback")
		other.MustExec("alter database ro read only = 1")
		txn.MustGetErrMsg("insert into ro.t values (2)", "[domain:8028]public schema ro read only state has changed")
		txn.MustExec("rollback")
		other.MustQuery("select * from ro.t").Check(testkit.Rows())
	})
	t.Run("SequenceMutation", func(t *testing.T) {
		enableReadOnlyDDLFp(t)
		store := testkit.CreateMockStore(t)
		tk := testkit.NewTestKit(t, store)
		tk.MustExec("create database ro")
		tk.MustExec("create sequence ro.s nocache")
		tk.MustQuery("select nextval(ro.s)").Check(testkit.Rows("1"))
		tk.MustExec("alter database ro read only = 1")
		require.ErrorContains(t, tk.QueryToErr("select nextval(ro.s)"), "[schema:3989]")
		require.ErrorContains(t, tk.QueryToErr("select setval(ro.s, 100)"), "[schema:3989]")
		tk.MustQuery("select lastval(ro.s)").Check(testkit.Rows("1"))
	})
	t.Run("MDLOffLazyTransaction", func(t *testing.T) {
		if kerneltype.IsNextGen() {
			t.Skip("MDL is always enabled in nextgen")
		}
		enableReadOnlyDDLFp(t)
		store := testkit.CreateMockStore(t)
		writer := testkit.NewTestKit(t, store)
		alter := testkit.NewTestKit(t, store)
		alter.MustExec("create database ro")
		alter.MustExec("create table ro.t(a int)")
		alter.MustExec("set global tidb_enable_metadata_lock = 0")
		defer alter.MustExec("set global tidb_enable_metadata_lock = 1")
		planned := make(chan struct{})
		resume := make(chan struct{})
		release := sync.OnceFunc(func() { close(resume) })
		defer release()
		var paused atomic.Bool
		testfailpoint.EnableCall(t, "github.com/pingcap/tidb/pkg/planner/core/afterCheckSchemaReadOnlyWithoutMDL", func(db ast.CIStr) {
			if db.L == "ro" && paused.CompareAndSwap(false, true) {
				close(planned)
				<-resume
			}
		})
		done := make(chan error, 1)
		go func() { done <- writer.ExecToErr("insert into ro.t values (1)") }()
		select {
		case <-planned:
		case <-time.After(5 * time.Second):
			t.Fatal("write was not planned")
		}
		alter.MustExec("alter database ro read only = 1")
		release()
		require.ErrorContains(t, <-done, "read only state has changed")
		alter.MustQuery("select * from ro.t").Check(testkit.Rows())
	})
	t.Run("MaterializedViewAttributes", func(t *testing.T) {
		enableReadOnlyDDLFp(t)
		store, dom := testkit.CreateMockStoreAndDomain(t)
		tk := testkit.NewTestKit(t, store)
		tk.MustExec("set tidb_mview_enable = on")
		tk.MustExec("create database ro")
		tk.MustExec("use ro")
		tk.MustExec("create table t(a int not null)")
		tk.MustExec("create materialized view log on t(a)")
		tk.MustExec("create materialized view mv(a, cnt) as select a, count(1) from t group by a")
		tk.MustExec("alter database ro read only = 1")
		tk.MustGetErrCode("alter materialized view mv attributes='mview_alert_warning=5,mview_alert_overdue=10,mview_alert_refresh_failed=yes'", 3989)
		mv, err := dom.InfoSchema().TableByName(context.Background(), ast.NewCIStr("ro"), ast.NewCIStr("mv"))
		require.NoError(t, err)
		require.False(t, mv.Meta().MaterializedView.AlertRefreshFailed)
	})
}

func TestSchemaReadOnlyIncompleteTransactionScan(t *testing.T) {
	for _, polling := range []bool{false, true} {
		t.Run(fmt.Sprintf("polling=%t", polling), func(t *testing.T) {
			enableReadOnlyDDLFp(t)
			store := testkit.CreateMockStore(t)
			txn := testkit.NewTestKit(t, store)
			alter := testkit.NewTestKit(t, store)
			txn.MustExec("create database ro")
			txn.MustExec("create table ro.t(a int)")
			alter.MustExec("set global tidb_ddl_error_count_limit = 1")
			if polling {
				txn.MustExec("begin optimistic")
				txn.MustExec("insert into ro.t values (1)")
				defer txn.MustExec("rollback")
			}
			testfailpoint.EnableCall(t, "github.com/pingcap/tidb/pkg/ddl/afterReadOnlyTxnQuery", func(ctx sessionctx.Context, sql string, rows *[]chunk.Row) {
				isPolling := strings.HasPrefix(sql, "SELECT ID ")
				if isPolling == polling {
					*rows = nil
					ctx.GetSessionVars().StmtCtx.AppendWarning(errors.New("remote TiDB transaction scan unavailable"))
				}
			})
			err := alter.ExecToErr("alter database ro read only = 1")
			require.ErrorContains(t, err, "remote TiDB transaction scan unavailable")
			alter.MustQuery("show create database ro").Check(testkit.Rows("ro CREATE DATABASE `ro` /*!40100 DEFAULT CHARACTER SET utf8mb4 */"))
		})
	}
}

func TestSchemaReadOnlyRollbackAfterPanic(t *testing.T) {
	for _, readOnly := range []bool{false, true} {
		t.Run(fmt.Sprintf("readOnly=%t", readOnly), func(t *testing.T) {
			enableReadOnlyDDLFp(t)
			store, dom := testkit.CreateMockStoreAndDomain(t)
			tk := testkit.NewTestKit(t, store)
			tk.MustExec("create database ro")
			if !readOnly {
				tk.MustExec("alter database ro read only = 1")
			}
			var panicked atomic.Bool
			testfailpoint.EnableCall(t, "github.com/pingcap/tidb/pkg/ddl/beforeUpdateSchemaVersion", func(job *model.Job, _ *error) {
				if job.Type == model.ActionModifySchemaReadOnly && panicked.CompareAndSwap(false, true) {
					panic("read-only metadata was written before schema version update")
				}
			})
			readOnlyValue := 0
			if readOnly {
				readOnlyValue = 1
			}
			err := tk.ExecToErr(fmt.Sprintf("alter database ro read only = %d", readOnlyValue))
			require.Error(t, err)
			require.True(t, panicked.Load())
			db, ok := dom.InfoSchema().SchemaByName(ast.NewCIStr("ro"))
			require.True(t, ok)
			require.Equal(t, !readOnly, db.ReadOnly)
		})
	}
}

func TestSchemaReadOnlyReplicaWriterSequence(t *testing.T) {
	enableReadOnlyDDLFp(t)
	store := testkit.CreateMockStore(t)
	admin := testkit.NewTestKit(t, store)
	writer := testkit.NewTestKit(t, store)
	admin.MustExec("create database ro")
	admin.MustExec("create sequence ro.s nocache")
	admin.MustExec("create user 'repl_writer'@'%'")
	admin.MustExec("grant all on ro.* to 'repl_writer'@'%'")
	require.NoError(t, writer.Session().Auth(&auth.UserIdentity{Username: "repl_writer", Hostname: "%"}, nil, nil, nil))
	writer.MustExec("prepare seq_stmt from 'select nextval(ro.s)'")
	admin.MustExec("alter database ro read only = 1")
	require.ErrorContains(t, writer.QueryToErr("select nextval(ro.s)"), "[schema:3989]")
	require.ErrorContains(t, writer.QueryToErr("execute seq_stmt"), "[schema:3989]")
	admin.MustExec("grant RESTRICTED_REPLICA_WRITER_ADMIN on *.* to 'repl_writer'@'%'")
	writer.MustQuery("execute seq_stmt").Check(testkit.Rows("1"))
	writer.MustQuery("select setval(ro.s, 100)").Check(testkit.Rows("100"))
	writer.MustQuery("select nextval(ro.s)").Check(testkit.Rows("101"))
}

func TestSchemaReadOnlyRecoveryAndCancellation(t *testing.T) {
	t.Run("CancelQueuedDuplicate", func(t *testing.T) {
		enableReadOnlyDDLFp(t)
		store, dom := testkit.CreateMockStoreAndDomain(t)
		first := testkit.NewTestKit(t, store)
		second := testkit.NewTestKit(t, store)
		admin := testkit.NewTestKit(t, store)
		admin.MustExec("create database ro")
		firstStarted := make(chan struct{})
		releaseFirst := make(chan struct{})
		release := sync.OnceFunc(func() { close(releaseFirst) })
		var firstID atomic.Int64
		var wg sync.WaitGroup
		testfailpoint.EnableCall(t, "github.com/pingcap/tidb/pkg/ddl/beforeRunOneJobStep", func(job *model.Job) {
			if job.Type == model.ActionModifySchemaReadOnly && firstID.CompareAndSwap(0, job.ID) {
				close(firstStarted)
				<-releaseFirst
			}
		})
		t.Cleanup(func() { release(); wg.Wait() })
		firstDone := make(chan error, 1)
		secondDone := make(chan error, 1)
		wg.Add(1)
		go func() { defer wg.Done(); firstDone <- first.ExecToErr("alter database ro read only = 1") }()
		select {
		case <-firstStarted:
		case <-time.After(5 * time.Second):
			t.Fatal("first ALTER did not start")
		}
		wg.Add(1)
		go func() { defer wg.Done(); secondDone <- second.ExecToErr("alter database ro read only = 1") }()
		var secondID string
		require.Eventually(t, func() bool {
			rows := admin.MustQuery("admin show ddl jobs where db_name = 'ro' and job_type = 'modify schema read only'").Rows()
			for _, row := range rows {
				if fmt.Sprint(row[0]) != fmt.Sprint(firstID.Load()) {
					secondID = fmt.Sprint(row[0])
					return true
				}
			}
			return false
		}, 5*time.Second, 10*time.Millisecond)
		admin.MustQuery("admin cancel ddl jobs " + secondID).Check(testkit.Rows(secondID + " successful"))
		release()
		require.NoError(t, <-firstDone)
		require.Error(t, <-secondDone)
		db, ok := dom.InfoSchema().SchemaByName(ast.NewCIStr("ro"))
		require.True(t, ok)
		require.True(t, db.ReadOnly)
	})
	t.Run("DroppedTableID", func(t *testing.T) {
		if kerneltype.IsNextGen() {
			t.Skip("MDL is always enabled in nextgen")
		}
		enableReadOnlyDDLFp(t)
		store := testkit.CreateMockStore(t)
		txn := testkit.NewTestKit(t, store)
		other := testkit.NewTestKit(t, store)
		txn.MustExec("create database ro")
		txn.MustExec("create table test.ghost(a int)")
		txn.MustExec("begin optimistic")
		txn.MustQuery("select * from test.ghost").Check(testkit.Rows())
		other.MustExec("set global tidb_enable_metadata_lock = 0")
		defer other.MustExec("set global tidb_enable_metadata_lock = 1")
		defer txn.MustExec("rollback")
		other.MustExec("drop table test.ghost")
		other.MustExec("set global tidb_ddl_error_count_limit = 1")
		other.MustExec("alter database ro read only = 1")
	})
	t.Run("RecoverTables", func(t *testing.T) {
		enableReadOnlyDDLFp(t)
		store := testkit.CreateMockStore(t)
		tk := testkit.NewTestKit(t, store)
		gcWasEnabled := ddlutil.IsEmulatorGCEnable()
		ddlutil.EmulatorGCDisable()
		defer func() {
			if gcWasEnabled {
				ddlutil.EmulatorGCEnable()
			}
		}()
		tk.MustExec(fmt.Sprintf("insert into mysql.tidb values ('tikv_gc_safe_point', '%s', '') on duplicate key update variable_value=values(variable_value)", time.Now().Add(-time.Hour).Format("20060102-15:04:05 -0700 MST")))
		tk.MustExec("create database ro")
		tk.MustExec("create table ro.a(id int)")
		tk.MustExec("create table ro.b(id int)")
		tk.MustExec("drop table ro.a, ro.b")
		jobs := tk.MustQuery("admin show ddl jobs where db_name = 'ro' and job_type = 'drop table'").Rows()
		var dropJobID string
		for _, job := range jobs {
			if job[2] == "b" {
				dropJobID = fmt.Sprint(job[0])
			}
		}
		require.NotEmpty(t, dropJobID)
		tk.MustExec("alter database ro read only = 1")
		tk.MustExec("use test")
		tk.MustGetErrCode("flashback table ro.a", 3989)
		tk.MustGetErrCode("recover table ro.b", 3989)
		tk.MustGetErrCode("recover table by job "+dropJobID, 3989)
		tk.MustQuery("select table_name from information_schema.tables where table_schema='ro' order by table_name").Check(testkit.Rows())
		tk.MustExec("grant RESTRICTED_REPLICA_WRITER_ADMIN on *.* to 'root'@'%'")
		require.NoError(t, tk.Session().Auth(&auth.UserIdentity{Username: "root", Hostname: "%"}, nil, nil, nil))
		tk.MustExec("flashback table ro.a")
		tk.MustExec("recover table ro.b")
		tk.MustQuery("select table_name from information_schema.tables where table_schema='ro' order by table_name").Check(testkit.Rows("a", "b"))
	})
}
