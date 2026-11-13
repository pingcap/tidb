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
	"github.com/pingcap/tidb/pkg/server"
	"github.com/pingcap/tidb/pkg/session/syssession"
	"github.com/pingcap/tidb/pkg/sessionctx"
	"github.com/pingcap/tidb/pkg/testkit"
	"github.com/pingcap/tidb/pkg/testkit/testfailpoint"
	ttlsession "github.com/pingcap/tidb/pkg/ttl/session"
	"github.com/pingcap/tidb/pkg/util/chunk"
	"github.com/stretchr/testify/require"
)

func TestSchemaReadOnlyReviewRegressions(t *testing.T) {
	t.Run("PreparedWriterPrivilege", func(t *testing.T) {
		for _, cache := range []bool{false, true} {
			for _, role := range []bool{false, true} {
				t.Run(fmt.Sprintf("cache=%t/role=%t", cache, role), func(t *testing.T) {
					enableReadOnlyDDLFp(t)
					store := testkit.CreateMockStore(t)
					admin := testkit.NewTestKit(t, store)
					writer := testkit.NewTestKit(t, store)
					admin.MustExec("create database ro")
					admin.MustExec("create table ro.t(id int primary key)")
					admin.MustExec("create user 'writer'@'%'")
					admin.MustExec("grant insert on ro.* to 'writer'@'%'")
					grantee := "'writer'@'%'"
					if role {
						admin.MustExec("create role 'replica_writer'")
						admin.MustExec("grant 'replica_writer' to 'writer'@'%'")
						grantee = "'replica_writer'"
					}
					admin.MustExec("grant RESTRICTED_REPLICA_WRITER_ADMIN on *.* to " + grantee)
					require.NoError(t, writer.Session().Auth(&auth.UserIdentity{Username: "writer", Hostname: "%"}, nil, nil, nil))
					if role {
						writer.MustExec("set role 'replica_writer'")
					}
					writer.MustExec(fmt.Sprintf("set tidb_enable_prepared_plan_cache = %t", cache))
					admin.MustExec("alter database ro read only = 1")
					writer.MustExec("prepare ins from 'insert into ro.t values (?)'")
					writer.MustExec("prepare explain_write from 'explain insert into ro.t values (99)'")
					writer.MustExec("set @id = 1")
					writer.MustExec("execute ins using @id")
					writer.MustExec("set @id = 2")
					writer.MustExec("execute ins using @id")
					require.Equal(t, cache, writer.Session().GetSessionVars().FoundInPlanCache)
					if role {
						writer.MustExec("set role none")
					} else {
						admin.MustExec("revoke RESTRICTED_REPLICA_WRITER_ADMIN on *.* from " + grantee)
					}
					writer.MustExec("set @id = 3")
					writer.MustGetErrCode("execute ins using @id", 3989)
					writer.MustGetErrCode("execute explain_write", 3989)
					admin.MustQuery("select * from ro.t").Check(testkit.Rows("1", "2"))
				})
			}
		}
	})
	t.Run("PreparedMultiDeleteUsesPrepareDB", func(t *testing.T) {
		enableReadOnlyDDLFp(t)
		store := testkit.CreateMockStore(t)
		admin := testkit.NewTestKit(t, store)
		writer := testkit.NewTestKit(t, store)
		admin.MustExec("create database ro")
		admin.MustExec("create table ro.t(id int primary key)")
		admin.MustExec("insert into ro.t values (1), (2), (3)")
		admin.MustExec("create user 'writer'@'%'")
		admin.MustExec("grant select, delete on ro.* to 'writer'@'%'")
		admin.MustExec("grant select on test.* to 'writer'@'%'")
		admin.MustExec("grant RESTRICTED_REPLICA_WRITER_ADMIN on *.* to 'writer'@'%'")
		admin.MustExec("alter database ro read only = 1")
		require.NoError(t, writer.Session().Auth(&auth.UserIdentity{Username: "writer", Hostname: "%"}, nil, nil, nil))
		writer.MustExec("use ro")
		writer.MustExec("prepare del from 'delete t from t where id = ?'")
		for _, id := range []int{1, 2} {
			writer.MustExec(fmt.Sprintf("set @id = %d", id))
			writer.MustExec("execute del using @id")
		}
		require.True(t, writer.Session().GetSessionVars().FoundInPlanCache)
		admin.MustExec("revoke RESTRICTED_REPLICA_WRITER_ADMIN on *.* from 'writer'@'%'")
		writer.MustExec("use test")
		writer.MustExec("set @id = 3")
		writer.MustGetErrCode("execute del using @id", 3989)
		admin.MustQuery("select * from ro.t").Check(testkit.Rows("3"))
	})
	t.Run("NativeMaterializedViewDDL", func(t *testing.T) {
		for _, tc := range []struct {
			name, sql string
			setup     []string
		}{
			{name: "CreateView", sql: "create materialized view ro.mv(a, cnt) as select a, count(1) from ro.t group by a", setup: []string{"create materialized view log on ro.t(a)"}},
			{name: "CreateLog", sql: "create materialized view log on ro.t(a)"},
			{name: "DropView", sql: "drop materialized view ro.mv", setup: []string{"create materialized view log on ro.t(a)", "create materialized view ro.mv(a, cnt) as select a, count(1) from ro.t group by a"}},
			{name: "DropLog", sql: "drop materialized view log on ro.t", setup: []string{"create materialized view log on ro.t(a)"}},
			{name: "AlterLog", sql: "alter materialized view log on ro.t add column (b)", setup: []string{"create materialized view log on ro.t(a)"}},
		} {
			t.Run(tc.name, func(t *testing.T) {
				enableReadOnlyDDLFp(t)
				store := testkit.CreateMockStore(t)
				tk := testkit.NewTestKit(t, store)
				tk.MustExec("set tidb_mview_enable = on")
				tk.MustExec("create database ro")
				tk.MustExec("create table ro.t(a int not null, b int)")
				for _, sql := range tc.setup {
					tk.MustExec(sql)
				}
				before := tk.MustQuery("show tables from ro").Rows()
				tk.MustExec("alter database ro read only = 1")
				tk.MustGetErrCode("prepare mv_ddl from '"+tc.sql+"'", 3989)
				tk.MustGetErrCode(tc.sql, 3989)
				require.Equal(t, before, tk.MustQuery("show tables from ro").Rows())
				if tc.name == "DropView" {
					tk.MustExec("grant RESTRICTED_REPLICA_WRITER_ADMIN on *.* to 'root'@'%'")
					require.NoError(t, tk.Session().Auth(&auth.UserIdentity{Username: "root", Hostname: "%"}, nil, nil, nil))
					tk.MustExec(tc.sql)
				}
			})
		}
	})
	t.Run("ExplicitLockTargets", func(t *testing.T) {
		enableReadOnlyDDLFp(t)
		store := testkit.CreateMockStore(t)
		tk := testkit.NewTestKit(t, store)
		tk.MustExec("create database ro")
		tk.MustExec("create database rw")
		tk.MustExec("create table ro.t(id int primary key)")
		tk.MustExec("create table rw.t(id int primary key)")
		tk.MustExec("insert into ro.t values (1)")
		tk.MustExec("insert into rw.t values (1)")
		tk.MustExec("alter database ro read only = 1")
		tk.MustExec("begin pessimistic")
		defer tk.MustExec("rollback")
		tk.MustQuery("select a.id from rw.t a join ro.t b on a.id = b.id for update of a").Check(testkit.Rows("1"))
		tk.MustGetErrCode("select a.id from rw.t a join ro.t b on a.id = b.id for update of b", 3989)
		tk.MustExec("prepare lock_rw from 'select a.id from rw.t a join ro.t b on a.id = b.id for update of a'")
		tk.MustQuery("execute lock_rw").Check(testkit.Rows("1"))
	})
	t.Run("PendingStateName", func(t *testing.T) {
		require.Equal(t, "pending read only", model.StatePendingReadOnly.String())
	})
	t.Run("BatchTransactionPolling", func(t *testing.T) {
		enableReadOnlyDDLFp(t)
		store := testkit.CreateMockStore(t)
		alter := testkit.NewTestKit(t, store)
		alter.MustExec("create database ro")
		alter.MustExec("create table ro.t(id int primary key)")
		alter.MustExec("create table test.unrelated(id int)")
		unrelated := testkit.NewTestKit(t, store)
		unrelated.MustExec("begin optimistic")
		unrelated.MustQuery("select * from test.unrelated").Check(testkit.Rows())
		defer unrelated.MustExec("rollback")
		txns := make([]*testkit.TestKit, 4)
		for i := range txns {
			txns[i] = testkit.NewTestKit(t, store)
			txns[i].MustExec("begin optimistic")
			txns[i].MustExec(fmt.Sprintf("insert into ro.t values (%d)", i))
		}
		scanned := make(chan struct{})
		resume := make(chan struct{})
		release := sync.OnceFunc(func() { close(resume) })
		var once sync.Once
		var polls atomic.Int64
		var pollRows atomic.Int64
		testfailpoint.EnableCall(t, "github.com/pingcap/tidb/pkg/ddl/afterReadOnlyTxnQuery", func(_ sessionctx.Context, sql string, rows *[]chunk.Row) {
			if strings.HasPrefix(sql, "SELECT RELATED_TABLE_IDS") {
				once.Do(func() { close(scanned); <-resume })
			} else if strings.HasPrefix(sql, "SELECT ID ") {
				polls.Add(1)
				pollRows.Store(int64(len(*rows)))
			}
		})
		var wg sync.WaitGroup
		defer func() {
			release()
			for _, txn := range txns {
				txn.MustExec("rollback")
			}
			wg.Wait()
		}()
		done := make(chan error, 1)
		wg.Add(1)
		go func() { defer wg.Done(); done <- alter.ExecToErr("alter database ro read only = 1") }()
		select {
		case <-scanned:
		case <-time.After(5 * time.Second):
			t.Fatal("read-only transaction scan did not run")
		}
		for _, txn := range txns {
			txn.MustExec("commit")
		}
		release()
		require.NoError(t, <-done)
		require.EqualValues(t, 1, polls.Load(), "one poll must confirm all completed transactions")
		require.Zero(t, pollRows.Load(), "polling must not return unrelated active transactions")
	})
	t.Run("MDLOnLazyTransaction", func(t *testing.T) {
		enableReadOnlyDDLFp(t)
		store := testkit.CreateMockStore(t)
		writer := testkit.NewTestKit(t, store)
		alter := testkit.NewTestKit(t, store)
		alter.MustExec("create database ro")
		alter.MustExec("create table ro.t(a int)")
		planned := make(chan struct{})
		resume := make(chan struct{})
		release := sync.OnceFunc(func() { close(resume) })
		var paused atomic.Bool
		testfailpoint.EnableCall(t, "github.com/pingcap/tidb/pkg/sessiontxn/isolation/beforeActivateTxnForSchemaReadOnly", func(ctx sessionctx.Context) {
			if ctx != writer.Session() || !paused.CompareAndSwap(false, true) {
				return
			}
			close(planned)
			<-resume
		})
		var wg sync.WaitGroup
		defer func() { release(); wg.Wait() }()
		done := make(chan error, 1)
		wg.Add(1)
		go func() { defer wg.Done(); done <- writer.ExecToErr("insert into ro.t values (1)") }()
		select {
		case <-planned:
		case <-time.After(5 * time.Second):
			t.Fatal("INSERT did not reach transaction activation")
		}
		registered := false
		writer.Session().GetSessionVars().GetRelatedTableForMDL().Range(func(_, _ any) bool {
			registered = true
			return false
		})
		require.True(t, registered, "INSERT must register its table before the transition")
		alter.MustExec("alter database ro read only = 1")
		release()
		require.ErrorContains(t, <-done, "read only state has changed")
		alter.MustQuery("select * from ro.t").Check(testkit.Rows())
	})
	t.Run("QueuedDDL", func(t *testing.T) {
		for _, bypass := range []bool{false, true} {
			t.Run(fmt.Sprintf("bypass=%t", bypass), func(t *testing.T) {
				enableReadOnlyDDLFp(t)
				store := testkit.CreateMockStore(t)
				admin := testkit.NewTestKit(t, store)
				alter := testkit.NewTestKit(t, store)
				create := testkit.NewTestKit(t, store)
				admin.MustExec("create database ro")
				if bypass {
					admin.MustExec("grant RESTRICTED_REPLICA_WRITER_ADMIN on *.* to 'root'@'%'")
					require.NoError(t, create.Session().Auth(&auth.UserIdentity{Username: "root", Hostname: "%"}, nil, nil, nil))
				}
				started := make(chan struct{})
				resume := make(chan struct{})
				release := sync.OnceFunc(func() { close(resume) })
				var paused atomic.Bool
				testfailpoint.EnableCall(t, "github.com/pingcap/tidb/pkg/ddl/beforeRunOneJobStep", func(job *model.Job) {
					if job.Type == model.ActionModifySchemaReadOnly && paused.CompareAndSwap(false, true) {
						close(started)
						<-resume
					}
				})
				var wg sync.WaitGroup
				defer func() { release(); wg.Wait() }()
				alterDone := make(chan error, 1)
				createDone := make(chan error, 1)
				wg.Add(1)
				go func() { defer wg.Done(); alterDone <- alter.ExecToErr("alter database ro read only = 1") }()
				select {
				case <-started:
				case <-time.After(5 * time.Second):
					t.Fatal("read-only job did not start")
				}
				wg.Add(1)
				go func() { defer wg.Done(); createDone <- create.ExecToErr("create table ro.late(a int)") }()
				require.Eventually(t, func() bool {
					return len(admin.MustQuery("admin show ddl jobs where db_name = 'ro' and job_type = 'create table'").Rows()) != 0
				}, 5*time.Second, 10*time.Millisecond)
				release()
				require.NoError(t, <-alterDone)
				if bypass {
					require.NoError(t, <-createDone)
					admin.MustQuery("show tables from ro").Check(testkit.Rows("late"))
				} else {
					require.ErrorContains(t, <-createDone, "[schema:3989]")
					admin.MustQuery("show tables from ro").Check(testkit.Rows())
				}
			})
		}
	})
	t.Run("InternalTTLTransaction", func(t *testing.T) {
		enableReadOnlyDDLFp(t)
		store, dom := testkit.CreateMockStoreAndDomain(t)
		sv := server.CreateMockServer(t, store)
		sv.SetDomain(dom)
		dom.InfoSyncer().SetSessionManager(sv)
		defer sv.Close()
		admin := testkit.NewTestKit(t, store)
		alter := testkit.NewTestKit(t, store)
		admin.MustExec("set global tidb_ttl_job_enable = off")
		admin.MustExec("create database ro")
		admin.MustExec("create table ro.t(id int primary key, ts datetime) TTL = ts + interval 1 second")
		admin.MustExec("insert into ro.t values (1, now() - interval 1 day)")
		deleted := make(chan struct{})
		resume := make(chan struct{})
		release := sync.OnceFunc(func() { close(resume) })
		var txnID atomic.Uint64
		var wg sync.WaitGroup
		defer func() { release(); wg.Wait() }()
		ttlDone := make(chan error, 1)
		wg.Add(1)
		go func() {
			defer wg.Done()
			ttlDone <- dom.AdvancedSysSessionPool().WithSession(func(s *syssession.Session) error {
				return s.WithSessionContext(func(ctx sessionctx.Context) error {
					ttl := ttlsession.NewSession(ctx, s.AvoidReuse)
					return ttl.RunInTxn(context.Background(), func() error {
						if _, err := ttl.ExecuteSQL(context.Background(), "delete from ro.t where ts < now() - interval 1 second"); err != nil {
							return err
						}
						txnID.Store(ctx.GetSessionVars().TxnCtx.StartTS)
						close(deleted)
						<-resume
						return nil
					}, ttlsession.TxnModeOptimistic)
				})
			})
		}()
		select {
		case <-deleted:
		case err := <-ttlDone:
			t.Fatalf("TTL transaction failed before DELETE: %v", err)
		case <-time.After(5 * time.Second):
			t.Fatal("TTL DELETE did not finish")
		}
		require.Contains(t, sv.GetInternalSessionStartTSList(), txnID.Load())
		require.NoError(t, admin.Session().Auth(&auth.UserIdentity{Username: "root", Hostname: "%"}, nil, nil, nil))
		admin.MustQuery(fmt.Sprintf("select id from information_schema.tidb_trx where id = %d", txnID.Load())).Check(testkit.Rows())
		scanned := make(chan bool, 1)
		var scanObserved atomic.Bool
		testfailpoint.EnableCall(t, "github.com/pingcap/tidb/pkg/ddl/afterReadOnlyTxnQuery", func(_ sessionctx.Context, sql string, rows *[]chunk.Row) {
			if !strings.HasPrefix(sql, "SELECT RELATED_TABLE_IDS") || !scanObserved.CompareAndSwap(false, true) {
				return
			}
			found := false
			for _, row := range *rows {
				found = found || uint64(row.GetInt64(1)) == txnID.Load()
			}
			scanned <- found
		})
		alterDone := make(chan error, 1)
		wg.Add(1)
		go func() { defer wg.Done(); alterDone <- alter.ExecToErr("alter database ro read only = 1") }()
		select {
		case found := <-scanned:
			require.True(t, found, "read-only transition omitted the uncommitted internal TTL DELETE")
		case <-time.After(5 * time.Second):
			t.Fatal("read-only transaction scan did not run")
		}
		select {
		case err := <-alterDone:
			t.Fatalf("read-only transition finished before TTL COMMIT: %v", err)
		default:
		}
		release()
		require.NoError(t, <-ttlDone)
		require.NoError(t, <-alterDone)
		admin.MustQuery("select * from ro.t").Check(testkit.Rows())
	})
}

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
